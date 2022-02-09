use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, Addr, Recipient, SyncArbiter};
use futures::future::select_all;
use futures::TryFutureExt;
use tokio::sync::RwLock;
use tracing::Instrument;

use coinnect_rt::broker::{ActixMessageBroker, Broker, MarketEventEnvelopeMsg};
// use actix::System;
// use tokio::select;
// use tokio::signal::unix::{signal, SignalKind};
use crate::connectivity::run_connectivity_checker;
use crate::nats::{NatsConsumer, NatsProducer, Subject};
use crate::server;
use crate::settings::{AvroFileLoggerSettings, OutputSettings, Settings, StreamSettings};
use coinnect_rt::prelude::*;
use logging::prelude::*;
use metrics::prom::PrometheusPushActor;
use portfolio::balance::BalanceReporter;
use portfolio::margin::MarginAccountReporter;
#[allow(unused_imports)]
use strategies::mean_reverting;
use strategy::plugin::plugin_registry;
use strategy::prelude::StrategyCopySettings;
use strategy::{self, Channel, StrategyKey, Trader};
use trading::engine::{new_trading_engine, TradingEngine};
use trading::interest::MarginInterestRateProvider;
use trading::order_manager::OrderManager;
use trading::types::AccountChannel;

pub mod bots;

/// # Panics
///
/// The system fails to boot
#[allow(clippy::too_many_lines)]
pub async fn start(settings: Arc<RwLock<Settings>>) -> anyhow::Result<()> {
    let settings_v = settings.read().await;

    // Verify credentials are here
    let keys_path = PathBuf::from(settings_v.keys.clone());
    fs::metadata(keys_path.clone()).map_err(|_| anyhow!("key file doesn't exist at {:?}", keys_path.clone()))?;

    // Configure exchanges
    let exchanges = &settings_v.exchanges;
    let exchanges_conf = Arc::new(exchanges.clone());
    let manager = Arc::new(Coinnect::new_manager());
    manager
        .build_exchange_apis(exchanges_conf.clone(), keys_path.clone())
        .await;
    // Temporarily load symbol cache from here
    // TODO: do this to a read-only memory mapped file somewhere else that is used as a cache
    Coinnect::load_pair_registries(manager.exchange_apis())
        .instrument(tracing::info_span!("loading pair registries"))
        .await?;
    // Message brokers
    let mut market_broker = ActixMessageBroker::<Channel, MarketEventEnvelopeMsg>::new();
    let mut account_broker = ActixMessageBroker::<AccountChannel, AccountEventEnveloppe>::new();
    // Termination handles to fuse the server with
    let mut termination_handles: Vec<Pin<Box<dyn Future<Output = std::io::Result<()>>>>> = vec![];
    // Message recipients
    let mut broadcast_recipients: Vec<Recipient<Arc<MarketEventEnvelope>>> = Vec::new();
    let mut strat_recipients: Vec<Recipient<Arc<MarketEventEnvelope>>> = Vec::new();
    let mut traders = vec![];

    // strategies, cf strategies crate
    let settings_arc = Arc::clone(&settings);

    for output in settings_v.outputs.clone() {
        match output {
            OutputSettings::AvroFileLogger(logger_settings) => {
                broadcast_recipients.push(file_actor(logger_settings).recipient());
            }
            OutputSettings::Nats(nats_settings) => {
                let producer = NatsProducer::new(&nats_settings.host, &nats_settings.username, &nats_settings.password)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotConnected, e))?;
                broadcast_recipients.push(NatsProducer::start(producer).recipient());
            }
            OutputSettings::Strategies => {
                let om = OrderManager::actor(&settings_v.storage, manager.clone()).await;
                termination_handles.push(Box::pin(bots::poll_pingables(vec![om.clone().recipient()])));
                for entry in manager.exchange_apis().iter() {
                    account_broker.register(
                        AccountChannel::new(*entry.key(), AccountType::Spot),
                        om.clone().recipient(),
                    );
                    account_broker.register(
                        AccountChannel::new(*entry.key(), AccountType::Margin),
                        om.clone().recipient(),
                    );
                }
                let mirp = MarginInterestRateProvider::actor(manager.clone());
                let engine = new_trading_engine(manager.clone(), om, mirp);
                let strategies = make_traders(settings_arc.clone(), Arc::new(engine))
                    .instrument(tracing::info_span!("starting strategies"))
                    .await;
                for trader in strategies {
                    for channel in &trader.channels {
                        market_broker.register(channel.clone(), trader.market_event_recipient());
                    }
                    strat_recipients.push(trader.market_event_recipient());
                    traders.push(trader.clone());
                }
            }
        }
    }

    // balance reporter
    if let Some(balance_reporter_opts) = &settings_v.balance_reporter {
        info!("starting balance reporter");
        let reporter_addr = BalanceReporter::actor(balance_reporter_opts, manager.clone()).await;
        for api_ref in manager.exchange_apis() {
            account_broker.register(
                AccountChannel::new(*api_ref.key(), AccountType::Spot),
                reporter_addr.clone().recipient(),
            );
        }
        termination_handles.push(Box::pin(bots::poll_pingables(vec![reporter_addr.recipient()])));
    }

    // margin balance reporter
    if let Some(margin_account_reporter_opts) = &settings_v.margin_account_reporter {
        info!("starting margin account reporter");
        let reporter_addr = MarginAccountReporter::actor(margin_account_reporter_opts, manager.clone()).await;
        for api_ref in manager.exchange_apis().iter() {
            account_broker.register(
                AccountChannel::new(*api_ref.key(), AccountType::Margin),
                reporter_addr.clone().recipient(),
            );
        }
        termination_handles.push(Box::pin(bots::poll_pingables(vec![reporter_addr.recipient()])));
    }

    // metrics actor
    let _prom_push = PrometheusPushActor::start(PrometheusPushActor::new(&settings_v.prometheus));

    let market_broker_ref = Arc::new(market_broker);
    let account_broker_ref = Arc::new(account_broker);

    for stream_settings in &settings_v.streams {
        match stream_settings {
            StreamSettings::ExchangeBots => {
                let mut all_recipients = vec![];
                all_recipients.extend(strat_recipients.clone());
                all_recipients.extend(broadcast_recipients.clone());
                let mut bots = bots::exchange_bots(exchanges_conf.clone(), keys_path.clone()).await?;
                if !bots.is_empty() {
                    let market_broker_ref = market_broker_ref.clone();
                    let fut = async move {
                        select_all(bots.iter_mut().map(|(_, bot)| {
                            let market_broker_ref = market_broker_ref.clone();
                            bot.add_sink(Box::new(move |msg| {
                                market_broker_ref.broadcast(msg);
                                Ok(())
                            }))
                        }))
                        .await;
                        Ok(())
                    }
                    .map_err(|e: anyhow::Error| std::io::Error::new(ErrorKind::Other, e));
                    termination_handles.push(Box::pin(fut));
                }
            }
            StreamSettings::AccountBots => {
                let mut bots = bots::spot_account_bots(exchanges_conf.clone(), keys_path.clone()).await?;
                let margin_bots = bots::margin_account_bots(exchanges_conf.clone(), keys_path.clone()).await?;
                bots.extend(margin_bots);
                let isolated_margin_bots =
                    bots::isolated_margin_account_bots(exchanges_conf.clone(), keys_path.clone()).await?;
                bots.extend(isolated_margin_bots);
                if !bots.is_empty() {
                    let account_broker_ref = account_broker_ref.clone();
                    let fut = async move {
                        select_all(bots.iter_mut().map(|bot| {
                            let account_broker_ref = account_broker_ref.clone();
                            bot.add_sink(Box::new(move |msg| {
                                account_broker_ref.broadcast(msg);
                                Ok(())
                            }))
                        }))
                        .await;
                        Ok(())
                    }
                    .map_err(|e: anyhow::Error| std::io::Error::new(ErrorKind::Other, e));
                    termination_handles.push(Box::pin(fut));
                }
            }
            StreamSettings::Nats(nats_settings) => {
                info!("nats consumers");
                // For now, give each strategy a nats consumer
                for trader in traders.clone() {
                    let topics = trader
                        .channels
                        .iter()
                        .map(<MarketEventEnvelope as Subject>::from_channel)
                        .collect();
                    let consumer = NatsConsumer::start(
                        NatsConsumer::new::<MarketEventEnvelope>(
                            &nats_settings.host,
                            &nats_settings.username,
                            &nats_settings.username,
                            topics,
                            vec![trader.market_event_recipient()],
                        )
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                    );
                    termination_handles.push(Box::pin(poll_actor(consumer)));
                }
                if !broadcast_recipients.is_empty() {
                    let consumer = NatsConsumer::start(
                        NatsConsumer::new(
                            &nats_settings.host,
                            &nats_settings.username,
                            &nats_settings.username,
                            vec![<MarketEventEnvelope as Subject>::glob()],
                            broadcast_recipients.clone(),
                        )
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                    );
                    termination_handles.push(Box::pin(poll_actor(consumer)));
                }
            }
        }
    }

    let traders_by_key: Arc<HashMap<StrategyKey, Trader>> =
        Arc::new(traders.clone().iter().map(|s| (s.key.clone(), s.clone())).collect());
    // API Server
    let server = server::httpserver(
        &settings_v.api,
        settings_v.version.clone(),
        manager.clone(),
        traders_by_key,
    );
    termination_handles.push(Box::pin(server));

    // Handle interrupts for graceful shutdown
    // let mut terminate = signal(SignalKind::terminate())?;
    // let mut interrupt = signal(SignalKind::interrupt())?;
    // let mut userint = signal(SignalKind::user_defined1())?;
    // Somehow necessary because settings_v doesn't live long enough
    termination_handles.push(Box::pin(tokio::signal::ctrl_c()));
    if let Some(interval) = settings_v.connectivity_check_interval {
        connectivity_checker(interval);
    }

    let x = select_all(termination_handles).await.0.map_err(|e| anyhow!(e));
    x
    //use futures::FutureExt;
    // select! {
    //     r = select_all(termination_handles).fuse() => {
    //         System::current().stop();
    //         r.0.map_err(|e| anyhow!(e))
    //     },
    //     _ = terminate.recv().fuse() => {
    //         info!("Caught termination signal");
    //         System::current().stop();
    //         Ok(())
    //     }
    //     _ = interrupt.recv().fuse() => {
    //         info!("Caught interrupt signal");
    //         System::current().stop();
    //         Ok(())
    //     }
    //     _ = userint.recv().fuse() => {
    //         info!("Caught user int signal");
    //         System::current().stop();
    //         Ok(())
    //     }
    // }
}

fn connectivity_checker(interval: u64) { actix::spawn(run_connectivity_checker(interval)); }

fn file_actor(settings: AvroFileLoggerSettings) -> Addr<AvroFileActor<MarketEventEnvelope>> {
    info!("starting avro file logger");
    SyncArbiter::start(settings.parallelism.unwrap_or(2), move || {
        let dir = Path::new(settings.basedir.as_str());
        fs::create_dir_all(&dir).unwrap();
        AvroFileActor::new(&FileActorOptions {
            base_dir: dir.to_str().unwrap().to_string(),
            max_file_size: settings.file_rotation.max_file_size,
            max_file_time: settings.file_rotation.max_file_time,
            partitioner: Rc::new(MarketEventPartitioner::new(settings.partitions_grace_period)),
        })
    })
}

#[tracing::instrument(skip(settings, engine), level = "info")]
async fn make_traders(settings: Arc<RwLock<Settings>>, engine: Arc<TradingEngine>) -> Vec<Trader> {
    let settings_v = settings.read().await;
    let mut drivers_settings = settings_v.strategies.clone();
    drivers_settings.extend(
        settings_v
            .strategies_copy
            .iter()
            .flat_map(StrategyCopySettings::all)
            .flatten(),
    );
    let storage = Arc::new(settings_v.storage.clone());
    let traders = futures::future::join_all(drivers_settings.into_iter().map(move |driver_settings| {
        let db = storage.clone();
        let actor_options = settings_v.strat_actor.clone();
        let arc = engine.clone();
        async move {
            Trader::try_new(
                plugin_registry(),
                db.as_ref(),
                &actor_options,
                &driver_settings,
                arc,
                None,
            )
            .unwrap()
        }
    }))
    .await;
    traders
}

pub async fn poll_actor<T: Actor>(addr: Addr<T>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        addr.connected();
    }
}
