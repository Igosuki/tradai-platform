use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, Addr, Recipient, SyncArbiter};
use futures::future::select_all;
use tokio::sync::RwLock;
use tracing::Instrument;

// use actix::System;
// use tokio::select;
// use tokio::signal::unix::{signal, SignalKind};
use coinnect_rt::exchange::manager::ExchangeManager;
use coinnect_rt::prelude::*;
use db::DbOptions;
use logging::prelude::*;
use metrics::prom::PrometheusPushActor;
use portfolio::balance::{BalanceReporter, BalanceReporterOptions};
use portfolio::margin::{MarginAccountReporter, MarginAccountReporterOptions};
use strategy::plugin::plugin_registry;
use strategy::{self, StrategyKey, Trader};
use trading::engine::{new_trading_engine, TradingEngine};
use trading::interest::MarginInterestRateProvider;
use trading::order_manager::OrderManager;

use crate::connectivity::run_connectivity_checker;
use crate::nats::{NatsConsumer, NatsProducer, Subject};
use crate::server;
use crate::settings::{AvroFileLoggerSettings, OutputSettings, Settings, StreamSettings};

pub mod bots;

pub async fn start(settings: Arc<RwLock<Settings>>) -> anyhow::Result<()> {
    let settings_v = settings.read().await;
    let exchanges = &settings_v.exchanges;

    let keys_path = PathBuf::from(settings_v.keys.clone());
    fs::metadata(keys_path.clone()).map_err(|_| anyhow!("key file doesn't exist at {:?}", keys_path.clone()))?;

    let exchanges_conf = Arc::new(exchanges.clone());
    let manager = Arc::new(Coinnect::new_manager());
    manager
        .build_exchange_apis(exchanges_conf.clone(), keys_path.clone())
        .await;
    let apis = manager.exchange_apis();
    // Temporarily load symbol cache from here
    // TODO: do this to a read-only memory mapped file somewhere else that is used as a cache
    Coinnect::load_pair_registries(apis.clone())
        .instrument(tracing::info_span!("loading pair registries"))
        .await?;

    let mut termination_handles: Vec<Pin<Box<dyn Future<Output = std::io::Result<()>>>>> = vec![];
    let mut broadcast_recipients: Vec<Recipient<Arc<MarketEventEnvelope>>> = Vec::new();
    let mut strat_recipients: Vec<Recipient<Arc<MarketEventEnvelope>>> = Vec::new();
    let mut spot_account_recipients: HashMap<Exchange, Vec<Recipient<AccountEventEnveloppe>>> =
        apis.keys().map(|xch| (*xch, vec![])).collect();
    let mut margin_account_recipients: HashMap<Exchange, Vec<Recipient<AccountEventEnveloppe>>> =
        apis.keys().map(|xch| (*xch, vec![])).collect();
    let mut traders = vec![];

    // strategies, cf strategies crate
    let settings_arc = Arc::clone(&settings);

    for output in settings_v.outputs.clone() {
        match output {
            OutputSettings::AvroFileLogger(logger_settings) => {
                broadcast_recipients.push(file_actor(logger_settings).recipient())
            }
            OutputSettings::Nats(nats_settings) => {
                let producer = NatsProducer::new(&nats_settings.host, &nats_settings.username, &nats_settings.password)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotConnected, e))?;
                broadcast_recipients.push(NatsProducer::start(producer).recipient())
            }
            OutputSettings::Strategies => {
                let om = order_manager(&settings_v.storage, manager.clone()).await;
                termination_handles.push(Box::pin(bots::poll_pingables(vec![om.clone().recipient()])));
                for xch in manager.exchange_apis().keys() {
                    spot_account_recipients
                        .get_mut(xch)
                        .unwrap()
                        .push(om.clone().recipient());
                }
                let mirp = margin_interest_rate_provider(apis.clone());
                let engine = new_trading_engine(manager.clone(), om, mirp);
                let strategies = strategies(settings_arc.clone(), Arc::new(engine))
                    .instrument(tracing::info_span!("starting strategies"))
                    .await;
                for trader in strategies.clone() {
                    strat_recipients.push(trader.live_event_recipient());
                    traders.push(trader.clone());
                }
            }
        }
    }

    // balance reporter
    if let Some(balance_reporter_opts) = &settings_v.balance_reporter {
        info!("starting balance reporter");
        let reporter_addr = balance_reporter(balance_reporter_opts, apis.clone()).await?;
        for xch in apis.keys() {
            spot_account_recipients
                .get_mut(xch)
                .unwrap()
                .push(reporter_addr.clone().recipient());
        }
        termination_handles.push(Box::pin(bots::poll_pingables(vec![reporter_addr.recipient()])));
    }

    // balance reporter
    if let Some(margin_account_reporter_opts) = &settings_v.margin_account_reporter {
        info!("starting margin account reporter");
        let reporter_addr = margin_account_reporter(margin_account_reporter_opts, apis.clone()).await?;
        for xch in apis.keys() {
            margin_account_recipients
                .get_mut(xch)
                .unwrap()
                .push(reporter_addr.clone().recipient());
        }
        termination_handles.push(Box::pin(bots::poll_pingables(vec![reporter_addr.recipient()])));
    }

    // metrics actor
    let _prom_push = PrometheusPushActor::start(PrometheusPushActor::new(&settings_v.prometheus));

    for stream_settings in &settings_v.streams {
        match stream_settings {
            StreamSettings::ExchangeBots => {
                let mut all_recipients = vec![];
                all_recipients.extend(strat_recipients.clone());
                all_recipients.extend(broadcast_recipients.clone());
                let bots = bots::exchange_bots(exchanges_conf.clone(), keys_path.clone(), all_recipients).await?;
                if !bots.is_empty() {
                    termination_handles.push(Box::pin(bots::poll_bots(bots)));
                }
            }
            StreamSettings::AccountBots => {
                let mut bots = bots::spot_account_bots(
                    exchanges_conf.clone(),
                    keys_path.clone(),
                    spot_account_recipients.clone(),
                )
                .await?;
                let margin_bots = bots::margin_account_bots(
                    exchanges_conf.clone(),
                    keys_path.clone(),
                    margin_account_recipients.clone(),
                )
                .await?;
                bots.extend(margin_bots);
                let isolated_margin_bots = bots::isolated_margin_account_bots(
                    exchanges_conf.clone(),
                    keys_path.clone(),
                    margin_account_recipients.clone(),
                )
                .await?;
                bots.extend(isolated_margin_bots);
                if !bots.is_empty() {
                    termination_handles.push(Box::pin(bots::poll_bots_vec(bots)));
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
                            vec![trader.live_event_recipient()],
                        )
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                    );
                    termination_handles.push(Box::pin(poll(consumer)));
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
                    termination_handles.push(Box::pin(poll(consumer)));
                }
            }
        }
    }

    let strats_map: Arc<HashMap<StrategyKey, Trader>> =
        Arc::new(traders.clone().iter().map(|s| (s.key.clone(), s.clone())).collect());
    // API Server
    let server = server::httpserver(&settings_v.api, settings_v.version.clone(), apis.clone(), strats_map);
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
async fn strategies(settings: Arc<RwLock<Settings>>, engine: Arc<TradingEngine>) -> Vec<Trader> {
    let settings_v = settings.read().await;
    let mut strategies = settings_v.strategies.clone();
    strategies.extend(settings_v.strategies_copy.iter().map(|sc| sc.all()).flatten().flatten());
    let storage = Arc::new(settings_v.storage.clone());
    let strats = futures::future::join_all(strategies.into_iter().map(move |strategy_settings| {
        let db = storage.clone();
        let actor_options = settings_v.strat_actor.clone();
        let arc = engine.clone();
        async move {
            Trader::try_new(
                plugin_registry(),
                db.as_ref(),
                &actor_options,
                &strategy_settings,
                arc,
                None,
            )
            .unwrap()
        }
    }))
    .await;
    strats
}

async fn balance_reporter(
    options: &BalanceReporterOptions,
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
) -> anyhow::Result<Addr<BalanceReporter>> {
    let balance_reporter = BalanceReporter::new(apis.clone(), options.clone());
    let balance_reporter_addr = BalanceReporter::start(balance_reporter);
    Ok(balance_reporter_addr)
}

async fn margin_account_reporter(
    options: &MarginAccountReporterOptions,
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
) -> anyhow::Result<Addr<MarginAccountReporter>> {
    let margin_account_reporter = MarginAccountReporter::new(apis.clone(), options.clone());
    let margin_account_reporter_addr = MarginAccountReporter::start(margin_account_reporter);
    Ok(margin_account_reporter_addr)
}

/// Get an order manager for each exchange
/// N.B.: Does not currently use test mode
async fn order_manager(db: &DbOptions<String>, exchange_manager: Arc<ExchangeManager>) -> Addr<OrderManager> {
    let order_manager = OrderManager::new(exchange_manager.exchange_apis().deref().clone(), db, "order_manager");
    OrderManager::start(order_manager)
}

fn margin_interest_rate_provider(
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
) -> Addr<MarginInterestRateProvider> {
    let provider = MarginInterestRateProvider::new(apis);
    MarginInterestRateProvider::start(provider)
}

pub async fn poll<T: Actor>(addr: Addr<T>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        addr.connected();
    }
}
