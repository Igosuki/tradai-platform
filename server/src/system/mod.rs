use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, Addr, Recipient, SyncArbiter};
use futures::future::select_all;
//use tokio::signal::unix::{signal, SignalKind};
use tracing::Instrument;

use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::exchange::manager::ExchangeManager;
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::{AccountEventEnveloppe, LiveEventEnvelope};
use db::DbOptions;
use metrics::prom::PrometheusPushActor;
use portfolio::balance::{BalanceReporter, BalanceReporterOptions};
use strategies::order_manager::OrderManager;
use strategies::{self, Strategy, StrategyKey};

use crate::logging::file_actor::{AvroFileActor, FileActorOptions};
use crate::logging::live_event::LiveEventPartitioner;
use crate::nats::{NatsConsumer, NatsProducer, Subject};
use crate::server;
use crate::settings::{AvroFileLoggerSettings, OutputSettings, Settings, StreamSettings};

pub mod bots;

pub async fn start(settings: Arc<RwLock<Settings>>) -> anyhow::Result<()> {
    let settings_v = settings.read().unwrap();
    let exchanges = settings_v.exchanges.clone();

    let keys_path = PathBuf::from(settings_v.keys.clone());
    if fs::metadata(keys_path.clone()).is_err() {
        return Err(anyhow!("key file doesn't exist at {:?}", keys_path.clone()));
    }

    let exchanges_conf = Arc::new(exchanges.clone());
    let manager = Coinnect::new_manager();
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
    let mut broadcast_recipients: Vec<Recipient<Arc<LiveEventEnvelope>>> = Vec::new();
    let mut strat_recipients: Vec<Recipient<Arc<LiveEventEnvelope>>> = Vec::new();
    let mut account_recipients: HashMap<Exchange, Vec<Recipient<AccountEventEnveloppe>>> =
        apis.keys().map(|xch| (*xch, vec![])).collect();
    let mut strategy_actors = vec![];
    let mut order_managers_addr = HashMap::new();

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
                let oms = Arc::new(order_managers(&settings_v.storage, &manager).await?);
                if !oms.is_empty() {
                    termination_handles.push(Box::pin(bots::poll_pingables(
                        oms.values().map(|addr| addr.clone().recipient()).collect(),
                    )));
                    for (xchg, addr) in oms.clone().iter() {
                        account_recipients.get_mut(xchg).unwrap().push(addr.clone().recipient());
                        order_managers_addr.insert(*xchg, addr.clone());
                    }
                }
                let strategies = strategies(settings_arc.clone(), oms.clone())
                    .instrument(tracing::info_span!("starting strategies"))
                    .await;
                for a in strategies.clone() {
                    strat_recipients.push(a.1.clone().recipient());
                    strategy_actors.push(a.clone());
                }
            }
        }
    }

    // balance reporter
    if let Some(balance_reporter_opts) = &settings_v.balance_reporter {
        info!("starting balance reporter");
        let reporter_addr = balance_reporter(balance_reporter_opts, apis.clone()).await?;
        for xch in apis.keys() {
            account_recipients
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
                let bots =
                    bots::account_bots(exchanges_conf.clone(), keys_path.clone(), account_recipients.clone()).await?;
                if !bots.is_empty() {
                    termination_handles.push(Box::pin(bots::poll_bots(bots)));
                }
            }
            StreamSettings::Nats(nats_settings) => {
                info!("nats consumers");
                // For now, give each strategy a nats consumer
                for strategy in strategy_actors.clone() {
                    let topics = strategy
                        .2
                        .iter()
                        .map(|c| <LiveEventEnvelope as Subject>::from_channel(c))
                        .collect();
                    let consumer = NatsConsumer::start(
                        NatsConsumer::new::<Arc<LiveEventEnvelope>>(
                            &nats_settings.host,
                            &nats_settings.username,
                            &nats_settings.username,
                            topics,
                            vec![strategy.1.recipient()],
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
                            vec![<LiveEventEnvelope as Subject>::glob()],
                            broadcast_recipients.clone(),
                        )
                        .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?,
                    );
                    termination_handles.push(Box::pin(poll(consumer)));
                }
            }
        }
    }

    let strats_map: Arc<HashMap<StrategyKey, Strategy>> = Arc::new(
        strategy_actors
            .clone()
            .iter()
            .map(|s| (s.0.clone(), s.clone()))
            .collect(),
    );
    // API Server
    let server = server::httpserver(
        &settings_v.api,
        settings_v.version.clone(),
        apis.clone(),
        strats_map,
        Arc::new(order_managers_addr),
    );
    termination_handles.push(Box::pin(server));

    // Handle interrupts for graceful shutdown
    // let mut terminate = signal(SignalKind::terminate())?;
    // let mut interrupt = signal(SignalKind::interrupt())?;
    // let mut userint = signal(SignalKind::user_defined1())?;
    let x = select_all(termination_handles).await.0.map_err(|e| anyhow!(e));
    x

    /*select! {
        r = select_all(termination_handles).fuse() => {
            r.0.map_err(|e| anyhow!(e))
        },
        _ = terminate.recv().fuse() => {
            info!("Caught termination signal");
            Ok(())
        }
        _ = interrupt.recv().fuse() => {
            info!("Caught interrupt signal");
            Ok(())
        }
        _ = userint.recv().fuse() => {
            info!("Caught user int signal");
            Ok(())
        }
    }*/
}

fn file_actor(settings: AvroFileLoggerSettings) -> Addr<AvroFileActor<LiveEventEnvelope>> {
    info!("starting avro file logger");
    SyncArbiter::start(settings.parallelism.unwrap_or(2), move || {
        let dir = Path::new(settings.basedir.as_str());
        fs::create_dir_all(&dir).unwrap();
        AvroFileActor::new(&FileActorOptions {
            base_dir: dir.to_str().unwrap().to_string(),
            max_file_size: settings.file_rotation.max_file_size,
            max_file_time: settings.file_rotation.max_file_time,
            partitioner: Rc::new(LiveEventPartitioner::new(settings.partitions_grace_period)),
        })
    })
}

#[tracing::instrument(skip(settings, oms), level = "info")]
async fn strategies(settings: Arc<RwLock<Settings>>, oms: Arc<HashMap<Exchange, Addr<OrderManager>>>) -> Vec<Strategy> {
    let arc = Arc::clone(&settings);
    let arc1 = arc.clone();
    let settings_v = arc1.read().unwrap();
    let exchanges = Arc::new(arc.read().unwrap().exchanges.clone());
    let mut strategies = settings_v.strategies.clone();
    strategies.extend(settings_v.strategies_copy.iter().map(|sc| sc.all()).flatten().flatten());
    let storage = Arc::new(settings_v.storage.clone());
    let strats = futures::future::join_all(strategies.into_iter().map(move |strategy_settings| {
        let exchanges_conf = exchanges.clone();
        let exchange = strategy_settings.exchange();
        let fees = exchanges_conf.get(&exchange).unwrap().fees;
        let oms = oms.clone();
        let db = storage.clone();
        async move { Strategy::new(db.as_ref(), fees, &strategy_settings, oms.get(&exchange).cloned()) }
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

/// Get an order manager for each exchange
/// N.B.: Does not currently use test mode
async fn order_managers(
    db: &DbOptions<String>,
    exchange_manager: &ExchangeManager,
) -> anyhow::Result<HashMap<Exchange, Addr<OrderManager>>> {
    let mut oms: HashMap<Exchange, Addr<OrderManager>> = HashMap::new();
    for (xch, api) in exchange_manager.exchange_apis().iter() {
        let om_db_path = format!("om_{}", xch);
        let order_manager = OrderManager::new(api.clone(), db, om_db_path);
        oms.insert(*xch, OrderManager::start(order_manager));
    }
    Ok(oms)
}

pub async fn poll<T: Actor>(addr: Addr<T>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        addr.connected();
    }
}
