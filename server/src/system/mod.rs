use std::collections::HashMap;
use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, Addr, Handler, Recipient, SyncArbiter};
use futures::{future::select_all, select, FutureExt};

use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::exchange::{Exchange, ExchangeApi, ExchangeSettings};
use coinnect_rt::exchange_bot::{ExchangeBot, Ping};
use coinnect_rt::metrics::PrometheusPushActor;
use coinnect_rt::types::{AccountEventEnveloppe, LiveEventEnveloppe};
use strategies::order_manager::OrderManager;
use strategies::{self, Strategy, StrategyKey};

use crate::logging::file_actor::{AvroFileActor, FileActorOptions};
use crate::logging::live_event::LiveEventPartitioner;
use crate::nats::{NatsConsumer, NatsProducer, Subject};
use crate::server;
use crate::settings::{AvroFileLoggerSettings, OutputSettings, Settings, StreamSettings};
use actix::dev::ToEnvelope;
use portfolio::balance::{BalanceReporter, BalanceReporterOptions};
use std::rc::Rc;
use tokio::signal::unix::{signal, SignalKind};

pub mod bots;

pub struct BotAndActorHandles<T: Actor> {
    pub act: Addr<T>,
    pub bot: Vec<Box<dyn ExchangeBot>>,
}

impl<T> BotAndActorHandles<T>
where
    T: Actor + Handler<Ping>,
    <T as Actor>::Context: ToEnvelope<T, Ping>,
{
    fn ping(&self) {
        for bot in &self.bot {
            bot.ping();
        }

        self.act.do_send(coinnect_rt::exchange_bot::Ping);
    }
}

pub async fn start(settings: Arc<RwLock<Settings>>) -> anyhow::Result<()> {
    let settings_v = settings.read().unwrap();
    let exchanges = settings_v.exchanges.clone();
    let mut termination_handles: Vec<Pin<Box<dyn Future<Output = std::io::Result<()>>>>> = vec![];
    let mut output_recipients: Vec<Recipient<LiveEventEnveloppe>> = Vec::new();
    let mut strat_recipients: Vec<Recipient<LiveEventEnveloppe>> = Vec::new();
    let mut strategy_actors = vec![];
    let mut order_managers_addr = HashMap::new();

    let db_path_str = Arc::new(settings_v.db_storage_path.clone());
    let keys_path = PathBuf::from(settings_v.keys.clone());
    if fs::metadata(keys_path.clone()).is_err() {
        return Err(anyhow!("key file doesn't exist at {:?}", keys_path.clone()));
    }

    let exchanges_conf = Arc::new(exchanges.clone());
    let apis = Arc::new(Coinnect::build_exchange_apis(exchanges_conf.clone(), keys_path.clone()).await);
    // Temporarily load symbol cache from here
    Coinnect::load_pair_registries(apis.clone()).await?;

    // strategies, cf strategies crate
    let settings_arc = Arc::clone(&settings);

    for output in settings_v.outputs.clone() {
        match output {
            OutputSettings::AvroFileLogger(logger_settings) => {
                output_recipients.push(file_actor(logger_settings).recipient())
            }
            OutputSettings::Nats(nats_settings) => {
                let producer = NatsProducer::new(&nats_settings.host, &nats_settings.username, &nats_settings.password)
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotConnected, e))?;
                output_recipients.push(NatsProducer::start(producer).recipient())
            }
            OutputSettings::Strategies => {
                let oms = Arc::new(order_managers(keys_path.clone(), &db_path_str, exchanges_conf.clone()).await?);
                if !oms.is_empty() {
                    termination_handles.push(Box::pin(bots::poll_account_bots(oms.clone())));
                    for (&xchg, om_system) in oms.clone().iter() {
                        order_managers_addr.insert(xchg, om_system.act.clone());
                    }
                }
                let strategies = strategies(settings_arc.clone(), oms.clone()).await;
                for a in strategies.clone() {
                    strat_recipients.push(a.1.clone().recipient());
                    strategy_actors.push(a.clone());
                }
            }
        }
    }

    // balance reporter
    if let Some(balance_reporter_opts) = &settings_v.balance_reporter {
        let bot = balance_reporter(
            balance_reporter_opts,
            apis.clone(),
            exchanges_conf.clone(),
            keys_path.clone(),
        )
        .await?;
        termination_handles.push(Box::pin(bots::poll_account_bot(bot)));
    }

    // metrics actor
    let _prom_push = PrometheusPushActor::start(PrometheusPushActor::new(&settings_v.prometheus));

    for stream_settings in &settings_v.streams {
        match stream_settings {
            StreamSettings::ExchangeBots => {
                let mut all_recipients = vec![];
                all_recipients.extend(strat_recipients.clone());
                all_recipients.extend(output_recipients.clone());
                let bots = bots::exchange_bots(exchanges_conf.clone(), keys_path.clone(), all_recipients).await?;
                if !bots.is_empty() {
                    termination_handles.push(Box::pin(bots::poll_bots(bots)));
                }
            }
            StreamSettings::Nats(nats_settings) => {
                // For now, give each strategy a nats consumer
                for strategy in strategy_actors.clone() {
                    let topics = strategy
                        .2
                        .iter()
                        .map(|c| <LiveEventEnveloppe as Subject>::from_channel(c))
                        .collect();
                    let consumer = NatsConsumer::start(
                        NatsConsumer::new::<LiveEventEnveloppe>(
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
                if !output_recipients.is_empty() {
                    let consumer = NatsConsumer::start(
                        NatsConsumer::new(
                            &nats_settings.host,
                            &nats_settings.username,
                            &nats_settings.username,
                            vec![<LiveEventEnveloppe as Subject>::glob()],
                            output_recipients.clone(),
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
        apis.clone(),
        strats_map,
        Arc::new(order_managers_addr),
        settings_v.api.port.0,
    );
    termination_handles.push(Box::pin(server));

    // Handle interrupts for graceful shutdown
    let mut terminate = signal(SignalKind::terminate())?;
    let mut interrupt = signal(SignalKind::interrupt())?;
    let mut userint = signal(SignalKind::user_defined1())?;
    select! {
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
    }
}

fn file_actor(settings: AvroFileLoggerSettings) -> Addr<AvroFileActor<LiveEventEnveloppe>> {
    SyncArbiter::start(2, move || {
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

async fn strategies(
    settings: Arc<RwLock<Settings>>,
    oms: Arc<HashMap<Exchange, BotAndActorHandles<OrderManager>>>,
) -> Vec<Strategy> {
    println!("creating strat actors");
    let arc = Arc::clone(&settings);
    let arc1 = arc.clone();
    let settings_v = arc1.read().unwrap();
    let db_path_str = Arc::new(arc.read().unwrap().db_storage_path.clone());
    let exchanges = Arc::new(arc.read().unwrap().exchanges.clone());
    settings_v
        .strategies
        .clone()
        .into_iter()
        .map(move |strategy_settings| {
            let db_path_a = db_path_str.clone();
            let exchanges_conf = exchanges.clone();
            let fees = exchanges_conf.get(&strategy_settings.exchange()).unwrap().fees;
            let order_manager = oms.get(&strategy_settings.exchange());
            Strategy::new(
                db_path_a,
                fees,
                strategy_settings,
                order_manager.map(|sys| sys.act.clone()),
            )
        })
        .collect()
}

async fn balance_reporter(
    options: &BalanceReporterOptions,
    apis: Arc<HashMap<Exchange, Box<dyn ExchangeApi>>>,
    exchanges: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
) -> anyhow::Result<BotAndActorHandles<BalanceReporter>> {
    let mut bots: Vec<Box<dyn ExchangeBot>> = vec![];
    let balance_reporter = BalanceReporter::new(apis.clone(), options.clone());
    let balance_reporter_addr = BalanceReporter::start(balance_reporter);
    for (xch, conf) in exchanges.iter() {
        if !conf.use_account {
            continue;
        }
        let recipients: Vec<Recipient<AccountEventEnveloppe>> = vec![balance_reporter_addr.clone().recipient()];
        let creds = Coinnect::credentials_for(*xch, keys_path.clone())?;
        let bot = Coinnect::new_account_stream(*xch, creds, recipients, conf.use_test).await?;
        bots.push(bot);
    }
    Ok(BotAndActorHandles {
        act: balance_reporter_addr,
        bot: bots,
    })
}

/// Get an order manager for each exchange
/// N.B.: Does not currently use test mode
async fn order_managers(
    keys_path: PathBuf,
    db_path: &str,
    exchanges: Arc<HashMap<Exchange, ExchangeSettings>>,
) -> anyhow::Result<HashMap<Exchange, BotAndActorHandles<OrderManager>>> {
    let mut bots: HashMap<Exchange, BotAndActorHandles<OrderManager>> = HashMap::new();
    for (xch, conf) in exchanges.iter() {
        if !conf.use_account {
            continue;
        }
        let api = Coinnect::build_exchange_api(keys_path.clone(), xch, conf.use_test)
            .await
            .unwrap();
        let om_db_path = format!("{}/om_{}", db_path, xch);
        let order_manager = OrderManager::new(Arc::new(api), Path::new(&om_db_path));
        let order_manager_addr = OrderManager::start(order_manager);
        let recipients: Vec<Recipient<AccountEventEnveloppe>> = vec![order_manager_addr.clone().recipient()];
        let creds = Coinnect::credentials_for(*xch, keys_path.clone())?;
        let bot = Coinnect::new_account_stream(*xch, creds, recipients, conf.use_test).await?;
        bots.insert(*xch, BotAndActorHandles {
            act: order_manager_addr.clone(),
            bot: vec![bot],
        });
    }
    Ok(bots)
}

pub async fn poll<T: Actor>(addr: Addr<T>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        addr.connected();
    }
}
