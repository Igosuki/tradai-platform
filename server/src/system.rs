use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use futures::{pin_mut, select, FutureExt};

use actix::{Actor, Addr, Recipient, SyncArbiter};
use actix_rt::signal::unix::{signal, Signal, SignalKind};
use std::time::Duration;

use coinnect_rt::binance::BinanceCreds;
use coinnect_rt::bitstamp::BitstampCreds;
use coinnect_rt::bittrex::BittrexCreds;
use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::exchange::{Exchange, ExchangeSettings};
use coinnect_rt::exchange_bot::ExchangeBot;
use coinnect_rt::metrics::PrometheusPushActor;
use coinnect_rt::types::{AccountEventEnveloppe, LiveEventEnveloppe};
use strategies::{self, Strategy, StrategyKey};

use crate::logging::file_actor::{AvroFileActor, FileActorOptions};
use crate::settings::Settings;
use crate::{logging, server};
use strategies::order_manager::OrderManager;

pub struct AccountSystem {
    pub om: Addr<OrderManager>,
    pub bot: Box<dyn ExchangeBot>,
}

impl AccountSystem {
    fn ping(&self) {
        self.bot.ping();
        self.om.do_send(coinnect_rt::exchange_bot::Ping);
    }
}

pub async fn start(settings: Arc<RwLock<Settings>>) -> std::io::Result<()> {
    let settings_v = settings.read().unwrap();
    // Live Events recipients
    let mut live_events_recipients: Vec<Recipient<LiveEventEnveloppe>> = Vec::new();
    // File actor that logs avro beans for all events
    let fa = file_actor(settings.clone()).recipient();
    live_events_recipients.push(fa);
    // Order Managers actors
    let db_path_str = Arc::new(settings_v.db_storage_path.clone());
    let keys_path = PathBuf::from(settings_v.keys.clone());
    let oms = Arc::new(
        order_managers(
            keys_path,
            &db_path_str,
            Arc::new(settings_v.exchanges.clone()),
        )
        .await,
    );
    // Strategy actors, cf strategy crate
    let arc = Arc::clone(&settings);
    let arc1 = arc.clone();
    let strategies = strategies(arc1, oms.clone()).await;

    for a in strategies.clone() {
        live_events_recipients.push(a.1.recipient().clone());
    }
    // Metrics pusher
    let _prom_push = PrometheusPushActor::start(PrometheusPushActor::new(
        &settings_v.prom_push_gw,
        &settings_v.prom_instance,
    ));

    // Exchange bot actors, they just receive data
    let keys_path = PathBuf::from(settings_v.keys.clone());
    let exchanges = settings_v.exchanges.clone();
    let bots = exchange_bots(exchanges.clone(), keys_path.clone(), live_events_recipients).await;
    let strats_map: HashMap<StrategyKey, Strategy> = strategies
        .clone()
        .iter()
        .map(|s| (s.0.clone(), s.clone()))
        .collect();
    let strats_map_a = Arc::new(strats_map);
    // API Server
    let server = server::httpserver(
        exchanges.clone(),
        strats_map_a,
        keys_path.clone(),
        settings_v.api.port.0,
    );

    // Handle interrupts for graceful shutdown
    // await_termination().await?;
    select! {
        _ = server.fuse() => println!("Http Server terminated."),
        // ping all bots at regular intervals
        _ = poll_bots(bots).fuse() => println!("Stopped polling bots."),
        // ping all account bots at regular intervals
        _ = poll_account_bots(oms).fuse() => println!("Stopped polling order bots."),
    };
    Ok(())
}

fn file_actor(settings: Arc<RwLock<Settings>>) -> Addr<AvroFileActor> {
    SyncArbiter::start(2, move || {
        let settings_v = &settings.read().unwrap();
        let data_dir = settings_v.data_dir.clone();
        let dir = Path::new(data_dir.as_str());
        fs::create_dir_all(&dir).unwrap();
        AvroFileActor::new(&FileActorOptions {
            base_dir: dir.to_str().unwrap().to_string(),
            max_file_size: settings_v.file_rotation.max_file_size,
            max_file_time: settings_v.file_rotation.max_file_time,
            partitioner: logging::live_event_partitioner,
        })
    })
}

async fn strategies(
    settings: Arc<RwLock<Settings>>,
    oms: Arc<HashMap<Exchange, AccountSystem>>,
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
        .map(move |strategy| {
            let db_path_a = db_path_str.clone();
            let exchanges_conf = exchanges.clone();
            let fees = exchanges_conf.get(&strategy.exchange()).unwrap().fees;
            let order_manager = oms.get(&strategy.exchange());
            Strategy::new(
                db_path_a,
                fees,
                strategy,
                order_manager.map(|sys| sys.om.clone()),
            )
        })
        .collect()
}

/// Get an order manager for each exchange
async fn order_managers(
    keys_path: PathBuf,
    db_path: &str,
    exchanges: Arc<HashMap<Exchange, ExchangeSettings>>,
) -> HashMap<Exchange, AccountSystem> {
    let mut bots: HashMap<Exchange, AccountSystem> = HashMap::new();
    for (xch, conf) in exchanges.iter() {
        if !conf.use_account {
            continue;
        }
        let api = server::build_exchange_api(keys_path.clone().into(), &xch);
        let om_path = format!("{}/om_{}", db_path, xch);
        let order_manager = OrderManager::new(Arc::new(api), Path::new(&om_path));
        let order_manager_addr = OrderManager::start(order_manager);
        let recipients: Vec<Recipient<AccountEventEnveloppe>> =
            vec![order_manager_addr.clone().recipient()];
        let bot = match xch {
            Exchange::Binance => {
                let creds = Box::new(
                    BinanceCreds::new_from_file(
                        coinnect_rt::binance::credentials::ACCOUNT_KEY,
                        keys_path.clone(),
                    )
                    .unwrap(),
                );
                Coinnect::new_account_stream(*xch, creds.clone(), recipients)
                    .await
                    .unwrap()
            }
            _ => {
                error!("Account streams are unsupported for {}", xch);
                unimplemented!()
            }
        };
        bots.insert(
            *xch,
            AccountSystem {
                om: order_manager_addr.clone(),
                bot,
            },
        );
    }
    bots
}

async fn exchange_bots(
    exchanges_settings: HashMap<Exchange, ExchangeSettings>,
    keys_path: PathBuf,
    recipients: Vec<Recipient<LiveEventEnveloppe>>,
) -> HashMap<Exchange, Box<dyn ExchangeBot>> {
    let mut bots: HashMap<Exchange, Box<dyn ExchangeBot>> = HashMap::new();
    // TODO : solve the annoying problem of credentials being a specific struct when new_stream and new are generic
    for (xch, conf) in exchanges_settings {
        let bot = match xch {
            Exchange::Bittrex => {
                let creds = Box::new(
                    BittrexCreds::new_from_file("account_bittrex", keys_path.clone()).unwrap(),
                );
                Coinnect::new_stream(xch, creds.clone(), conf, recipients.clone())
                    .await
                    .unwrap()
            }
            Exchange::Bitstamp => {
                let creds = Box::new(
                    BitstampCreds::new_from_file("account_bitstamp", keys_path.clone()).unwrap(),
                );
                Coinnect::new_stream(xch, creds.clone(), conf, recipients.clone())
                    .await
                    .unwrap()
            }
            Exchange::Binance => {
                let creds = Box::new(
                    BinanceCreds::new_from_file(
                        coinnect_rt::binance::credentials::ACCOUNT_KEY,
                        keys_path.clone(),
                    )
                    .unwrap(),
                );
                Coinnect::new_stream(xch, creds.clone(), conf, recipients.clone())
                    .await
                    .unwrap()
            }
            _ => unimplemented!(),
        };
        bots.insert(xch, bot);
    }
    bots
}

#[allow(dead_code)]
async fn await_termination() -> std::io::Result<()> {
    let _stream: Signal = signal(SignalKind::terminate())?;
    let mut stream: Signal = signal(SignalKind::interrupt())?;
    let mut stream2: Signal = signal(SignalKind::user_defined1())?;
    let t1 = stream.recv().fuse();
    let t2 = stream2.recv().fuse();
    pin_mut!(t1, t2);

    select! {
        _ = t1 => info!("Interrupt"),
        _ = t2 => info!("SigUSR1"),
    }
    Ok(())
}

async fn poll_bots(bots: HashMap<Exchange, Box<dyn ExchangeBot>>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        for bot in bots.values() {
            bot.ping();
        }
    }
}

async fn poll_account_bots(systems: Arc<HashMap<Exchange, AccountSystem>>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        for system in systems.values() {
            system.ping();
        }
    }
}
