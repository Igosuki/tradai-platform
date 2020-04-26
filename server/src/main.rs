#![feature(try_trait)]

extern crate clap;
extern crate coinnect_rt;
#[cfg(feature = "flame_it")]
extern crate flame;
#[cfg(feature = "flame_it")]
#[macro_use]
extern crate flamer;
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate trader;

use actix_rt::System;
use std::collections::HashMap;
#[cfg(feature = "flame_it")]
use std::fs::File;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::mpsc::channel;
use std::sync::{Arc, RwLock};
use std::{fs, io};

use futures::{pin_mut, select, FutureExt};

use actix::{Actor, Addr, Recipient, SyncArbiter};
use actix_rt::signal::unix::{signal, Signal, SignalKind};
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use structopt::StructOpt;

use coinnect_rt::binance::BinanceCreds;
use coinnect_rt::bitstamp::BitstampCreds;
use coinnect_rt::bittrex::BittrexCreds;
use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::exchange::{Exchange, ExchangeSettings};
use coinnect_rt::exchange_bot::ExchangeBot;
use coinnect_rt::metrics::PrometheusPushActor;
use coinnect_rt::types::LiveEventEnveloppe;
#[cfg(feature = "gprof")]
use gperftools::heap_profiler::HEAP_PROFILER;
use std::time::Duration;
use strategies::{self, StrategyActor, StrategyActorOptions};
use trader::logging::file_actor::{AvroFileActor, FileActorOptions};
use trader::settings::{self, Settings};
use trader::{logging, server};

//lazy_static! {
//    static ref CONFIG_FILE: String = {
//        let trader_env : String = std::env::var("TRADER_ENV").unwrap_or("development".to_string());
//        format!("config/{}.yaml", trader_env)
//    };
//}
//
//lazy_static! {
//    static ref SETTINGS: RwLock<Config> = RwLock::new({
//        let mut settings = Config::default();
//        settings.merge(File::with_name(&CONFIG_FILE)).unwrap();
//
//        settings
//    });
//}
//
//fn show() {
//    println!(" * Settings :: \n\x1b[31m{:?}\x1b[0m",
//             SETTINGS
//                 .read()
//                 .unwrap()
//                 .clone()
//                 .try_into::<HashMap<String, String>>()
//                 .unwrap());
//}
#[derive(StructOpt, Debug)]
#[structopt(name = "basic")]
struct Opts {
    #[structopt(short, long)]
    debug: bool,
}

// TODO : clean up the ugly code for settings access
#[actix_rt::main]
#[cfg_attr(feature = "flame_it", flame)]
async fn main() -> io::Result<()> {
    #[cfg(feature = "gprof")]
    HEAP_PROFILER
        .lock()
        .unwrap()
        .start("./trader.hprof")
        .unwrap();

    // Logging, App config
    env_logger::init();
    let opts: Opts = Opts::from_args();
    let env = std::env::var("TRADER_ENV").unwrap_or("development".to_string());
    let settings =
        Arc::new(RwLock::new(settings::Settings::new(env).map_err(|e| {
            std::io::Error::new(std::io::ErrorKind::InvalidData, e)
        })?));

    // Create a channel to receive the events.
    let (tx, _rx) = channel();
    let mut watcher: RecommendedWatcher =
        Watcher::new(tx, std::time::Duration::from_secs(2)).unwrap();

    watcher
        .watch(
            &settings.read().unwrap().__config_file,
            RecursiveMode::NonRecursive,
        )
        .unwrap();
    let arc = Arc::clone(&settings);
    let arc1 = arc.clone();
    if opts.debug {
        arc1.write().unwrap().sanitize();
        process::exit(0x0100);
    }
    let settings_v = arc1.read().unwrap();

    if settings_v.profile_main {
        #[cfg(feature = "flame_it")]
        flame::start("main bot");
    }

    // Live Events recipients
    let mut recipients: Vec<Recipient<LiveEventEnveloppe>> = Vec::new();
    // File actor that logs avro beans for all events
    let fa = file_actor(settings.clone()).recipient();
    recipients.push(fa);
    // Strategy actors, cf strategy crate
    let arc = Arc::clone(&settings);
    let arc1 = arc.clone();
    let strat_actors: Vec<Recipient<LiveEventEnveloppe>> = strategy_actors(arc1)
        .into_iter()
        .map(|a| a.recipient())
        .collect();
    for actor in strat_actors {
        recipients.push(actor);
    }

    // Metrics pusher
    let _prom_push = PrometheusPushActor::start(PrometheusPushActor::new(&settings_v.prom_push_gw));

    // Exchange bot actors, they just receive data
    let keys_path = PathBuf::from(settings_v.keys.clone());
    let exchanges = settings_v.exchanges.clone();
    let bots = exchange_bots(exchanges.clone(), keys_path.clone(), recipients).await;

    // API Server
    let server = server::httpserver(exchanges.clone(), keys_path.clone());

    // Handle interrupts for graceful shutdown
    // await_termination().await?;
    select! {
        _ = server.fuse() => println!("Http Server terminated."),
        // ping all bots at regular intervals
        _ = poll_bots(bots).fuse() => println!("Stopped polling bots."),
    };
    if settings_v.profile_main {
        #[cfg(feature = "gprof")]
        HEAP_PROFILER.lock().unwrap().stop().unwrap();
    }
    System::current().stop();
    info!("Caught interrupt and stopped the system");

    if settings_v.profile_main {
        #[cfg(feature = "flame_it")]
        flame::end("main bot");

        #[cfg(feature = "flame_it")]
        flame::dump_html(&mut File::create("flame-graph.html").unwrap()).unwrap();
    }
    Ok(())
}

fn file_actor(settings: Arc<RwLock<Settings>>) -> Addr<AvroFileActor> {
    SyncArbiter::start(2, move || {
        let settings_v = &settings.read().unwrap();
        let data_dir = settings_v.data_dir.clone();
        let dir = Path::new(data_dir.as_str()).clone();
        fs::create_dir_all(&dir).unwrap();
        AvroFileActor::new(&FileActorOptions {
            base_dir: dir.to_str().unwrap().to_string(),
            max_file_size: settings_v.file_rotation.max_file_size,
            max_file_time: settings_v.file_rotation.max_file_time,
            partitioner: logging::live_event_partitioner,
        })
    })
}

fn strategy_actors(settings: Arc<RwLock<Settings>>) -> Vec<Addr<StrategyActor>> {
    println!("creating strat actors");
    let arc = Arc::clone(&settings);
    let arc1 = arc.clone();
    let settings_v = arc1.read().unwrap();
    let string = Arc::new(arc.clone().read().unwrap().db_storage_path.clone());
    let exchanges = Arc::new(arc.clone().read().unwrap().exchanges.clone());
    settings_v
        .strategies
        .clone()
        .into_iter()
        .map(move |strategy| {
            let arc2 = string.clone();
            let arc3 = exchanges.clone();
            SyncArbiter::start(1, move || {
                StrategyActor::new(StrategyActorOptions {
                    strategy: strategies::from_settings(
                        arc2.clone().as_ref(),
                        arc3.get(&strategy.exchange()).unwrap().fees,
                        &strategy,
                    ),
                })
            })
        })
        .collect()
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
                    BinanceCreds::new_from_file("account_binance", keys_path.clone()).unwrap(),
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
        for (_, bot) in &bots {
            bot.ping();
        }
    }
}
