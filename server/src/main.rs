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

    trader::system::start(arc).await;
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
