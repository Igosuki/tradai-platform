#![feature(try_trait)]
#![feature(let_chains)]
// This example shows how to use the generic API provided by Coinnect.
// This method is useful if you have to iterate throught multiple accounts of
// different exchanges and perform the same operation (such as get the current account's balance)
// You can also use the Coinnect generic API if you want a better error handling since all methods
// return Result<_, Error>.

extern crate actix;
extern crate actix_derive;
extern crate byte_unit;
extern crate clap;
extern crate coinnect_rt;
extern crate config;
#[cfg(feature = "flame_it")]
extern crate flame;
#[cfg(feature = "flame_it")]
#[macro_use]
extern crate flamer;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate prometheus;
extern crate prometheus_static_metric;
extern crate rand;
extern crate serde_derive;
extern crate uuid;

use std::{fs, io};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::process;
use std::sync::{Arc, RwLock};
use std::sync::mpsc::channel;

use actix::{Actor, Recipient, SyncArbiter};
use actix_rt::System;
use coinnect_rt::binance::BinanceCreds;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::exchange_bot::ExchangeBot;
use coinnect_rt::metrics::PrometheusPushActor;
use coinnect_rt::types::LiveEventEnveloppe;
use notify::{RecommendedWatcher, RecursiveMode, Watcher};
use structopt::StructOpt;
// use actix_rt::signal::unix::Signal;
// use futures::{FutureExt, pin_mut, select};
// use tokio::signal::unix::{signal, SignalKind};

use crate::coinnect_rt::coinnect::Coinnect;
use crate::handlers::file_actor::{AvroFileActor, FileActorOptions};
use coinnect_rt::bittrex::BittrexCreds;
use coinnect_rt::bitstamp::BitstampCreds;
#[cfg(feature = "flame_it")]
use std::fs::File;

pub mod settings;
pub mod avro_gen;
pub mod handlers;
pub mod api;
pub mod server;
pub mod strategies;
pub mod math;
pub mod serdes;

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
    debug: bool
}

#[actix_rt::main]
#[cfg_attr(feature = "flame_it", flame)]
async fn main() -> io::Result<()> {
    env_logger::init();
    let opts: Opts = Opts::from_args();
    let env = std::env::var("TRADER_ENV").unwrap_or("development".to_string());
    let settings = Arc::new(RwLock::new(settings::Settings::new(env).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?));

    // Create a channel to receive the events.
    let (tx, _rx) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx, std::time::Duration::from_secs(2)).unwrap();

    watcher
        .watch(&settings.read().unwrap().__config_file, RecursiveMode::NonRecursive)
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
    let mut recipients: Vec<Recipient<LiveEventEnveloppe>> = vec![];
    let fa = SyncArbiter::start(1, move || {
        let settings_v = &arc.read().unwrap();
        let data_dir = settings_v.data_dir.clone();
        let dir = Path::new(data_dir.as_str()).clone();
        fs::create_dir_all(&dir).unwrap();
        AvroFileActor::new(&FileActorOptions {
            base_dir: dir.to_str().unwrap().to_string(),
            max_file_size: settings_v.file_rotation.max_file_size,
            max_file_time: settings_v.file_rotation.max_file_time,
            partitioner: handlers::live_event_partitioner,
        })
    });
    let fa2 = fa.clone();
    recipients.push(fa.recipient());

    // Metrics
    let _prom_push = PrometheusPushActor::start(PrometheusPushActor::new());

    //
    let path = PathBuf::from(settings_v.keys.clone());
    let mut bots: HashMap<Exchange, Box<dyn ExchangeBot>> = HashMap::new();
    let exchanges = settings_v.exchanges.clone();
    // TODO : solve the annoying problem of credentials being a specific struct when new_stream and new are generic
    for (xch, conf) in exchanges.clone() {
        let bot = match xch {
            Exchange::Bittrex => {
                let creds = Box::new(BittrexCreds::new_from_file("account_bittrex", path.clone()).unwrap());
                Coinnect::new_stream(xch, creds.clone(), conf, recipients.clone()).await.unwrap()
            }
            Exchange::Bitstamp => {
                let creds = Box::new(BitstampCreds::new_from_file("account_bitstamp", path.clone()).unwrap());
                Coinnect::new_stream(xch, creds.clone(), conf, recipients.clone()).await.unwrap()
            }
            Exchange::Binance => {
                let creds = Box::new(BinanceCreds::new_from_file("account_binance", path.clone()).unwrap());
                Coinnect::new_stream(xch, creds.clone(), conf, recipients.clone()).await.unwrap()
            }
            _ => unimplemented!()
        };
        bots.insert(xch, bot);
    }


    let server = server::httpserver(exchanges.clone(), path.clone());
    // Handle interrupts for graceful shutdown
    // let mut stream: Signal = signal(SignalKind::terminate())?;
    // let mut stream: Signal = signal(SignalKind::interrupt())?;
    // let mut stream2: Signal = signal(SignalKind::user_defined1())?;
    // let mut t1 = stream.recv().fuse();
    // let mut t2 = stream2.recv().fuse();
    // pin_mut!(t1, t2);
    //
    // select! {
    //     _ = t1 => info!("Interrupt"),
    //     _ = t2 => info!("SigUSR1"),
    // }
    let server = server.await;
    drop(bots);
    drop(recipients);
    drop(fa2);
    System::current().stop();
    info!("Caught interrupt and stopped the system");

    if settings_v.profile_main {
        #[cfg(feature = "flame_it")]
            flame::end("main bot");

        #[cfg(feature = "flame_it")]
            flame::dump_html(&mut File::create("flame-graph.html").unwrap()).unwrap();
    }

    server
}
