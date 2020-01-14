#![feature(try_trait)]
// This example shows how to use the generic API provided by Coinnect.
// This method is useful if you have to iterate throught multiple accounts of
// different exchanges and perform the same operation (such as get the current account's balance)
// You can also use the Coinnect generic API if you want a better error handling since all methods
// return Result<_, Error>.

extern crate actix;
extern crate actix_derive;
extern crate byte_unit;
extern crate coinnect_rt;
extern crate config;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
extern crate serde_derive;
extern crate uuid;
extern crate rand;

use std::{fs, io};


use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::sync::mpsc::channel;

use actix::{Actor, Recipient};

use actix_rt::{Arbiter, System};

use coinnect_rt::bittrex::BittrexCreds;
use coinnect_rt::exchange_bot::{ExchangeBot};

use coinnect_rt::types::LiveEventEnveloppe;

use notify::{RecommendedWatcher, RecursiveMode, Watcher};

use crate::coinnect_rt::coinnect::Coinnect;
use crate::coinnect_rt::exchange::Exchange::*;

use crate::handlers::file_actor::{AvroFileActor, FileActorOptions};
use std::collections::HashMap;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::bitstamp::BitstampCreds;

pub mod settings;
pub mod avro_gen;
pub mod handlers;
pub mod api;

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

#[actix_rt::main]
async fn main() -> io::Result<()> {
    env_logger::init();
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
    let settings_v = arc1.read().unwrap();

    let mut recipients: Vec<Recipient<LiveEventEnveloppe>> = vec![];
    let fa = AvroFileActor::create(|_ctx| {
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
    recipients.push(fa.recipient());

    let path = PathBuf::from(settings_v.keys.clone());
    let mut bots : HashMap<Exchange, Box<ExchangeBot>> = HashMap::new();

    let exchanges = settings_v.exchanges.clone();
    for (xch, conf) in exchanges {
        match xch {
            Exchange::Bittrex => {
                let my_creds = BittrexCreds::new_from_file("account_bittrex", path.clone()).unwrap();
                let bot: Box<ExchangeBot> = Coinnect::new_stream(Bittrex, my_creds, conf,recipients.clone()).await.unwrap();
                bots.insert(Exchange::Bittrex, bot);
            },
            Exchange::Bitstamp => {
                let my_creds = BitstampCreds::new_from_file("account_bitstamp", path.clone()).unwrap();
                let bot = Coinnect::new_stream(Bitstamp, my_creds, conf,recipients.clone()).await.unwrap();
                bots.insert(Exchange::Bitstamp, bot);
            }
            _ => unimplemented!()
        }
    }

    tokio::signal::ctrl_c().await
}
