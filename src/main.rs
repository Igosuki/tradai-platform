#![feature(try_trait)]
// This example shows how to use the generic API provided by Coinnect.
// This method is useful if you have to iterate throught multiple accounts of
// different exchanges and perform the same operation (such as get the current account's balance)
// You can also use the Coinnect generic API if you want a better error handling since all methods
// return Result<_, Error>.

extern crate actix;
#[macro_use]
extern crate actix_derive;
#[macro_use]
extern crate byte_unit;
extern crate coinnect_rt;
extern crate config;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
extern crate uuid;

use std::{fs, io, thread};
use std::collections::HashMap;
use std::env::var;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::sync::mpsc::channel;

use actix::{Actor, ActorContext, Addr, AsyncContext, Context, Handler, io::SinkWrite, Recipient, StreamHandler};
use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_rt::{Arbiter, System};
use avro_rs::{Schema, Writer};
use awc::{
    BoxedSocket,
    Client,
    error::WsProtocolError, ws::{Codec, Frame, Message},
};
use byte_unit::Byte;
use bytes::Buf;
use bytes::Bytes;
use chrono::Duration;
use coinnect_rt::bittrex::BittrexCreds;
use coinnect_rt::exchange_bot::{DefaultWsActor, ExchangeBot};
use coinnect_rt::helpers;
use coinnect_rt::types::LiveEvent;
use config::*;
use futures::stream::{SplitSink, StreamExt};
use notify::{DebouncedEvent, RecommendedWatcher, RecursiveMode, Watcher};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::coinnect_rt::coinnect::Coinnect;
use crate::coinnect_rt::exchange::Exchange::*;
use crate::coinnect_rt::types::Pair::*;
use crate::handlers::file_actor::{AvroFileActor, FileActorOptions};

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

fn main() -> io::Result<()> {
    env_logger::init();
    let env = std::env::var("TRADER_ENV").unwrap_or("development".to_string());
    let settings = Arc::new(RwLock::new(settings::Settings::new(env).map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?));

    // Create a channel to receive the events.
    let (tx, rx) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx, std::time::Duration::from_secs(2)).unwrap();

    watcher
        .watch(&settings.read().unwrap().__config_file, RecursiveMode::NonRecursive)
        .unwrap();

    let sys = System::new("websocket-client");
    let arc = Arc::clone(&settings);
    Arbiter::spawn(async {
        let mut recipients: Vec<Recipient<LiveEvent>> = vec![];
        let fa = AvroFileActor::create(move |ctx| {
            let settings_v = arc.read().unwrap();
            let data_dir = settings_v.data_dir.clone();
            let dir = Path::new(data_dir.as_str()).clone();
            fs::create_dir_all(&dir).unwrap();
            let max_file_size = Byte::from_str("50 Mb");
            if max_file_size.is_err() {
                debug!("Wrong max file size");
                std::process::exit(1);
            }
            AvroFileActor::new(&FileActorOptions {
                base_dir: dir.to_str().unwrap().to_string(),
                max_file_size: max_file_size.unwrap().get_bytes() as u64,
                max_file_time: Duration::seconds(1800),
                partitioner: handlers::liveEventPartitioner,
            })
        });
        recipients.push(fa.recipient());

        let path = PathBuf::from("keys_real.json");
//        let my_creds = BitstampCreds::new_from_file("account_bitstamp", path.clone()).unwrap();
//        let _bot = Coinnect::new_stream(Bitstamp, my_creds, recipients.clone()).await.unwrap();
        let my_creds = BittrexCreds::new_from_file("account_bittrex", path.clone()).unwrap();
        let _bot: Box<ExchangeBot> = Coinnect::new_stream(Bittrex, my_creds, recipients).await.unwrap();
    });

    sys.run().unwrap();
    Ok(())
}
