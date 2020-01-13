#![feature(try_trait)]
// This example shows how to use the generic API provided by Coinnect.
// This method is useful if you have to iterate throught multiple accounts of
// different exchanges and perform the same operation (such as get the current account's balance)
// You can also use the Coinnect generic API if you want a better error handling since all methods
// return Result<_, Error>.

extern crate actix;
extern crate coinnect_rt;
#[macro_use]
extern crate actix_derive;
#[macro_use]
extern crate lazy_static;
extern crate uuid;
#[macro_use] extern crate log;
#[macro_use] extern crate byte_unit;
extern crate config;

use crate::coinnect_rt::bitstamp::BitstampCreds;
use crate::coinnect_rt::coinnect::Coinnect;
use crate::coinnect_rt::exchange::Exchange::*;
use crate::coinnect_rt::types::Pair::*;
use actix::{io::SinkWrite, Actor, ActorContext, AsyncContext, Context, Handler, StreamHandler, Recipient, Addr};
use actix_codec::{AsyncRead, AsyncWrite, Framed};
use actix_rt::{Arbiter, System};
use avro_rs::{Schema, Writer};
use awc::{
    error::WsProtocolError,
    ws::{Codec, Frame, Message},
    BoxedSocket, Client,
};
use bytes::Buf;
use bytes::Bytes;
use coinnect_rt::helpers;
use futures::stream::{SplitSink, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::{PathBuf, Path};
use std::{fs, io, thread};
use crate::handlers::file_actor::{AvroFileActor, FileActorOptions};
use coinnect_rt::types::LiveEvent;
use coinnect_rt::exchange_bot::{DefaultWsActor, ExchangeBot};
use coinnect_rt::bittrex::BittrexCreds;
use byte_unit::Byte;
use chrono::{Duration};
use notify::{RecommendedWatcher, DebouncedEvent, Watcher, RecursiveMode};
use std::sync::mpsc::channel;
use std::sync::RwLock;
use std::env::var;
pub mod avro_gen;
pub mod handlers;
pub mod api;
use config::*;

lazy_static! {
    static ref CONFIG_FILE: String = {
        let trader_env : String = std::env::var("TRADER_ENV").unwrap_or("development".to_string());
        format!("config/{}.yaml", trader_env)
    };
}

lazy_static! {
    static ref SETTINGS: RwLock<Config> = RwLock::new({
        let mut settings = Config::default();
        settings.merge(File::with_name(&CONFIG_FILE)).unwrap();

        settings
    });
}

fn show() {
    println!(" * Settings :: \n\x1b[31m{:?}\x1b[0m",
             SETTINGS
                 .read()
                 .unwrap()
                 .clone()
                 .try_into::<HashMap<String, String>>()
                 .unwrap());
}

fn main() -> io::Result<()> {
    env_logger::init();

    // Create a channel to receive the events.
    let (tx, rx) = channel();
    let mut watcher: RecommendedWatcher = Watcher::new(tx, std::time::Duration::from_secs(2)).unwrap();

    watcher
        .watch(&*CONFIG_FILE, RecursiveMode::NonRecursive)
        .unwrap();

    let sys = System::new("websocket-client");

    Arbiter::spawn(async {
        let mut recipients: Vec<Recipient<LiveEvent>> = vec![];

        let fa = AvroFileActor::create(|ctx| {
            let dir = Path::new("./data").clone();
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
        let _bot : Box<ExchangeBot> = Coinnect::new_stream(Bittrex, my_creds, recipients).await.unwrap();
    });
    sys.run().unwrap();
    Ok(())
}
