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

pub mod avro_gen;
pub mod handlers;
pub mod api;

fn main() -> io::Result<()> {
    env_logger::init();

    let sys = System::new("websocket-client");

    Arbiter::spawn(async {
        let mut recipients: Vec<Recipient<LiveEvent>> = vec![];

        let fa = AvroFileActor::create(|ctx| {
            let dir = Path::new("./data").clone();
            fs::remove_dir_all(dir).unwrap();
            fs::create_dir_all(&dir).unwrap();
            let max_file_size = Byte::from_str("50.00 MB").unwrap();
            AvroFileActor::new(&FileActorOptions {
                base_dir: dir.to_str().unwrap().to_string(),
                max_file_size: max_file_size.get_bytes() as u64,
                max_file_time: Duration::seconds(5),
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
