#![feature(try_trait)]
// This example shows how to use the generic API provided by Coinnect.
// This method is useful if you have to iterate throught multiple accounts of
// different exchanges and perform the same operation (such as get the current account's balance)
// You can also use the Coinnect generic API if you want a better error handling since all methods
// return Result<_, Error>.

extern crate actix;
extern crate coinnect;
#[macro_use]
extern crate actix_derive;
#[macro_use]
extern crate lazy_static;
extern crate uuid;

use crate::coinnect::bitstamp::BitstampCreds;
use crate::coinnect::coinnect::Coinnect;
use crate::coinnect::exchange::Exchange::*;
use crate::coinnect::types::Pair::*;
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
use coinnect::helpers;
use futures::stream::{SplitSink, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use std::collections::HashMap;
use std::path::{PathBuf, Path};
use std::time::Duration;
use std::{fs, io};
use crate::handlers::file_actor::{AvroFileActor, FileActorOptions};
use coinnect::types::LiveEvent;
use coinnect::exchange_bot::ExchangeBot;

pub mod avro_gen;
pub mod handlers;
pub mod api;

fn main() -> io::Result<()> {
    let sys = System::new("websocket-client");

    Arbiter::spawn(async {
        let mut recipients: Vec<Recipient<LiveEvent>> = vec![];

        let fa = AvroFileActor::create(|ctx| {
            let dir = Path::new("./data/trades").clone();
            fs::remove_dir_all(dir).unwrap();
            fs::create_dir_all(&dir).unwrap();
            AvroFileActor::new(&FileActorOptions {
                base_dir: dir.to_str().unwrap().to_string(),
            })
        });
        recipients.push(fa.recipient());

        let path = PathBuf::from("keys_real.json");
        let my_creds = BitstampCreds::new_from_file("account_bitstamp", path).unwrap();
        let _stream : Addr<ExchangeBot> = Coinnect::new_stream(Bitstamp, my_creds, recipients).await.unwrap();
    });
    sys.run().unwrap();
    Ok(())
}
