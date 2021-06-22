#![feature(try_trait_v2)]

extern crate clap;
extern crate coinnect_rt;
#[cfg(feature = "flame_it")]
extern crate flame;
#[cfg(feature = "flame_it")]
#[macro_use]
extern crate flamer;
extern crate lazy_static;
extern crate log;
extern crate trader;

use actix::{Actor, Recipient};
use coinnect_rt::metrics::PrometheusPushActor;
use coinnect_rt::types::LiveEventEnveloppe;
use futures::select;
use futures::FutureExt;
use std::io;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use trader::nats::NatsProducer;
use trader::settings::Settings;

#[actix_rt::main]
#[cfg_attr(feature = "flame_it", flame)]
async fn main() -> io::Result<()> { trader::runner::with_config(start).await }

pub async fn start(settings: Arc<RwLock<Settings>>) -> io::Result<()> {
    let settings_v = settings.read().unwrap();
    // Live Events recipients
    let mut live_events_recipients: Vec<Recipient<LiveEventEnveloppe>> = Vec::new();
    let pusher = NatsProducer::new("127.0.0.1", "ruser", "T0pS3cr3t")
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::NotConnected, e))?;
    let nats = NatsProducer::start(pusher);

    live_events_recipients.push(nats.recipient());

    // Metrics pusher
    let _prom_push = PrometheusPushActor::start(PrometheusPushActor::new(
        &settings_v.prom_push_gw,
        &settings_v.prom_instance,
    ));

    // Exchange bot actors, they just receive data
    let keys_path = PathBuf::from(settings_v.keys.clone());
    let exchanges = settings_v.exchanges.clone();
    let bots = trader::system::bots::exchange_bots(exchanges.clone(), keys_path.clone(), live_events_recipients).await;

    // Handle interrupts for graceful shutdown
    select! {
        // ping all bots at regular intervals
        _ = trader::system::bots::poll_bots(bots).fuse() => println!("Stopped polling bots."),
    };
    Ok(())
}
