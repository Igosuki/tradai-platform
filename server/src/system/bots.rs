use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actix::Recipient;

use coinnect_rt::binance::BinanceCreds;
use coinnect_rt::bitstamp::BitstampCreds;
use coinnect_rt::bittrex::BittrexCreds;
use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::exchange::{Exchange, ExchangeSettings};
use coinnect_rt::exchange_bot::ExchangeBot;
use coinnect_rt::types::LiveEventEnveloppe;

use crate::system::AccountSystem;

pub async fn exchange_bots(
    exchanges_settings: HashMap<Exchange, ExchangeSettings>,
    keys_path: PathBuf,
    recipients: Vec<Recipient<LiveEventEnveloppe>>,
) -> HashMap<Exchange, Box<dyn ExchangeBot>> {
    let mut bots: HashMap<Exchange, Box<dyn ExchangeBot>> = HashMap::new();
    // TODO : solve the annoying problem of credentials being a specific struct when new_stream and new are generic
    for (xch, conf) in exchanges_settings {
        let bot = match xch {
            Exchange::Bittrex => {
                let creds = Box::new(BittrexCreds::new_from_file("account_bittrex", keys_path.clone()).unwrap());
                Coinnect::new_stream(xch, creds.clone(), conf, recipients.clone())
                    .await
                    .unwrap()
            }
            Exchange::Bitstamp => {
                let creds = Box::new(BitstampCreds::new_from_file("account_bitstamp", keys_path.clone()).unwrap());
                Coinnect::new_stream(xch, creds.clone(), conf, recipients.clone())
                    .await
                    .unwrap()
            }
            Exchange::Binance => {
                let creds = Box::new(
                    BinanceCreds::new_from_file(coinnect_rt::binance::credentials::ACCOUNT_KEY, keys_path.clone())
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

pub async fn poll_bots(bots: HashMap<Exchange, Box<dyn ExchangeBot>>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        for bot in bots.values() {
            bot.ping();
        }
    }
}

pub async fn poll_account_bots(systems: Arc<HashMap<Exchange, AccountSystem>>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        for system in systems.values() {
            system.ping();
        }
    }
}
