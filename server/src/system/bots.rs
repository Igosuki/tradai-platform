use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actix::{Actor, Handler, Recipient};

use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::exchange::{Exchange, ExchangeSettings};
use coinnect_rt::exchange_bot::{ExchangeBot, Ping};
use coinnect_rt::types::LiveEventEnveloppe;

use crate::system::BotAndActorHandles;
use actix::dev::ToEnvelope;
use strategies::order_manager::OrderManager;

pub async fn exchange_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
    recipients: Vec<Recipient<LiveEventEnveloppe>>,
) -> anyhow::Result<HashMap<Exchange, Box<dyn ExchangeBot>>> {
    let mut bots: HashMap<Exchange, Box<dyn ExchangeBot>> = HashMap::new();
    for (xch, conf) in exchanges_settings.clone().iter() {
        let creds = Coinnect::credentials_for(*xch, keys_path.clone())?;
        let bot = Coinnect::new_stream(*xch, creds, conf.clone(), recipients.clone()).await?;
        bots.insert(*xch, bot);
    }
    Ok(bots)
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

pub async fn poll_account_bots(
    systems: Arc<HashMap<Exchange, BotAndActorHandles<OrderManager>>>,
) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        for system in systems.values() {
            system.ping();
        }
    }
}

pub async fn poll_account_bot<T>(system: BotAndActorHandles<T>) -> std::io::Result<()>
where
    T: Actor + Handler<Ping>,
    <T as Actor>::Context: ToEnvelope<T, Ping>,
{
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        system.ping();
    }
}
