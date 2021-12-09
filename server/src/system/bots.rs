use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actix::Recipient;
use tracing::Instrument;

use coinnect_rt::bot::{ExchangeBot, Ping};
use coinnect_rt::broker::{ActixMessageBroker, MarketEventEnvelopeMsg};
use coinnect_rt::pair::pair_to_symbol;
use coinnect_rt::prelude::*;
use strategy::Channel;

pub async fn exchange_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
    broker: Arc<ActixMessageBroker<Channel, MarketEventEnvelopeMsg>>,
) -> anyhow::Result<HashMap<Exchange, Box<dyn ExchangeBot>>> {
    let mut bots: HashMap<Exchange, Box<dyn ExchangeBot>> = HashMap::new();
    for (xch, conf) in exchanges_settings.clone().iter() {
        let creds = Coinnect::credentials_for(*xch, keys_path.clone())?;
        let bot = Coinnect::new_stream(*xch, creds, conf.clone(), broker.clone())
            .instrument(tracing::info_span!("new exchange stream", xchg = ?xch))
            .await?;
        bots.insert(*xch, bot);
    }
    Ok(bots)
}

pub async fn spot_account_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
    recipients: HashMap<Exchange, Vec<Recipient<AccountEventEnveloppe>>>,
) -> anyhow::Result<Vec<Box<dyn ExchangeBot>>> {
    make_bots(
        exchanges_settings,
        keys_path,
        AccountType::Spot,
        recipients,
        |(_, conf)| conf.use_account,
    )
    .await
}

pub async fn margin_account_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
    recipients: HashMap<Exchange, Vec<Recipient<AccountEventEnveloppe>>>,
) -> anyhow::Result<Vec<Box<dyn ExchangeBot>>> {
    make_bots(
        exchanges_settings,
        keys_path,
        AccountType::Margin,
        recipients,
        |(_, conf)| conf.use_margin_account,
    )
    .await
}

pub async fn isolated_margin_account_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
    recipients: HashMap<Exchange, Vec<Recipient<AccountEventEnveloppe>>>,
) -> anyhow::Result<Vec<Box<dyn ExchangeBot>>> {
    let mut bots = vec![];
    for (xch, conf) in exchanges_settings
        .iter()
        .filter(|(_, conf)| conf.use_isolated_margin_account)
    {
        for pair in &conf.isolated_margin_account_pairs {
            let symbol = pair_to_symbol(xch, &Pair::from(pair.as_str()))?;
            let creds = Coinnect::credentials_for(*xch, keys_path.clone())?;
            let bot = Coinnect::new_account_stream(
                *xch,
                creds.clone(),
                recipients.get(xch).cloned().unwrap_or_default(),
                conf.use_test,
                AccountType::IsolatedMargin(symbol.to_string()),
            )
            .await?;
            bots.push(bot);
        }
    }
    Ok(bots)
}

pub async fn make_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
    account_type: AccountType,
    recipients: HashMap<Exchange, Vec<Recipient<AccountEventEnveloppe>>>,
    pred: fn(&(&Exchange, &ExchangeSettings)) -> bool,
) -> anyhow::Result<Vec<Box<dyn ExchangeBot>>> {
    let mut bots = vec![];
    for (xch, conf) in exchanges_settings.iter().filter(pred) {
        let creds = Coinnect::credentials_for(*xch, keys_path.clone())?;
        let bot = Coinnect::new_account_stream(
            *xch,
            creds.clone(),
            recipients.get(xch).cloned().unwrap_or_default(),
            conf.use_test,
            account_type.clone(),
        )
        .await?;
        bots.push(bot);
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

pub async fn poll_bots_vec(bots: Vec<Box<dyn ExchangeBot>>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        for bot in &bots {
            bot.ping();
        }
    }
}

pub async fn poll_pingables(recipients: Vec<Recipient<Ping>>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        for recipient in recipients.iter() {
            recipient
                .do_send(coinnect_rt::bot::Ping)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }
    }
}
