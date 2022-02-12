use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use actix::Recipient;
use tracing::Instrument;

use brokers::bot::{AccountExchangeBot, ExchangeBot, MarketExchangeBot, Ping};
use brokers::pair::pair_to_symbol;
use brokers::prelude::*;
use brokers::types::PrivateStreamChannel;

pub async fn exchange_bots<'a>(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
) -> anyhow::Result<HashMap<Exchange, Box<MarketExchangeBot>>> {
    let mut bots: HashMap<Exchange, Box<MarketExchangeBot>> = HashMap::new();
    for (xch, conf) in exchanges_settings.clone().iter() {
        let creds = Brokerages::credentials_for(*xch, keys_path.clone())?;
        let bot = Brokerages::new_market_bot(*xch, creds, conf.clone())
            .instrument(tracing::info_span!("new exchange stream", xchg = ?xch))
            .await?;
        bots.insert(*xch, bot);
    }
    Ok(bots)
}

pub async fn spot_account_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
) -> anyhow::Result<Vec<Box<AccountExchangeBot>>> {
    make_account_bots(exchanges_settings, keys_path, AccountType::Spot, |(_, conf)| {
        conf.use_account
    })
    .await
}

pub async fn margin_account_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
) -> anyhow::Result<Vec<Box<AccountExchangeBot>>> {
    make_account_bots(exchanges_settings, keys_path, AccountType::Margin, |(_, conf)| {
        conf.use_margin_account
    })
    .await
}

pub async fn isolated_margin_account_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
) -> anyhow::Result<Vec<Box<AccountExchangeBot>>> {
    let mut bots = vec![];
    for (xch, conf) in exchanges_settings
        .iter()
        .filter(|(_, conf)| conf.use_isolated_margin_account)
    {
        for pair in &conf.isolated_margin_account_pairs {
            let symbol = pair_to_symbol(xch, &Pair::from(pair.as_str()))?;
            let creds = Brokerages::credentials_for(*xch, keys_path.clone())?;
            let bot = Brokerages::new_account_stream(
                *xch,
                creds.clone(),
                conf.use_test,
                AccountType::IsolatedMargin(symbol.to_string()),
                PrivateStreamChannel::all(),
            )
            .await?;
            bots.push(bot);
        }
    }
    Ok(bots)
}

pub async fn make_account_bots(
    exchanges_settings: Arc<HashMap<Exchange, ExchangeSettings>>,
    keys_path: PathBuf,
    account_type: AccountType,
    pred: fn(&(&Exchange, &ExchangeSettings)) -> bool,
) -> anyhow::Result<Vec<Box<AccountExchangeBot>>> {
    let mut bots = vec![];

    for (xch, conf) in exchanges_settings.iter().filter(pred) {
        let creds = Brokerages::credentials_for(*xch, keys_path.clone())?;
        let bot = Brokerages::new_account_stream(
            *xch,
            creds.clone(),
            conf.use_test,
            account_type.clone(),
            PrivateStreamChannel::all(),
        )
        .await?;
        bots.push(bot);
    }
    Ok(bots)
}

pub async fn poll_bots<E>(bots: HashMap<Exchange, Box<dyn ExchangeBot<E>>>) -> std::io::Result<()> {
    let mut interval = tokio::time::interval(Duration::from_secs(10));
    loop {
        interval.tick().await;
        for bot in bots.values() {
            bot.ping();
        }
    }
}

pub async fn poll_bots_many<E>(bots: Vec<Box<dyn ExchangeBot<E>>>) -> std::io::Result<()> {
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
        for recipient in &recipients {
            recipient
                .do_send(brokers::bot::Ping)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        }
    }
}
