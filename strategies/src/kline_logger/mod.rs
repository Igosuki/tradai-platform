use brokers::prelude::*;
use serde_json::Value;
use std::collections::HashSet;
use strategy::driver::{DefaultStrategyContext, Strategy, TradeSignals};
use strategy::error::*;
use strategy::plugin::{provide_options, StrategyPlugin};
use strategy::settings::DefaultOptions;
use strategy::Channel;

inventory::submit! {
    StrategyPlugin::new("kline_logger", provide_options::<DefaultOptions>, |name, _ctx, _conf| {
        Ok(Box::new(KlineLoggerStrategy::new(name.to_string(), Exchange::Binance, "BTC_USDT".into())))
    })
}

pub struct KlineLoggerStrategy {
    key: String,
    exchange: Exchange,
    pair: Pair,
}

impl KlineLoggerStrategy {
    #[allow(clippy::cast_sign_loss)]
    pub fn new(strat_key: String, exchange: Exchange, pair: Pair) -> Self {
        Self {
            key: strat_key,
            exchange,
            pair,
        }
    }
}

#[async_trait]
impl Strategy for KlineLoggerStrategy {
    fn key(&self) -> String { self.key.clone() }

    fn init(&mut self) -> Result<()> { Ok(()) }

    async fn eval(&mut self, _le: &MarketEventEnvelope, _ctx: &DefaultStrategyContext) -> Result<Option<TradeSignals>> {
        Ok(None)
    }

    fn model(&self) -> Vec<(String, Option<Value>)> { vec![] }

    fn channels(&self) -> HashSet<Channel> {
        let channels = vec![Channel::Candles {
            xch: self.exchange,
            pair: self.pair.clone(),
        }];
        let mut hs = HashSet::new();
        hs.extend(channels);
        hs
    }
}
