mod metrics;

use crate::bbplusb::metrics::BBPlusMetrics;
use chrono::Duration;
use coinnect_rt::exchange::Exchange;
use coinnect_rt::prelude::MarketEventEnvelope;
use coinnect_rt::types::Pair;
use serde_json::Value;
use stats::{BollingerBands, BollingerBandsOutput, Next};
use std::collections::HashSet;
use std::sync::Arc;
use strategy::driver::{DefaultStrategyContext, Strategy, TradeSignals};
use strategy::error::Result;
use strategy::models::io::SerializedModel;
use strategy::plugin::{provide_options, StrategyPlugin, StrategyPluginContext};
use strategy::settings::{StrategyOptions, StrategySettingsReplicator};
use strategy::{Channel, StrategyKey};
use trading::position::{OperationKind, PositionKind};
use trading::signal::new_trade_signal;
use trading::types::OrderConf;

pub fn provide_strat(name: &str, _ctx: StrategyPluginContext, conf: serde_json::Value) -> Result<Box<dyn Strategy>> {
    let options: BollingerPlusStrategyOptions = serde_json::from_value(conf)?;
    Ok(Box::new(BollingerPlusStrategy::new(name.to_string(), &options)))
}

inventory::submit! {
    StrategyPlugin::new("bbplusb", provide_options::<BollingerPlusStrategyOptions>, provide_strat)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BollingerPlusStrategyOptions {
    length: Option<u32>,
    mult: Option<f64>,
    ob: Option<f64>,
    ob_close: Option<f64>,
    os: Option<f64>,
    os_close: Option<f64>,
    #[serde(
        deserialize_with = "util::ser::decode_duration_str",
        serialize_with = "util::ser::encode_duration_str"
    )]
    ticker_time_frame: Duration,
    pair: String,
    exchange: Exchange,
}

impl BollingerPlusStrategyOptions {
    fn length(&self) -> u32 { self.length.unwrap_or(21) }

    // TODO: 0.001 < mult < 50 should be enforced
    fn mult(&self) -> f64 { self.mult.unwrap_or(2.0) }

    // TODO: precision is 0.1 and should be enforced
    fn ob(&self) -> f64 { self.ob.unwrap_or(1.2) }
    fn ob_close(&self) -> f64 { self.ob_close.unwrap_or(1.0) }
    fn os(&self) -> f64 { self.os.unwrap_or(-0.2) }
    fn os_close(&self) -> f64 { self.os_close.unwrap_or(0.2) }
}

impl StrategySettingsReplicator for BollingerPlusStrategyOptions {
    fn replicate_for_pairs(&self, pairs: HashSet<Pair>) -> Vec<Value> {
        pairs
            .into_iter()
            .map(|pair| {
                let mut new = self.clone();
                new.pair = pair.to_string();
                serde_json::to_value(new).unwrap()
            })
            .collect()
    }
}

impl StrategyOptions for BollingerPlusStrategyOptions {
    fn key(&self) -> StrategyKey { StrategyKey("breakout".to_string(), self.pair.to_string()) }
}

pub struct BollingerPlusStrategy {
    name: String,
    pair: Pair,
    exchange: Exchange,
    order_conf: OrderConf,
    bb: BollingerBands,
    #[allow(dead_code)]
    ticker_time_frame: Duration,
    //highs: Window<f64>,
    ob: f64,
    ob_close: f64,
    os: f64,
    os_close: f64,
    last_bands_ppo: Option<f64>,
    last_bands: Option<BollingerBandsOutput>,
    metrics: Arc<BBPlusMetrics>,
}

impl BollingerPlusStrategy {
    pub fn new(name: String, options: &BollingerPlusStrategyOptions) -> Self {
        let metrics = BBPlusMetrics::for_strat(prometheus::default_registry(), &options.pair);
        Self {
            name,
            pair: options.pair.clone().into(),
            exchange: options.exchange,
            order_conf: Default::default(),
            bb: BollingerBands::new(options.length() as usize, options.mult()).unwrap(),
            ticker_time_frame: options.ticker_time_frame,
            ob: options.ob(),
            ob_close: options.ob_close(),
            os: options.os(),
            os_close: options.os_close(),
            last_bands_ppo: None,
            last_bands: None,
            metrics: Arc::new(metrics),
        }
    }
}

#[async_trait]
impl Strategy for BollingerPlusStrategy {
    fn key(&self) -> String { self.name.clone() }

    fn init(&mut self) -> strategy::error::Result<()> { todo!() }

    async fn eval(
        &mut self,
        e: &MarketEventEnvelope,
        ctx: &DefaultStrategyContext,
    ) -> strategy::error::Result<Option<TradeSignals>> {
        let market_event = &e.e;
        let vwap = market_event.vwap();
        let bands = self.bb.next(vwap);
        let bands_ppo = (vwap - bands.lower) / (bands.upper - bands.lower);
        self.metrics
            .log_constants(self.ob, self.ob_close, self.os, self.os_close);
        self.metrics.log_model(&bands, bands_ppo);
        let price = market_event.close();
        self.metrics.log_price(price);
        self.last_bands = Some(bands);
        self.last_bands_ppo = Some(bands_ppo);
        let new_signal = |op, pos, price, qty| {
            Some(new_trade_signal(
                self.pair.clone(),
                self.exchange,
                &self.order_conf,
                e.ts,
                e.trace_id,
                op,
                pos,
                price,
                qty,
            ))
        };
        let qty = Some(ctx.portfolio.value() / price);
        let signal = match ctx.portfolio.open_position(self.exchange, self.pair.clone()) {
            Some(pos) if pos.is_long() && bands_ppo >= self.os_close => {
                new_signal(OperationKind::Close, PositionKind::Long, price, None)
            }
            Some(pos) if pos.is_short() && bands_ppo <= self.ob_close => {
                new_signal(OperationKind::Close, PositionKind::Short, price, None)
            }

            None if bands_ppo < self.os => new_signal(OperationKind::Open, PositionKind::Long, price, qty),
            None if bands_ppo > self.ob => new_signal(OperationKind::Open, PositionKind::Short, price, qty),
            _ => None,
        };
        let signals = TradeSignals::from_iter(signal);
        Ok(Some(signals))
    }

    fn model(&self) -> SerializedModel {
        vec![
            (
                "bbppo".to_string(),
                self.last_bands_ppo.and_then(|v| serde_json::to_value(v).ok()),
            ),
            (
                "sma".to_string(),
                self.last_bands
                    .as_ref()
                    .and_then(|v| serde_json::to_value(v.average).ok()),
            ),
            (
                "bblower".to_string(),
                self.last_bands
                    .as_ref()
                    .and_then(|v| serde_json::to_value(v.lower).ok()),
            ),
            (
                "bbupper".to_string(),
                self.last_bands
                    .as_ref()
                    .and_then(|v| serde_json::to_value(v.upper).ok()),
            ),
        ]
    }

    fn constants(&self) -> SerializedModel {
        vec![
            ("ob".to_string(), serde_json::to_value(self.ob).ok()),
            ("ob_close".to_string(), serde_json::to_value(self.ob_close).ok()),
            ("os".to_string(), serde_json::to_value(self.os).ok()),
            ("os_close".to_string(), serde_json::to_value(self.os_close).ok()),
        ]
    }

    fn channels(&self) -> HashSet<Channel> {
        let channels = vec![Channel::Orderbooks {
            xch: self.exchange,
            pair: self.pair.clone(),
        }];
        let mut hs = HashSet::new();
        hs.extend(channels);
        hs
    }
}
