use brokers::exchange::Exchange;
use brokers::prelude::MarketEventEnvelope;
use brokers::types::Pair;
use chrono::{DateTime, Duration, TimeZone, Utc};
use serde_json::Value;
use stats::indicators::cross::{CrossAbove, CrossUnder};
use stats::indicators::ema::{ExponentialMovingAverage, MovingAverageType};
use stats::ta_indicators::SimpleMovingAverage;
use stats::yata_methods::{LowerReversalSignal, UpperReversalSignal};
use stats::{Action, Method, Next, Window};
use std::collections::HashSet;
use strategy::driver::{DefaultStrategyContext, Strategy, TradeSignals};
use strategy::error::Result;
use strategy::models::io::SerializedModel;
use strategy::plugin::{provide_options, StrategyPlugin, StrategyPluginContext};
use strategy::settings::{StrategyOptions, StrategySettingsReplicator};
use strategy::{MarketChannel, StrategyKey};
use trading::position::{OperationKind, PositionKind};
use trading::signal::new_trade_signal;
use trading::types::OrderConf;
use util::time::now;

pub fn provide_strat(name: &str, _ctx: StrategyPluginContext, conf: serde_json::Value) -> Result<Box<dyn Strategy>> {
    let options: BreakoutStrategyOptions = serde_json::from_value(conf)?;
    Ok(Box::new(BreakoutStrategy::new(name.to_string(), &options)))
}

inventory::submit! {
    StrategyPlugin::new("breakout", provide_options::<BreakoutStrategyOptions>, provide_strat)
}

const MA1_DEFAULT: u32 = 10;
const MA2_DEFAULT: u32 = 20;
const MA3_DEFAULT: u32 = 50;
const LOOKBACK_DEFAULT: u32 = 3;
const MA_FILTER_DEFAULT: bool = true;
const ADR_FILTER_DEFAULT: bool = true;
const TRAIL_MA_DEFAULT: u8 = 1;
const ADR_PERC_DEFAULT: f64 = 120.0;
const ADR_LEN_DEFAULT: usize = 21;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct BreakoutStrategyOptions {
    ma_type: MovingAverageType,
    ma_1_len: Option<u32>,
    ma_2_len: Option<u32>,
    ma_3_len: Option<u32>,
    adr_len: Option<usize>,
    lb_low: Option<u32>,
    lb_high: Option<u32>,
    use_ma_filter: Option<bool>,
    use_adr_filter: Option<bool>,
    trail_ma_input: Option<u8>,
    adr_perc: Option<f64>,
    #[serde(
        deserialize_with = "util::ser::string_duration_chrono",
        serialize_with = "util::ser::encode_duration_str"
    )]
    ticker_time_frame: Duration,
    pair: String,
    exchange: Exchange,
}

impl BreakoutStrategyOptions {
    fn ma_1_len(&self) -> u32 { self.ma_1_len.unwrap_or(MA1_DEFAULT) }
    fn ma_2_len(&self) -> u32 { self.ma_2_len.unwrap_or(MA2_DEFAULT) }
    fn ma_3_len(&self) -> u32 { self.ma_3_len.unwrap_or(MA3_DEFAULT) }
    fn adr_len(&self) -> usize { self.adr_len.unwrap_or(ADR_LEN_DEFAULT) }
    fn lb_low(&self) -> u32 { self.lb_low.unwrap_or(LOOKBACK_DEFAULT) }
    fn lb_high(&self) -> u32 { self.lb_high.unwrap_or(LOOKBACK_DEFAULT) }
    fn use_ma_filter(&self) -> bool { self.use_ma_filter.unwrap_or(MA_FILTER_DEFAULT) }
    fn use_adr_filter(&self) -> bool { self.use_adr_filter.unwrap_or(ADR_FILTER_DEFAULT) }
    fn trail_ma_input(&self) -> u8 { self.trail_ma_input.unwrap_or(TRAIL_MA_DEFAULT) }
    fn adr_perc(&self) -> f64 { self.adr_perc.unwrap_or(ADR_PERC_DEFAULT) }
}

impl StrategySettingsReplicator for BreakoutStrategyOptions {
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

impl StrategyOptions for BreakoutStrategyOptions {
    fn key(&self) -> StrategyKey { StrategyKey("breakout".to_string(), self.pair.to_string()) }
}

pub struct BreakoutStrategy {
    name: String,
    pair: Pair,
    exchange: Exchange,
    order_conf: OrderConf,
    ma_1: ExponentialMovingAverage,
    ma_2: ExponentialMovingAverage,
    ma_3: ExponentialMovingAverage,
    /// Average Daily Range
    adr: SimpleMovingAverage,
    /// Pivot Low
    ph: UpperReversalSignal,
    /// Pivot High
    pl: LowerReversalSignal,
    ma_cross: CrossUnder,
    #[allow(dead_code)]
    buy_cross: CrossAbove,
    use_ma_filter: bool,
    use_adr_filter: bool,
    trail_ma_input: u8,
    #[allow(dead_code)]
    ticker_time_frame: Duration,
    adr_perc: f64,
    lows: Window<f64>,
    highs: Window<f64>,
    ma_crossed_at: DateTime<Utc>,
    // These can be better defined as 'windows'
    last_stop_level: f64,
    last_close: f64,
    last_stop_define: f64,
    last_buy_level: f64,
    high_level: f64,
    low_level: f64,
}

impl BreakoutStrategy {
    #[allow(clippy::useless_conversion)]
    pub fn new(name: String, options: &BreakoutStrategyOptions) -> Self {
        Self {
            name,
            pair: options.pair.clone().into(),
            exchange: options.exchange,
            order_conf: Default::default(),
            ma_1: ExponentialMovingAverage::new(2.0, options.ma_1_len()).unwrap(),
            ma_2: ExponentialMovingAverage::new(2.0, options.ma_2_len()).unwrap(),
            ma_3: ExponentialMovingAverage::new(2.0, options.ma_3_len()).unwrap(),
            adr: SimpleMovingAverage::new(options.adr_len()).unwrap(),
            ph: UpperReversalSignal::new(options.lb_high().into(), options.lb_high().into(), &0.0).unwrap(),
            pl: LowerReversalSignal::new(options.lb_low().into(), options.lb_low().into(), &0.0).unwrap(),
            ma_cross: CrossUnder::new(&(0.0, 0.0)).unwrap(),
            buy_cross: CrossAbove::new(&(0.0, 0.0)).unwrap(),
            use_ma_filter: options.use_ma_filter(),
            use_adr_filter: options.use_adr_filter(),
            trail_ma_input: options.trail_ma_input(),
            ticker_time_frame: options.ticker_time_frame,
            adr_perc: options.adr_perc(),
            ma_crossed_at: Utc.timestamp_millis_opt(0).unwrap(),
            last_stop_level: f64::NAN,
            last_close: f64::NAN,
            last_stop_define: f64::NAN,
            last_buy_level: f64::NAN,
            lows: Window::new(options.lb_low().into(), f64::NAN),
            highs: Window::new(options.lb_high().into(), f64::NAN),
            high_level: f64::NAN,
            low_level: f64::NAN,
        }
    }
}

#[async_trait]
impl Strategy for BreakoutStrategy {
    fn key(&self) -> String { self.name.clone() }

    fn init(&mut self) -> strategy::error::Result<()> { todo!() }

    async fn eval(
        &mut self,
        e: &MarketEventEnvelope,
        ctx: &DefaultStrategyContext,
    ) -> strategy::error::Result<Option<TradeSignals>> {
        let mut signals = TradeSignals::new();
        let market_event = &e.e;
        let vwap = market_event.vwap();
        self.last_close = market_event.close();
        let oldest_low = self.lows.push(market_event.low());
        let oldest_high = self.highs.push(market_event.high());
        // TODO: Make a candler aggregator from trades with GapsOff for gaps, LookAheadOn for lookahead and ticker_time_frame for time frame

        // --- Model
        let ma_1 = self.ma_1.next(vwap);
        let ma_2 = self.ma_2.next(vwap);
        let ma_3 = self.ma_3.next(vwap);
        let trail_ma = match self.trail_ma_input {
            1 => ma_1,
            2 => ma_2,
            _ => unreachable!(),
        };
        let ma_crossed = self.ma_cross.next((market_event.low(), trail_ma));
        let ma_cross_level = if ma_crossed {
            self.ma_crossed_at = now();
            market_event.low()
        } else {
            0.0
        };
        //let ma_cross_pc = Change::new(maCrossLevel) != 0 ? na : color.new(color.blue, 0); //Removes color when there is a change to ensure only the levels are shown (i.e. no diagonal lines connecting the levels)
        let ph = self.ph.next(&market_event.high());
        self.high_level = if let Action::Buy(_) = ph { oldest_high } else { 0.0 };
        let pl = self.pl.next(&market_event.low());
        self.low_level = if let Action::Sell(_) = ph { oldest_low } else { 0.0 };

        // --- Decisions
        let is_long = ctx
            .portfolio
            .open_position(self.exchange, self.pair.clone())
            .map_or(false, |p| p.is_long());

        // long entry
        let buy_level = if let Action::Buy(_) = ph { oldest_high } else { 0.0 };
        let adr_value = self
            .adr
            .next((market_event.high() - market_event.low()) / market_event.low().abs() * 100.0);
        let adr_compare = (self.adr_perc * adr_value) / 100.0;
        let buy_conditions = if self.use_ma_filter { buy_level > ma_3 } else { true }
            && if self.use_adr_filter {
                (buy_level - self.last_stop_level) < adr_compare
            } else {
                true
            };
        if !is_long && buy_conditions {
            //self.entry_stop = PositionStopper::new(e.e.close(), PositionKind::Long);
            signals.push(new_trade_signal(
                self.pair.clone(),
                self.exchange,
                &self.order_conf,
                market_event.time(),
                e.trace_id,
                OperationKind::Open,
                PositionKind::Long,
                market_event.close(),
                None,
            ));
        }

        let entry_price = ctx
            .portfolio
            .open_position(self.exchange, self.pair.clone())
            .map_or(f64::NAN, |pos| pos.open_order.as_ref().unwrap().price.unwrap());

        // long stop loss
        //let buy_signal = self.buy_cross.next((e.e.high(), buy_level)) && buy_conditions;
        let buy_level = if is_long { self.last_buy_level } else { buy_level };
        if is_long && entry_price < buy_level {
            signals.push(new_trade_signal(
                self.pair.clone(),
                self.exchange,
                &self.order_conf,
                market_event.time(),
                e.trace_id,
                OperationKind::Close,
                PositionKind::Long,
                market_event.close(),
                None,
            ));
        }

        // possibly close the buy position
        let stop_define = if let Action::Sell(_) = pl { oldest_low } else { 0.0 }; //Stop Level at Swing Low
        let in_profit = ctx.portfolio.position_avg_price() <= self.last_stop_define;
        // stopLevel := inPosition ? stopLevel[1] : stopDefine // Set stop loss based on swing low and leave it there
        let stop_level = if is_long && !in_profit {
            stop_define
        } else if is_long && in_profit {
            self.last_stop_level
        } else {
            stop_define
        }; // Trail stop loss until in profit
        let last_pos = ctx.portfolio.last_position();
        let trail_cross = last_pos
            .map_or(Utc.timestamp_millis_opt(0), |pos| pos.meta.open_at)
            .unwrap()
            < self.ma_crossed_at;
        if is_long {
            let trail_stop_level = if trail_cross { ma_cross_level } else { f64::NAN };
            let is_buy_stop = if stop_level > trail_stop_level {
                stop_level
            } else {
                self.last_close
            } > trail_stop_level;
            let is_trail_ma_stop = self.last_close > trail_ma;
            let stop_gain_level = if is_buy_stop && is_trail_ma_stop {
                trail_stop_level
            } else {
                stop_level
            };

            if market_event.close() > stop_gain_level {
                signals.push(new_trade_signal(
                    self.pair.clone(),
                    self.exchange,
                    &self.order_conf,
                    market_event.time(),
                    e.trace_id,
                    OperationKind::Close,
                    PositionKind::Long,
                    market_event.close(),
                    None,
                ));
            }
        }
        // exit long when stop = stopLevel > trailStopLevel ? stopLevel : close[1] > trailStopLevel and close[1] > trailMa ? trailStopLevel : stopLevel
        Ok(Some(signals))
    }

    fn model(&self) -> SerializedModel { todo!() }

    fn channels(&self) -> HashSet<MarketChannel> { todo!() }
}
