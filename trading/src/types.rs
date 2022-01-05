// --------- Event Types ---------

use uuid::Uuid;

use coinnect_rt::types::{AddOrderRequest, AssetType, MarginSideEffect, MarketEvent, OrderEnforcement, OrderType,
                         TradeType};

use crate::signal::ExecutionInstruction;

#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize, EnumString, AsRefStr, juniper::GraphQLEnum)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TradeKind {
    #[strum(serialize = "buy")]
    Buy,
    #[strum(serialize = "sell")]
    Sell,
}

impl From<TradeType> for TradeKind {
    fn from(tk: TradeType) -> TradeKind {
        match tk {
            TradeType::Buy => TradeKind::Buy,
            TradeType::Sell => TradeKind::Sell,
        }
    }
}

impl From<TradeKind> for TradeType {
    fn from(tk: TradeKind) -> TradeType {
        match tk {
            TradeKind::Buy => TradeType::Buy,
            TradeKind::Sell => TradeType::Sell,
        }
    }
}

impl From<TradeKind> for i32 {
    fn from(tk: TradeKind) -> i32 {
        match tk {
            TradeKind::Buy => 0,
            TradeKind::Sell => 1,
        }
    }
}

#[derive(Copy, Clone, PartialEq, Eq, Debug, Deserialize, Serialize, juniper::GraphQLEnum)]
#[serde(rename_all = "snake_case")]
pub enum OrderMode {
    Market,
    Limit,
}

impl Default for OrderMode {
    fn default() -> Self { Self::Limit }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct TradeOperation {
    pub id: String,
    pub kind: TradeKind,
    pub pair: String,
    pub qty: f64,
    pub price: f64,
    pub dry_mode: bool,
    #[serde(default)]
    pub mode: OrderMode,
    #[serde(default)]
    pub asset_type: AssetType,
    pub side_effect: Option<MarginSideEffect>,
}

#[juniper::graphql_object]
impl TradeOperation {
    fn kind(&self) -> &TradeKind { &self.kind }
    fn pair(&self) -> &str { &self.pair }
    fn qty(&self) -> f64 { self.qty }
    fn price(&self) -> f64 { self.price }
    fn dry_mode(&self) -> bool { self.dry_mode }
    fn mode(&self) -> OrderMode { self.mode }
    fn asset_type(&self) -> &str { self.asset_type.as_ref() }
}

impl TradeOperation {
    pub fn new_id() -> String { Uuid::new_v4().to_string() }
    pub fn with_new_price(&mut self, new_price: f64) { self.price = new_price; }
}

impl From<TradeOperation> for AddOrderRequest {
    fn from(to: TradeOperation) -> Self {
        let mut request = AddOrderRequest {
            order_id: to.id,
            pair: to.pair.into(),
            side: to.kind.into(),
            quantity: Some(to.qty),
            price: Some(to.price),
            dry_run: to.dry_mode,
            side_effect_type: to.side_effect,
            ..AddOrderRequest::default()
        };
        match to.mode {
            OrderMode::Limit => {
                request.order_type = OrderType::Limit;
                request.enforcement = Some(OrderEnforcement::FOK);
            }
            OrderMode::Market => {
                request.order_type = OrderType::Market;
            }
        }

        request.asset_type = Some(to.asset_type);
        request
    }
}

pub type FeeAmount = f64;

/// All potential fees incurred by a [`FillEvent`].
#[derive(Debug, Default, Copy, Clone, PartialOrd, PartialEq, Serialize, Deserialize)]
pub struct Fees {
    /// Fee taken by the exchange/broker (eg/ commission).
    pub exchange: FeeAmount,
    /// Order book slippage modelled as a fee.
    pub slippage: FeeAmount,
    /// Fee incurred by any required network transactions (eg/ GAS).
    pub network: FeeAmount,
}

impl Fees {
    pub fn total(&self) -> f64 { self.exchange + self.slippage + self.network }
}

fn default_dry_mode() -> bool { true }

/// Order execution instructions for a market
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OrderConf {
    /// dry mode means orders won't be sent to the exchange but rather considered executed on the spot
    #[serde(default = "default_dry_mode")]
    pub dry_mode: bool,
    /// preference between limit and market orders, default is `OrderMode::Limit`
    #[serde(default)]
    pub order_mode: OrderMode,
    /// asset type to use for trading, this can change which api and account gets used, default is `AssetType::Spot`
    #[serde(default)]
    pub asset_type: AssetType,
    /// execution instructions for the portfolio, default is None
    #[allow(dead_code)]
    pub execution_instruction: Option<ExecutionInstruction>,
}

impl Default for OrderConf {
    fn default() -> Self {
        Self {
            dry_mode: true,
            order_mode: OrderMode::Limit,
            asset_type: AssetType::Spot,
            execution_instruction: None,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct MarketStat {
    #[serde(deserialize_with = "util::ser::parse_null_to_f64")]
    /// Weighted price
    pub w_price: f64,
    /// Volume
    pub vol: f64,
}

impl From<MarketEvent> for MarketStat {
    fn from(e: MarketEvent) -> Self {
        Self {
            w_price: e.vwap(),
            vol: e.vol(),
        }
    }
}

impl From<&MarketEvent> for MarketStat {
    fn from(e: &MarketEvent) -> Self {
        Self {
            w_price: e.vwap(),
            vol: e.vol(),
        }
    }
}
