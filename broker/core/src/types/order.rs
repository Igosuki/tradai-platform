use std::str::FromStr;

use rust_decimal::prelude::{FromPrimitive, ToPrimitive};
use rust_decimal::{Decimal, RoundingStrategy};
use uuid::Uuid;

use crate::error::Error;
use crate::exchange::Exchange;
use crate::pair::{step_precision, PairConf};
use crate::types::margin::MarginSideEffect;
use crate::types::{Asset, Pair};
use crate::{error, util};

#[derive(Debug, PartialEq, Clone, Deserialize, Serialize)]
pub struct OrderFill {
    pub id: Option<String>,
    pub price: f64,
    pub qty: f64,
    pub fee: f64,
    pub fee_asset: Asset,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct OrderSubmission {
    /// UNIX timestamp in ms (when the response was received)
    pub timestamp: i64,
    /// This identifiers is specific to the platform you use. You must store it somewhere if
    /// you want to modify/cancel the order later
    pub id: String,
    pub pair: Pair,
    pub client_id: String,
    pub price: f64,
    pub qty: f64,
    pub executed_qty: f64,
    pub cummulative_quote_qty: f64,
    pub status: OrderStatus,
    pub enforcement: OrderEnforcement,
    pub order_type: OrderType,
    pub side: TradeType,
    pub asset_type: AssetType,
    pub trades: Vec<OrderFill>,
    pub borrowed_amount: Option<f64>,
    pub borrow_asset: Option<String>,
}

impl Default for OrderSubmission {
    fn default() -> Self {
        Self {
            timestamp: 0,
            id: Uuid::new_v4().to_string(),
            pair: Pair::default(),
            client_id: "".to_string(),
            price: 0.0,
            qty: 0.0,
            executed_qty: 0.0,
            cummulative_quote_qty: 0.0,
            status: OrderStatus::New,
            enforcement: OrderEnforcement::GTC,
            order_type: OrderType::Limit,
            side: TradeType::Sell,
            asset_type: AssetType::Spot,
            trades: vec![],
            borrowed_amount: None,
            borrow_asset: None,
        }
    }
}

#[derive(Debug, PartialEq, Deserialize, Serialize, Clone, Copy, EnumString)]
pub enum OrderType {
    Limit,
    Market,
    StopLoss,
    StopLossLimit,
    TakeProfit,
    TakeProfitLimit,
    LimitMaker,
}

impl Default for OrderType {
    fn default() -> Self { Self::Limit }
}

#[derive(Display, Copy, Debug, PartialEq, Clone, Deserialize, Serialize)]
pub enum TradeType {
    Sell,
    Buy,
}

impl Default for TradeType {
    fn default() -> Self { Self::Buy }
}

impl From<String> for TradeType {
    fn from(s: String) -> Self {
        match s.to_lowercase().as_str() {
            "buy" => TradeType::Buy,
            _ => TradeType::Sell,
        }
    }
}

impl From<i64> for TradeType {
    fn from(v: i64) -> Self {
        if v == 0 {
            return TradeType::Buy;
        }
        TradeType::Sell
    }
}

impl From<TradeType> for i32 {
    fn from(tt: TradeType) -> Self {
        match tt {
            TradeType::Buy => 0,
            TradeType::Sell => 1,
        }
    }
}

impl From<bool> for TradeType {
    fn from(is_buyer_maker: bool) -> Self {
        if is_buyer_maker {
            TradeType::Buy
        } else {
            TradeType::Sell
        }
    }
}

/// Status of an order, this can typically change over time
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrderStatus {
    /// The order has been accepted by the engine.
    New,
    /// A part of the order has been filled.
    PartiallyFilled,
    /// The order has been completely filled.
    Filled,
    /// The order has been canceled by the user.
    Canceled,
    /// (currently unused)
    PendingCancel,
    /// The order was not accepted by the engine and not processed.
    Rejected,
    /// The order was canceled according to the order type's rules (e.g. LIMIT FOK orders with no fill, LIMIT IOC or MARKET orders that partially fill) or by the exchange, (e.g. orders canceled during liquidation, orders canceled during maintenance)
    Expired,
    /// Part of the order or all of the order's quantity has filled.
    Traded,
}

impl OrderStatus {
    pub fn is_rejection(&self) -> bool {
        matches!(
            self,
            OrderStatus::Rejected | OrderStatus::PendingCancel | OrderStatus::Canceled | OrderStatus::Expired
        )
    }
}

#[derive(Message, Debug, Clone, Serialize, Deserialize, PartialEq)]
#[rtype(result = "()")]
pub enum OrderQuery {
    AddOrder(AddOrderRequest),
}

impl OrderQuery {
    pub fn id(&self) -> String {
        match self {
            Self::AddOrder(req) => req.order_id.clone(),
        }
    }

    pub fn xch(&self) -> Exchange {
        match self {
            Self::AddOrder(req) => req.xch,
        }
    }

    pub fn pair(&self) -> Pair {
        match self {
            Self::AddOrder(req) => req.pair.clone(),
        }
    }

    pub fn validate(&self) -> error::Result<()> {
        match self {
            Self::AddOrder(req) => req.validate(),
        }
    }

    pub fn truncate(&self, pair_conf: &PairConf) -> Self {
        match self {
            Self::AddOrder(req) => Self::AddOrder(req.truncate(pair_conf)),
        }
    }
}

/// Order Request
/// perform an order for the account
#[derive(Debug, Clone, Deserialize, Serialize, PartialEq, Default)]
pub struct AddOrderRequest {
    pub xch: Exchange,
    pub pair: Pair,
    pub side: TradeType,
    pub order_type: OrderType,
    pub enforcement: Option<OrderEnforcement>,
    pub quantity: Option<f64>,
    pub quote_order_qty: Option<f64>,
    pub price: Option<f64>,
    /// A unique id for the order, automatically generated if not sent.
    pub order_id: String,
    /// Optional transaction id, if this order was part of a larger transaction
    pub transaction_id: Option<String>,
    /// Optional emitter id, for the entity that emitted the order
    pub emitter_id: Option<String>,
    /// Used with stop loss, stop loss limit, take profit and take profit limit order types.
    pub stop_price: Option<f64>,
    /// Used with limit, stop loss limit and take profit limit to create an iceberg order.
    pub iceberg_qty: Option<f64>,
    /// Whether or not to perform a test order instead of a real order
    pub dry_run: bool,
    /// Asset Type, Spot by default
    pub asset_type: Option<AssetType>,
    /// Side effect, for AssetType::Margin
    pub side_effect_type: Option<MarginSideEffect>,
}

impl AddOrderRequest {
    pub fn new_id() -> String { Uuid::new_v4().to_string() }

    pub fn with_new_order_id(&mut self) -> String {
        let string = Self::new_id();
        self.order_id = string.clone();
        string
    }

    pub fn validate(&self) -> error::Result<()> {
        if self.pair.is_empty() {
            return Err(Error::EmptyPair);
        }
        if self.quantity.filter(|&qty| qty >= 0.0).is_none() {
            return Err(Error::InvalidQty);
        }
        Ok(())
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
    pub fn truncate(&self, pair_conf: &PairConf) -> Self {
        // Change precision if the symbol information exists
        let mut new = self.clone();
        new.quantity = new.quantity.map(|q| {
            let precision = pair_conf
                .step_qty
                .and_then(|step_size| step_precision(step_size, '1'))
                .or_else(|| pair_conf.base_precision.map(|p| p as i32));
            if let Some(precision) = precision {
                Decimal::from_f64(q)
                    .and_then(|d| {
                        let rounded = d.round_dp_with_strategy(precision as u32, match self.asset_type {
                            Some(AssetType::Margin | AssetType::IsolatedMargin) => match self.side {
                                TradeType::Sell => RoundingStrategy::ToZero,
                                TradeType::Buy => RoundingStrategy::AwayFromZero,
                            },
                            // Some(AssetType::Spot)
                            _ => RoundingStrategy::ToZero,
                        });
                        rounded.to_f64()
                    })
                    .unwrap_or(q)
            } else {
                q
            }
        });
        new
    }

    pub fn with_dry(mut self) -> Self {
        self.dry_run = true;
        self
    }
}

const DEFAULT_BORROW_RATE: f64 = 1.0;

impl From<AddOrderRequest> for OrderSubmission {
    fn from(aor: AddOrderRequest) -> Self {
        let asset_type = aor.asset_type.unwrap_or(AssetType::Spot);
        let qty = aor.quantity.unwrap_or(0.0);
        let price = aor.price.unwrap_or(0.0);
        let pair_string = aor.pair.to_string();
        let (base_asset, quote_asset) = pair_string.split_once('_').unwrap();
        let (fee, fee_asset) = match aor.side {
            TradeType::Sell => (price * qty * aor.xch.default_fees(), quote_asset),
            TradeType::Buy => (qty * aor.xch.default_fees(), base_asset),
        };
        Self {
            timestamp: util::get_unix_timestamp_ms(),
            id: Uuid::new_v4().to_string(),
            pair: aor.pair.clone(),
            client_id: aor.order_id,
            price,
            qty,
            executed_qty: qty,
            cummulative_quote_qty: qty * price,
            status: OrderStatus::Filled,
            enforcement: aor.enforcement.unwrap_or(OrderEnforcement::FOK),
            order_type: aor.order_type,
            side: aor.side,
            asset_type,
            trades: vec![OrderFill {
                id: None,
                price,
                qty,
                fee,
                fee_asset: fee_asset.into(),
            }],
            // Borrow the full amount if we are in margin and doing margin_buy as a side effect
            borrowed_amount: aor
                .side_effect_type
                .filter(|e| asset_type.is_margin() && *e == MarginSideEffect::MarginBuy)
                .map(|_| match aor.side {
                    TradeType::Sell => qty,
                    TradeType::Buy => qty * price,
                } * DEFAULT_BORROW_RATE),
            borrow_asset: aor
                .side_effect_type
                .filter(|e| asset_type.is_margin() && *e == MarginSideEffect::MarginBuy)
                .map(|_| {
                    let borrow_asset = match aor.side {
                        TradeType::Sell => base_asset,
                        TradeType::Buy => quote_asset,
                    };
                    borrow_asset.to_string()
                }),
        }
    }
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, EnumString, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum AssetType {
    #[strum(serialize = "spot")]
    Spot,
    #[strum(serialize = "margin")]
    Margin,
    #[strum(serialize = "margin_funding")]
    MarginFunding,
    #[strum(serialize = "isolated_margin")]
    IsolatedMargin,
    #[strum(serialize = "index")]
    Index,
    #[strum(serialize = "binary")]
    Binary,
    #[strum(serialize = "perpetual_contract")]
    PerpetualContract,
    #[strum(serialize = "perpetual_swap")]
    PerpetualSwap,
    #[strum(serialize = "futures")]
    Futures,
    #[strum(serialize = "upside_profit_contract")]
    UpsideProfitContract,
    #[strum(serialize = "downside_profit_contract")]
    DownsideProfitContract,
    #[strum(serialize = "coin_margined_futures")]
    CoinMarginedFutures,
    #[strum(serialize = "usdt_margined_futures")]
    UsdtMarginedFutures,
    // TODO: this is a temporary fix because asset_type changed from camel to snake case
    #[serde(other)]
    Other,
}

impl AssetType {
    pub fn is_margin(&self) -> bool { matches!(self, Self::IsolatedMargin | Self::MarginFunding | Self::Margin) }
}

impl Default for AssetType {
    fn default() -> Self { Self::Spot }
}

impl From<String> for AssetType {
    fn from(s: String) -> Self { Self::from_str(s.as_ref()).unwrap() }
}

/// Why an order was rejected
#[derive(Clone, Debug, Deserialize, Serialize)]
pub enum OrderRejectionReason {
    None,
    UnknownInstrument,
    MarketClosed,
    PriceQtyExceedsHardLimit,
    UnknownOrder,
    DuplicateOrder,
    UnknownAccount,
    InsufficientBalance,
    AccountInactive,
    AccountCannotSettle,
    OrderWouldTriggerImmediatly,
}

/// How an order will be resolved
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Copy, EnumString)]
pub enum OrderEnforcement {
    /// Good Till Canceled
    GTC,
    /// Immediate Or Cancel
    IOC,
    /// Fill or Kill
    FOK,
    /// Good till executed
    GTX,
}

/// Order
/// the latest state of an order
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Order {
    pub xch: Exchange,
    pub symbol: Pair,
    pub order_id: String,
    pub orig_order_id: String,
    pub price: f64,
    pub orig_qty: f64,
    pub executed_qty: f64,
    pub cumulative_quote_qty: f64,
    pub status: OrderStatus,
    pub enforcement: OrderEnforcement,
    pub order_type: OrderType,
    pub side: TradeType,
    pub stop_price: f64,
    pub iceberg_qty: f64,
    pub orig_time: u64,
    pub last_event_time: u64,
    pub is_in_transaction: bool,
    pub orig_quote_order_qty: f64,
    pub asset_type: AssetType,
}
