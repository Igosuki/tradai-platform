#![allow(non_snake_case)]

use serde::{Deserialize, Serialize};

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct FillEntry {
    #[serde(alias = "F")]
    FillType: String,
    #[serde(alias = "I")]
    Id: i32,
    #[serde(alias = "OT")]
    OrderType: String,
    #[serde(alias = "P")]
    Price: f64,
    #[serde(alias = "Q")]
    Quantity: f64,
    #[serde(alias = "T")]
    TimeStamp: i64,
    #[serde(alias = "U")]
    Uuid: String,
    #[serde(alias = "t")]
    Total: f64,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OrderPair {
    #[serde(alias = "Q")]
    pub Q: f64,
    #[serde(alias = "R")]
    pub R: f64,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct ExchangeState {
    #[serde(alias = "M")]
    pub MarketName: String,
    #[serde(alias = "N")]
    pub Nonce: i32,
    #[serde(alias = "Z")]
    pub Buys: Vec<OrderPair>,
    #[serde(alias = "S")]
    pub Sells: Vec<OrderPair>,
    #[serde(alias = "f")]
    Fills: Vec<FillEntry>,
}

#[allow(non_snake_case, clippy::struct_excessive_bools)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Order {
    #[serde(alias = "U")]
    Uuid: String,
    #[serde(alias = "OU")]
    OrderUuid: String,
    #[serde(alias = "I")]
    Id: i64,
    #[serde(alias = "E")]
    Exchange: String,
    #[serde(alias = "OT")]
    OrderType: String,
    #[serde(alias = "Q")]
    Quantity: f64,
    #[serde(alias = "q")]
    QuantityRemaining: f64,
    #[serde(alias = "X")]
    Limit: f64,
    #[serde(alias = "n")]
    CommissionPaid: f64,
    #[serde(alias = "P")]
    Price: f64,
    #[serde(alias = "PU")]
    PricePerUnit: f64,
    #[serde(alias = "Y")]
    Opened: i64,
    #[serde(alias = "C")]
    Closed: i64,
    #[serde(alias = "i")]
    IsOpen: bool,
    #[serde(alias = "CI")]
    CancelInitiated: bool,
    #[serde(alias = "K")]
    ImmediateOrCancel: bool,
    #[serde(alias = "k")]
    IsConditional: bool,
    #[serde(alias = "J")]
    Condition: String,
    #[serde(alias = "j")]
    ConditionTarget: f64,
    #[serde(alias = "u")]
    Updated: i64,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OrderDelta {
    #[serde(alias = "w")]
    AccountUuid: String,
    #[serde(alias = "N")]
    Nonce: i32,
    #[serde(alias = "TY")]
    Type: i32,
    #[serde(alias = "o")]
    Order: Order,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub(crate) enum TradeType {
    Add = 0,
    Remove = 1,
    Update = 2,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct OrderLog {
    #[serde(alias = "TY")]
    pub Type: i32,
    #[serde(alias = "R")]
    pub Rate: f64,
    #[serde(alias = "Q")]
    pub Quantity: f64,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct Fill {
    #[serde(alias = "FI")]
    FillId: i32,
    #[serde(alias = "OT")]
    pub OrderType: String,
    #[serde(alias = "R")]
    pub Rate: f64,
    #[serde(alias = "Q")]
    pub Quantity: f64,
    #[serde(alias = "T")]
    pub TimeStamp: u64,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct MarketDelta {
    #[serde(alias = "M")]
    pub MarketName: String,
    #[serde(alias = "N")]
    Nonce: i32,
    #[serde(alias = "Z")]
    pub Buys: Vec<OrderLog>,
    #[serde(alias = "S")]
    pub Sells: Vec<OrderLog>,
    #[serde(alias = "f")]
    pub Fills: Vec<Fill>,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SummaryDelta {
    #[serde(alias = "M")]
    MarketName: String,
    #[serde(alias = "H")]
    High: f64,
    #[serde(alias = "L")]
    Low: f64,
    #[serde(alias = "V")]
    Volume: f64,
    #[serde(alias = "l")]
    Last: f64,
    #[serde(alias = "m")]
    BaseVolume: f64,
    #[serde(alias = "T")]
    TimeStamp: i64,
    #[serde(alias = "B")]
    Bid: f64,
    #[serde(alias = "A")]
    Ask: f64,
    #[serde(alias = "G")]
    OpenBuyOrders: i32,
    #[serde(alias = "g")]
    OpenSellOrders: i32,
    #[serde(alias = "PD")]
    PrevDay: f64,
    #[serde(alias = "x")]
    Created: i64,
}

#[allow(non_snake_case)]
#[derive(Debug, Serialize, Deserialize)]
pub(crate) struct SummaryDeltaResponse {
    #[serde(alias = "N")]
    Nonce: i32,
    #[serde(alias = "D")]
    Deltas: Vec<SummaryDelta>,
}
