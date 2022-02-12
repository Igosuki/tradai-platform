#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TradesSettings {
    pub symbols: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum OrderbookStyle {
    Live,
    Detailed,
    Diff,
}

impl Default for OrderbookStyle {
    fn default() -> Self { Self::Live }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderbookSettings {
    pub symbols: Vec<String>,
    #[serde(default)]
    pub style: OrderbookStyle,
}

fn default_as_false() -> bool { false }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct ExchangeSettings {
    pub orderbook: Option<OrderbookSettings>,
    pub orderbook_depth: Option<u16>,
    pub trades: Option<TradesSettings>,
    pub fees: f64,
    pub use_account: bool,
    pub use_margin_account: bool,
    pub use_isolated_margin_account: bool,
    pub isolated_margin_account_pairs: Vec<String>,
    #[serde(default = "default_as_false")]
    pub use_test: bool,
}

impl ExchangeSettings {
    pub fn default_test(fees: f64) -> Self {
        Self {
            fees,
            trades: None,
            orderbook: None,
            orderbook_depth: None,
            use_margin_account: true,
            use_account: true,
            use_test: true,
            use_isolated_margin_account: true,
            isolated_margin_account_pairs: vec![],
        }
    }
}
