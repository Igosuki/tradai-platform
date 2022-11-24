use crate::types::MarketChannel;

fn default_as_false() -> bool { false }

#[derive(Clone, Debug, Serialize, Deserialize)]
#[allow(clippy::struct_excessive_bools)]
pub struct BrokerSettings {
    pub market_channels: Vec<MarketChannel>,
    pub fees: f64,
    pub use_account: bool,
    pub use_margin_account: bool,
    pub use_isolated_margin_account: bool,
    pub isolated_margin_account_pairs: Vec<String>,
    #[serde(default = "default_as_false")]
    pub use_test: bool,
}

impl BrokerSettings {
    pub fn default_test(fees: f64) -> Self {
        Self {
            fees,
            market_channels: vec![],
            use_margin_account: true,
            use_account: true,
            use_test: true,
            use_isolated_margin_account: true,
            isolated_margin_account_pairs: vec![],
        }
    }
}
