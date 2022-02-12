#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, EnumString, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum MarginSideEffect {
    #[strum(serialize = "no_side_effect")]
    NoSideEffect,
    #[strum(serialize = "margin_buy")]
    MarginBuy,
    #[strum(serialize = "auto_repay")]
    AutoRepay,
}

#[derive(Copy, Clone, Debug, Serialize, Deserialize, PartialEq, Eq, EnumString, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum InterestRatePeriod {
    #[strum(serialize = "daily")]
    Daily,
    #[strum(serialize = "yearly")]
    Yearly,
    #[strum(serialize = "hourly")]
    Hourly,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarginAccountDetails {
    pub summary: MarginAssetSummary,
    pub borrow_enabled: bool,
    pub trade_enabled: bool,
    pub transfer_enabled: bool,
    pub margin_assets: Vec<MarginAsset>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarginIsolatedAsset {}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarginAsset {
    pub asset: String,
    pub borrowed: f64,
    pub free: f64,
    pub interest: f64,
    pub locked: f64,
    pub net_asset: f64,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MarginAssetSummary {
    pub asset: String,
    pub margin_level: f64,
    pub total: f64,
    pub total_liability: f64,
    pub total_net: f64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct InterestRate {
    pub symbol: String,
    pub ts: u64,
    pub rate: f64,
    pub period: InterestRatePeriod,
}

impl InterestRate {
    #[allow(clippy::cast_sign_loss, clippy::cast_lossless, clippy::cast_precision_loss)]
    pub fn resolve(&self, amount: f64, hours: i64) -> f64 {
        let divider = match self.period {
            InterestRatePeriod::Daily => 24,
            InterestRatePeriod::Yearly => 365 * 24,
            InterestRatePeriod::Hourly => 1,
        };
        amount * (self.rate / divider as f64) * hours as f64
    }
}
