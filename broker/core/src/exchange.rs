#[derive(
    Debug, Display, PartialEq, Clone, Copy, Eq, Hash, Deserialize, Serialize, PartialOrd, Ord, EnumString, AsRefStr,
)]
#[serde(rename_all = "snake_case")]
pub enum Exchange {
    #[strum(serialize = "bitstamp")]
    Bitstamp,
    #[strum(serialize = "kraken")]
    Kraken,
    #[strum(serialize = "poloniex")]
    Poloniex,
    #[strum(serialize = "bittrex")]
    Bittrex,
    #[strum(serialize = "coinbase")]
    Coinbase,
    #[strum(serialize = "binance")]
    Binance,
}

impl Exchange {
    #[must_use]
    pub fn default_fees() -> f64 { 0.001 }

    #[must_use]
    pub fn capitalized(&self) -> String {
        let mut c = self.as_ref().chars();
        match c.next() {
            None => String::new(),
            Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
        }
    }
}

impl Default for Exchange {
    fn default() -> Self { Self::Binance }
}
