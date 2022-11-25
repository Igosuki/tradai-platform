use crate::error::Error;
use crate::exchange::Exchange;
use chrono::{DateTime, TimeZone, Utc};
use std::hash::{Hash, Hasher};
use string_cache::DefaultAtom as Atom;

pub type Amount = f64;
pub type Price = f64;
pub type Volume = f64;

/// A pair is string representation of a base and quote asset joined together, for instance 'BTC_USDT' or 'TSLA_USDT'
pub type Pair = Atom;

/// A market symbol is the market side representation of a symbol, for instance 'BTCUSDT' or 'TSLA'
pub type MarketSymbol = Atom;

/// An asset is string representation of a single base or quote asset used in markets, for instance 'BTC', 'USDT' or 'TSLA'
pub type Asset = Atom;

/// Type of tradable security / underlying asset
#[derive(Serialize, Deserialize, Debug, Copy, Clone, PartialEq, Eq, Hash, EnumString, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum SecurityType {
    /// US Equity Security
    #[strum(serialize = "equity")]
    Equity,
    /// Option Security Type
    #[strum(serialize = "option")]
    Option,
    /// Commodity Security Type
    #[strum(serialize = "commodity")]
    Commodity,
    /// FOREX Security
    #[strum(serialize = "forex")]
    Forex,
    /// Future Security Type
    #[strum(serialize = "futures", serialize = "future")]
    Future,
    /// Contract For a Difference Security Type.
    #[strum(serialize = "cfd")]
    Cfd,
    /// Cryptocurrency Security Type.
    #[strum(serialize = "crypto", serialize = "spot")]
    Crypto,
    /// Futures Options Security Type.
    /// <remarks>
    /// Futures options function similar to equity options, but with a few key differences.
    /// Firstly, the contract unit of trade is 1x, rather than 100x. This means that each
    /// option represents the right to buy or sell 1 future contract at expiry/exercise.
    /// The contract multiplier for Futures Options plays a big part in determining the premium
    /// of the option, which can also differ from the underlying future's multiplier.
    /// </remarks>
    #[strum(serialize = "future_option")]
    FutureOption,
    /// Index Security Type.
    #[strum(serialize = "index")]
    Index,
    /// Index Option Security Type.
    /// For index options traded on American markets, they tend to be European-style options and are Cash-settled.
    #[strum(serialize = "index_option")]
    IndexOption,
}

impl SecurityType {
    pub fn is_option(&self) -> bool { matches!(self, Self::Option | Self::FutureOption | Self::IndexOption) }
}

/// A unique value for a ticker symbol, an exchange market, and a security type
#[derive(typed_builder::TypedBuilder, Serialize, Deserialize, Debug, Clone)]
pub struct Symbol {
    pub r#type: SecurityType,
    /// First date at which the security was traded
    #[builder(default_code = "Utc.timestamp_millis_opt(0)").unwrap()]
    pub date: DateTime<Utc>,
    pub value: Pair,
    pub xch: Exchange,
    #[builder(default, setter(strip_option))]
    strike_price: Option<f64>,
    #[builder(default, setter(strip_option))]
    option_type: Option<OptionType>,
}

impl Symbol {
    pub fn new(symbol: Pair, security_type: SecurityType, exchange: Exchange) -> Self {
        Self::builder()
            .xch(exchange)
            .value(symbol)
            .r#type(security_type)
            .build()
    }

    pub fn new_option(symbol: Pair, exchange: Exchange, strike_price: f64, option_type: OptionType) -> Self {
        Self::builder()
            .xch(exchange)
            .value(symbol)
            .r#type(SecurityType::Option)
            .strike_price(strike_price)
            .option_type(option_type)
            .build()
    }

    pub fn new_future(symbol: Pair, exchange: Exchange, expiry: Option<DateTime<Utc>>) -> Self {
        Self::builder()
            .xch(exchange)
            .value(symbol)
            .r#type(SecurityType::Future)
            .date(expiry.unwrap_or_else(|| Utc.timestamp_millis_opt(0))).unwrap()
            .build()
    }

    pub fn strike_price(&self) -> crate::error::Result<f64> {
        match self.strike_price {
            Some(v) => Ok(v),
            None if !self.r#type.is_option() => Err(Error::InvalidOperation(
                "strike_price".to_string(),
                "non option type".to_string(),
            )),
            None => Err(Error::InvalidOperation(
                "strike_price".to_string(),
                "not set".to_string(),
            )),
        }
    }

    pub fn option_type(&self) -> crate::error::Result<OptionType> {
        match self.option_type {
            Some(v) => Ok(v),
            None if !self.r#type.is_option() => Err(Error::InvalidOperation(
                "option_type".to_string(),
                "non option type".to_string(),
            )),
            None => Err(Error::InvalidOperation(
                "option_type".to_string(),
                "not set".to_string(),
            )),
        }
    }
}

impl Hash for Symbol {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.value.hash(state);
        self.xch.hash(state);
        self.r#type.hash(state);
    }
}

impl PartialEq for Symbol {
    fn eq(&self, other: &Self) -> bool {
        self.value.eq(&other.value) && self.xch.eq(&other.xch) && self.r#type.eq(&other.r#type)
    }
}

impl Eq for Symbol {}

/// Types of options
#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum OptionType {
    /// A call option, the right to buy at the strike price
    Call,
    /// A put option, the right to sell at the strike price
    Put,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum OptionStyle {
    /// American style options are able to be exercised at any time on or before the expiration date
    American,
    /// European style options are able to be exercised on the expiration date only
    European,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum DelistingType {
    Warning,
    Delisted,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum SplitType {
    Warning,
    Split,
}

#[derive(Copy, Clone, Serialize, Deserialize, Debug)]
pub enum SettlementType {
    PhysicalDelivery,
    Cash,
}
