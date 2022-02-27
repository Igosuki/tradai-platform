#[allow(dead_code)]
use crate::error::Error;
use chrono::{Date, Utc};
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
pub enum SecurityType {
    /// Base class for all security types:
    Base,

    /// US Equity Security
    Equity,

    /// Option Security Type
    Option,

    /// Commodity Security Type
    Commodity,

    /// FOREX Security
    Forex,

    /// Future Security Type
    Future,

    /// Contract For a Difference Security Type.
    Cfd,

    /// Cryptocurrency Security Type.
    Crypto,

    /// Futures Options Security Type.
    /// <remarks>
    /// Futures options function similar to equity options, but with a few key differences.
    /// Firstly, the contract unit of trade is 1x, rather than 100x. This means that each
    /// option represents the right to buy or sell 1 future contract at expiry/exercise.
    /// The contract multiplier for Futures Options plays a big part in determining the premium
    /// of the option, which can also differ from the underlying future's multiplier.
    /// </remarks>
    FutureOption,

    /// Index Security Type.
    Index,

    /// Index Option Security Type.
    /// For index options traded on American markets, they tend to be European-style options and are Cash-settled.
    IndexOption,
}

impl SecurityType {
    pub fn is_option(&self) -> bool { matches!(self, Self::Option | Self::FutureOption | Self::IndexOption) }
}

struct Symbol {
    value: String,
    id: SecurityId,
    underlying: Box<Option<Symbol>>,
    r#type: SecurityType,
}

struct SecurityId {
    r#type: SecurityType,
    underlying: Box<Option<SecurityId>>,
    date: Date<Utc>,
    symbol: String,
    xch: String,
    strike_price: Option<f64>,
    option_type: Option<OptionType>,
}

impl SecurityId {
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

/// Types of options
#[derive(Copy, Clone)]
enum OptionType {
    /// A call option, the right to buy at the strike price
    Call,
    /// A put option, the right to sell at the strike price
    Put,
}

enum OptionStyle {
    /// American style options are able to be exercised at any time on or before the expiration date
    American,
    /// European style options are able to be exercised on the expiration date only
    European,
}
