use crate::error::*;
use crate::exchange::Exchange;
use crate::pair::symbol_to_pair;
use crate::types::{MarketSymbol, Pair};

pub trait StreamingApi {
    const NAME: &'static str;
    const EXCHANGE: Exchange;

    /// # Errors
    ///
    /// if the pair cannot be converted
    fn get_pair(&self, symbol: &str) -> Result<Pair> { symbol_to_pair(&Self::EXCHANGE, &MarketSymbol::from(symbol)) }
}
