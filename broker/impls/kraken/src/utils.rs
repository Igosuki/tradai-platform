use broker_core::error::*;
use broker_core::prelude::*;
use broker_core::types::*;

/// Return the name associated to the pair used by Kraken
/// If the Pair is not supported, None is returned.
pub fn get_pair_string(pair: &Pair) -> Result<MarketSymbol> {
    broker_core::pair::pair_to_symbol(&Exchange::Kraken, pair)
}

/// Return the Pair associated to the string used by Kraken
/// If the Pair is not supported, None is returned.
#[allow(dead_code)]
pub fn get_pair(symbol: &str) -> Result<Pair> {
    broker_core::pair::symbol_to_pair(&Exchange::Kraken, &MarketSymbol::from(symbol))
}

#[derive(Deserialize)]
pub struct KrakenResponse<T> {
    error: Vec<String>,
    result: T,
}

/// If error array is null, return the result (encoded in a json object)
/// else return the error string found in array
pub fn parse_result<T>(response: &KrakenResponse<T>) -> Result<&T> {
    if response.error.is_empty() {
        Ok(&response.result)
    } else {
        let error = match response.error[0].as_ref() {
            "EService:Unavailable" => Error::ServiceUnavailable("Unknown...".to_string()),
            "EAPI:Invalid key" => Error::BadCredentials,
            "EAPI:Invalid nonce" => Error::InvalidNonce,
            "EOrder:Rate limit exceeded" => Error::RateLimitExceeded,
            "EQuery:Unknown asset pair" => Error::PairUnsupported,
            "EGeneral:Invalid arguments" => Error::InvalidArguments,
            "EGeneral:Permission denied" => Error::PermissionDenied,
            "EOrder:Insufficient funds" => Error::InsufficientFunds,
            "EOrder:Order minimum not met" => Error::InsufficientOrderSize,
            other => Error::ExchangeSpecificError(other.to_string()),
        };
        Err(error)
    }
}

/// Return the currency enum associated with the
/// string used by Kraken. If no currency is found,
/// return None
/// # Examples
///
/// ```
/// use crate::coinnect::kraken::utils::get_currency_enum;
/// use crate::coinnect::types::Currency;
///
/// let currency = get_currency_enum("ZUSD");
/// assert_eq!(Some(Currency::USD), currency);
/// ```
pub fn get_currency_enum(currency: &str) -> Option<Asset> {
    match currency {
        "ZEUR" => Some("EUR".into()),
        "ZCAD" => Some("CAD".into()),
        "ZGBP" => Some("GBP".into()),
        "ZJPY" => Some("JPY".into()),
        "ZUSD" => Some("USD".into()),
        "XDASH" => Some("DASH".into()),
        "XETC" => Some("ETC".into()),
        "XETH" => Some("ETH".into()),
        "XGNO" => Some("GNO".into()),
        "XICN" => Some("ICN".into()),
        "XLTC" => Some("LTC".into()),
        "XMLN" => Some("MLN".into()),
        "XREP" => Some("REP".into()),
        "XUSDT" => Some("USDT".into()),
        "XXBT" => Some("BTC".into()),
        "XXDG" => Some("XDG".into()),
        "XXLM" => Some("XLM".into()),
        "XXMR" => Some("XMR".into()),
        "XXRP" => Some("XRP".into()),
        "XZEC" => Some("ZEC".into()),
        _ => None,
    }
}

/// Return the currency String associated with the
/// string used by Kraken. If no currency is found,
/// return None
/// # Examples
///
/// ```
/// use crate::coinnect::kraken::utils::get_currency_string;
/// use crate::coinnect::types::Currency;
///
/// let currency = get_currency_string(Currency::BTC);
/// assert_eq!(currency, Some("XXBT".to_string()));
/// ```
#[allow(dead_code)]
pub fn get_currency_string(currency: &Asset) -> Option<String> {
    match currency.as_ref() {
        "EUR" => Some("ZEUR".to_string()),
        "CAD" => Some("ZCAD".to_string()),
        "GBP" => Some("ZGBP".to_string()),
        "JPY" => Some("ZJPY".to_string()),
        "USD" => Some("ZUSD".to_string()),
        "DASH" => Some("XDASH".to_string()),
        "ETC" => Some("XETC".to_string()),
        "ETH" => Some("XETH".to_string()),
        "GNO" => Some("XGNO".to_string()),
        "ICN" => Some("XICN".to_string()),
        "LTC" => Some("XLTC".to_string()),
        "MLN" => Some("XMLN".to_string()),
        "REP" => Some("XREP".to_string()),
        "USDT" => Some("XUSDT".to_string()),
        "BTC" => Some("XXBT".to_string()),
        "XDG" => Some("XXDG".to_string()),
        "XLM" => Some("XXLM".to_string()),
        "XMR" => Some("XXMR".to_string()),
        "XRP" => Some("XXRP".to_string()),
        "ZEC" => Some("XZEC".to_string()),
        _ => None,
    }
}
