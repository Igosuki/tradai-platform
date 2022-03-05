#![allow(dead_code)]

//! Predefined currencies directly usable in code
//! The currency value is a string representation, symbol is the currency symbol such as '$'
//! usable in numerical representations

pub struct Currency {
    pub value: &'static str,
    pub symbol: &'static str,
}

impl Currency {
    const fn new(name: &'static str, symbol: &'static str) -> Self { Self { value: name, symbol } }
}

pub const USD: Currency = Currency::new("USD", "$");
pub const GBP: Currency = Currency::new("GBP", "₤");
pub const JPY: Currency = Currency::new("JPY", "¥");
pub const EUR: Currency = Currency::new("EUR", "€");
pub const NZD: Currency = Currency::new("NZD", "$");
pub const AUD: Currency = Currency::new("AUD", "$");
pub const CAD: Currency = Currency::new("CAD", "$");
pub const CHF: Currency = Currency::new("CHF", "Fr");
pub const HKD: Currency = Currency::new("HKD", "$");
pub const SGD: Currency = Currency::new("SGD", "$");
pub const XAG: Currency = Currency::new("XAG", "Ag");
pub const XAU: Currency = Currency::new("XAU", "Au");
pub const CNH: Currency = Currency::new("CNH", "¥");
pub const CNY: Currency = Currency::new("CNY", "¥");
pub const CZK: Currency = Currency::new("CZK", "Kč");
pub const DKK: Currency = Currency::new("DKK", "kr");
pub const HUF: Currency = Currency::new("HUF", "Ft");
pub const INR: Currency = Currency::new("INR", "₹");
pub const MXN: Currency = Currency::new("MXN", "$");
pub const NOK: Currency = Currency::new("NOK", "kr");
pub const PLN: Currency = Currency::new("PLN", "zł");
pub const SAR: Currency = Currency::new("SAR", "﷼");
pub const SEK: Currency = Currency::new("SEK", "kr");
pub const THB: Currency = Currency::new("THB", "฿");
pub const TRY: Currency = Currency::new("TRY", "₺");
pub const TWD: Currency = Currency::new("TWD", "NT$");
pub const ZAR: Currency = Currency::new("ZAR", "R");
pub const BTC: Currency = Currency::new("BTC", "฿");
pub const BCH: Currency = Currency::new("BCH", "฿");
pub const LTC: Currency = Currency::new("LTC", "Ł");
pub const ETH: Currency = Currency::new("ETH", "Ξ");
pub const EOS: Currency = Currency::new("EOS", "EOS");
pub const XRP: Currency = Currency::new("XRP", "XRP");
pub const XLM: Currency = Currency::new("XLM", "XLM");
pub const ETC: Currency = Currency::new("ETC", "ETC");
pub const ZRX: Currency = Currency::new("ZRX", "ZRX");
pub const USDT: Currency = Currency::new("USDT", "USDT");
