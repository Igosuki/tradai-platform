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

const USD: Currency = Currency::new("USD", "$");
const GBP: Currency = Currency::new("GBP", "₤");
const JPY: Currency = Currency::new("JPY", "¥");
const EUR: Currency = Currency::new("EUR", "€");
const NZD: Currency = Currency::new("NZD", "$");
const AUD: Currency = Currency::new("AUD", "$");
const CAD: Currency = Currency::new("CAD", "$");
const CHF: Currency = Currency::new("CHF", "Fr");
const HKD: Currency = Currency::new("HKD", "$");
const SGD: Currency = Currency::new("SGD", "$");
const XAG: Currency = Currency::new("XAG", "Ag");
const XAU: Currency = Currency::new("XAU", "Au");
const CNH: Currency = Currency::new("CNH", "¥");
const CNY: Currency = Currency::new("CNY", "¥");
const CZK: Currency = Currency::new("CZK", "Kč");
const DKK: Currency = Currency::new("DKK", "kr");
const HUF: Currency = Currency::new("HUF", "Ft");
const INR: Currency = Currency::new("INR", "₹");
const MXN: Currency = Currency::new("MXN", "$");
const NOK: Currency = Currency::new("NOK", "kr");
const PLN: Currency = Currency::new("PLN", "zł");
const SAR: Currency = Currency::new("SAR", "﷼");
const SEK: Currency = Currency::new("SEK", "kr");
const THB: Currency = Currency::new("THB", "฿");
const TRY: Currency = Currency::new("TRY", "₺");
const TWD: Currency = Currency::new("TWD", "NT$");
const ZAR: Currency = Currency::new("ZAR", "R");
const BTC: Currency = Currency::new("BTC", "฿");
const BCH: Currency = Currency::new("BCH", "฿");
const LTC: Currency = Currency::new("LTC", "Ł");
const ETH: Currency = Currency::new("ETH", "Ξ");
const EOS: Currency = Currency::new("EOS", "EOS");
const XRP: Currency = Currency::new("XRP", "XRP");
const XLM: Currency = Currency::new("XLM", "XLM");
const ETC: Currency = Currency::new("ETC", "ETC");
const ZRX: Currency = Currency::new("ZRX", "ZRX");
const USDT: Currency = Currency::new("USDT", "USDT");
