use std::borrow::Borrow;
use std::cmp::Ordering;
use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::ops::Neg;
use std::sync::Arc;

use bimap::BiMap;
use dashmap::DashMap;
use fancy_regex::Regex;
use once_cell::sync::OnceCell;

use crate::api::ExchangeApi;
use crate::error::*;
use crate::exchange::Exchange;
use crate::types::{Pair, Symbol};

static DEFAULT_PAIR_REGISTRY: OnceCell<PairRegistry> = OnceCell::new();

/// Default registry (global static).
pub fn default_pair_registry() -> &'static PairRegistry { DEFAULT_PAIR_REGISTRY.get_or_init(PairRegistry::default) }

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct PairConf {
    /// the base asset the conf applies to
    pub base: String,
    /// the quote (or counter) asset
    pub quote: String,
    /// the exchange side symbol
    pub symbol: Symbol,
    /// the server side pair
    pub pair: Pair,
    /// price >= min_price
    pub min_price: Option<f64>,
    /// price <= max_price
    pub max_price: Option<f64>,
    /// (quantity - min_price) % step_price == 0
    pub step_price: Option<f64>,
    /// quantity >= min_qty
    pub min_qty: Option<f64>,
    /// quantity <= max_qty
    pub max_qty: Option<f64>,
    /// (quantity - min_qty) % step-qty == 0
    pub step_qty: Option<f64>,
    /// quantity >= min_qty
    pub min_market_qty: Option<f64>,
    /// quantity <= max_qty
    pub max_market_qty: Option<f64>,
    /// (quantity - min_qty) % step-qty == 0
    pub step_market_qty: Option<f64>,
    /// minimum price * qty
    pub min_size: Option<f64>,
    /// The base asset precision
    pub base_precision: Option<u32>,
    /// The quote asset precision
    pub quote_precision: Option<u32>,
    /// If spot trading is allowed
    pub spot_allowed: bool,
    /// If margin trading is allowed
    pub cross_margin_allowed: bool,
    /// If isolated margin trading is allowed
    pub isolated_margin_allowed: bool,
}

impl Hash for PairConf {
    fn hash<H: Hasher>(&self, state: &mut H) { self.symbol.hash(state); }
}

impl PartialEq for PairConf {
    fn eq(&self, other: &Self) -> bool { self.symbol == other.symbol }
}

impl Eq for PairConf {}

impl PartialOrd for PairConf {
    fn partial_cmp(&self, other: &PairConf) -> Option<Ordering> { Some(self.cmp(other)) }
}

// ordering only depends on the important data
impl Ord for PairConf {
    fn cmp(&self, other: &PairConf) -> Ordering { self.symbol.cmp(&other.symbol) }
}

impl Borrow<Symbol> for PairConf {
    fn borrow(&self) -> &Symbol { &self.symbol }
}

/// A struct for registering pairs for an exchange
#[derive(Clone, Debug)]
pub struct PairRegistry {
    pairs: Arc<DashMap<Exchange, BiMap<Pair, PairConf>>>,
}

impl Default for PairRegistry {
    fn default() -> Self {
        PairRegistry {
            pairs: Arc::new(DashMap::new()),
        }
    }
}

impl PairRegistry {
    pub fn new() -> Self { Self::default() }

    pub fn register(&self, xchg: Exchange, pairs: Vec<PairConf>) {
        let exchange_map = pairs
            .into_iter()
            .map(|pair_conf| (pair_conf.pair.clone(), pair_conf))
            .collect();
        let writer = self.pairs.as_ref();
        writer.insert(xchg, exchange_map);
    }

    pub fn pair_to_symbol(&self, xchg: &Exchange, p: &Pair) -> Result<Symbol> {
        let pairs_map = self.pairs.as_ref();
        pairs_map
            .get(xchg)
            .and_then(|pair_to_symbol| pair_to_symbol.get_by_left(p).map(|s| s.symbol.clone()))
            .ok_or(Error::PairUnsupported)
    }

    pub fn symbol_to_pair(&self, xchg: &Exchange, symbol: &Symbol) -> Result<Pair> {
        let pairs_map = self.pairs.as_ref();
        let option = pairs_map.get(xchg);
        option
            .and_then(|pair_to_symbol| pair_to_symbol.get_by_right::<Symbol>(symbol).cloned())
            .ok_or_else(|| Error::SymbolPairConversion(symbol.to_string()))
    }

    pub fn pair_conf(&self, xchg: &Exchange, p: &Pair) -> Result<PairConf> {
        let pairs_map = self.pairs.as_ref();
        let option = pairs_map.get(xchg);
        option
            .and_then(|pair_to_symbol| pair_to_symbol.get_by_left(p).map(ToOwned::to_owned))
            .ok_or(Error::PairUnsupported)
    }

    pub fn pair_confs(&self, xchg: &Exchange) -> Result<Vec<PairConf>> {
        let pairs_map = self.pairs.as_ref();
        pairs_map
            .get(xchg)
            .ok_or(Error::ExchangeNotInPairRegistry)
            .map(|bimap| bimap.right_values().cloned().collect())
    }

    pub fn pair_string(&self, xchg: Exchange, pair: &Pair) -> Result<String> {
        self.pair_to_symbol(&xchg, pair).map(|p| p.to_string())
    }

    /// # Panics
    ///
    /// if pair expressions are not valid regular expressions
    pub fn filter_pairs(&self, xchg: &Exchange, pairs_expressions: &[String]) -> Result<HashSet<Pair>> {
        let regexps: Vec<Regex> = pairs_expressions
            .iter()
            .map(|s| Regex::new(s.as_str()).unwrap())
            .collect();
        let slice = regexps.as_slice();
        let pairs_map = self.pairs.as_ref();
        pairs_map
            .get(xchg)
            .ok_or(Error::ExchangeNotInPairRegistry)
            .map(|xchg_map| {
                xchg_map
                    .left_values()
                    .filter(|&pair| slice.iter().any(|r| r.is_match(pair.as_ref()).unwrap()))
                    .cloned()
                    .collect()
            })
    }

    pub fn register_pair(&self, xchg: &Exchange, pair: Pair, symbol: Symbol) {
        self.register(*xchg, vec![PairConf {
            symbol,
            pair,
            ..PairConf::default()
        }]);
    }
}

pub fn pair_to_symbol(xchg: &Exchange, p: &Pair) -> Result<Symbol> { default_pair_registry().pair_to_symbol(xchg, p) }

pub fn symbol_to_pair(xchg: &Exchange, symbol: &Symbol) -> Result<Pair> {
    default_pair_registry().symbol_to_pair(xchg, symbol)
}

pub fn pair_conf(xchg: &Exchange, p: &Pair) -> Result<PairConf> { default_pair_registry().pair_conf(xchg, p) }

pub fn pair_confs(xchg: &Exchange) -> Result<Vec<PairConf>> { default_pair_registry().pair_confs(xchg) }

pub fn pair_string(xchg: Exchange, pair: &Pair) -> Result<String> { pair_to_symbol(&xchg, pair).map(|p| p.to_string()) }

pub fn filter_pairs(xchg: &Exchange, pairs_expressions: &[String]) -> Result<HashSet<Pair>> {
    default_pair_registry().filter_pairs(xchg, pairs_expressions)
}

pub async fn refresh_pairs(xchg: &Exchange, api: &'_ dyn ExchangeApi) -> Result<()> {
    let pair_confs = api.pairs().await?;
    default_pair_registry().register(*xchg, pair_confs);
    Ok(())
}

pub fn register_pair(xchg: &Exchange, pair: Pair, symbol: Symbol) {
    default_pair_registry().register_pair(xchg, pair, symbol);
}

/// Returns the precision x in 10^x of this float defined by `pattern`
/// # Arguments
///
/// * `step_size`: a floating point value
/// * `pattern`: the character to pattern match on, for instance '1'
///
/// returns: Option<i32>
///
/// # Examples
///
/// ```
///
/// ```
#[allow(clippy::cast_possible_truncation, clippy::cast_possible_wrap)]
pub fn step_precision(step_size: f64, pattern: char) -> Option<i32> {
    let step_size_str = step_size.to_string();
    let dot = step_size_str.find('.');
    step_size_str.find(pattern).map(|i| {
        let i = i as i32;
        if let Some(dot) = dot {
            let dot = dot as i32;
            if i < dot {
                i.neg()
            } else {
                i - dot
            }
        } else {
            i.neg()
        }
    })
}

/// # Panics
///
/// if the pair cannot be split once at '_'
pub fn register_pair_default(xch: Exchange, symbol: &str, pair: &str) {
    let (base, quote) = pair.split_once('_').unwrap();
    let conf = PairConf {
        symbol: symbol.into(),
        pair: pair.into(),
        base: base.to_string(),
        quote: quote.to_string(),
        ..PairConf::default()
    };
    default_pair_registry().register(xch, vec![conf]);
}

#[cfg(test)]
mod test {
    use std::collections::HashSet;
    use test::Bencher;

    use crate::exchange::Exchange;
    use crate::pair::{PairConf, PairRegistry};
    use crate::types::{Pair, Symbol};

    #[tokio::test]
    async fn registry_and_pair_fns() {
        let registry = PairRegistry::default();
        let exchange = Exchange::Binance;
        let symbol: Symbol = "BTCUSDT".into();
        let conf = PairConf {
            symbol: symbol.clone(),
            pair: "BTC_USDT".into(),
            ..PairConf::default()
        };
        registry.register(exchange, vec![conf.clone()]);
        assert_eq!(registry.symbol_to_pair(&exchange, &symbol), Ok("BTC_USDT".into()));
        assert_eq!(
            registry.pair_to_symbol(&exchange, &"BTC_USDT".to_string().into()),
            Ok(symbol)
        );
        assert_eq!(
            registry.pair_conf(&exchange, &"BTC_USDT".to_string().into()).unwrap(),
            conf
        );
    }

    #[bench]
    fn pair_conf_bench(b: &mut Bencher) {
        let registry = PairRegistry::default();
        let exchange = Exchange::Binance;
        let pair: Pair = "BTC_USDT".into();
        let conf = PairConf {
            symbol: "BTC_USDT".into(),
            pair: pair.clone(),
            ..PairConf::default()
        };
        registry.register(exchange, vec![conf]);
        b.iter(|| registry.pair_conf(&exchange, &pair).unwrap());
    }

    #[bench]
    fn symbol_to_pair2_bench(b: &mut Bencher) {
        let registry = PairRegistry::default();
        let exchange = Exchange::Binance;
        let symbol: Symbol = "BTCUSDT".into();
        let conf = PairConf {
            symbol: symbol.clone(),
            pair: "BTC_USDT".into(),
            ..PairConf::default()
        };
        registry.register(exchange, vec![conf]);
        b.iter(|| registry.symbol_to_pair(&exchange, &symbol).unwrap());
    }

    #[bench]
    fn pair_to_symbol2_bench(b: &mut Bencher) {
        let registry = PairRegistry::default();
        let exchange = Exchange::Binance;
        let symbol: Symbol = "BTCUSDT".into();
        let pair: Pair = "BTC_USDT".into();
        let conf = PairConf {
            symbol,
            pair: pair.clone(),
            ..PairConf::default()
        };
        registry.register(exchange, vec![conf]);
        b.iter(|| registry.pair_to_symbol(&exchange, &pair).unwrap());
    }

    #[tokio::test]
    async fn filter_pairs_with_regex() {
        let registry = PairRegistry::default();
        let exchange = Exchange::Binance;
        let symbol: Symbol = "BTCUSDT".into();
        let pair: Pair = "BTC_USDT".into();
        let conf = PairConf {
            symbol,
            pair: pair.clone(),
            ..PairConf::default()
        };
        registry.register(exchange, vec![conf]);
        let expected: crate::error::Result<HashSet<Pair>> = Ok(vec![pair].into_iter().collect());
        assert_eq!(registry.filter_pairs(&exchange, &[".*_USDT".to_string()]), expected);
        assert_eq!(registry.filter_pairs(&exchange, &["BTC_USDT".to_string()]), expected);
        assert_eq!(
            registry.filter_pairs(&exchange, &["INVALID".to_string()]),
            Ok(HashSet::new())
        );
        assert_eq!(
            registry.filter_pairs(&Exchange::Bittrex, &[]),
            Err(crate::error::Error::ExchangeNotInPairRegistry)
        );
    }
}
