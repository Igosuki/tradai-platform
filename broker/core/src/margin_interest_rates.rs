use dashmap::mapref::entry::Entry;
use dashmap::DashMap;
use std::sync::Arc;

use prometheus::CounterVec;

use crate::api::ExchangeApi;
use crate::error::*;
use crate::exchange::Exchange;
use crate::types::{InterestRate, Pair};

lazy_static! {
    static ref DEFAULT_INTEREST_RATE_REGISTRY: InterestRateRegistry = { InterestRateRegistry::default() };
    static ref FETCH_COUNTER: CounterVec =
        register_counter_vec!(opts!("fetch_interest_rate", "Fetch interest rate."), &["xchg", "asset"]).unwrap();
    static ref ERROR_COUNTER: CounterVec = register_counter_vec!(
        opts!("fetch_interest_rate_error", "Error when fetching interest rate."),
        &["xchg", "asset"]
    )
    .unwrap();
}

/// Default registry (global static).
#[must_use]
pub fn default_interest_rate_registry() -> &'static InterestRateRegistry {
    lazy_static::initialize(&DEFAULT_INTEREST_RATE_REGISTRY);
    &DEFAULT_INTEREST_RATE_REGISTRY
}

/// A struct for registering pairs for an exchange
#[derive(Clone, Debug)]
pub struct InterestRateRegistry {
    interest_rates: Arc<DashMap<Exchange, DashMap<String, InterestRate>>>,
}

impl Default for InterestRateRegistry {
    fn default() -> Self {
        InterestRateRegistry {
            interest_rates: Arc::new(DashMap::default()),
        }
    }
}

impl InterestRateRegistry {
    #[must_use]
    pub fn new() -> Self { Self::default() }

    pub fn register(&self, xchg: Exchange, asset: &str, interest_rate: InterestRate) {
        let writer = self.interest_rates.as_ref();
        match writer.entry(xchg) {
            Entry::Occupied(mut o) => {
                let asset_map = o.get_mut();
                asset_map.insert(asset.to_string(), interest_rate);
            }
            Entry::Vacant(v) => {
                let asset_map = DashMap::new();
                asset_map.insert(asset.to_string(), interest_rate);
                v.insert(asset_map);
            }
        }
    }

    #[must_use]
    pub fn interest_rate(&self, xchg: &Exchange, asset: &str) -> Option<InterestRate> {
        let asset_map = self.interest_rates.as_ref();
        let option = asset_map.get(xchg);
        option.and_then(|asset_interest_rates| asset_interest_rates.get(asset).map(|o| o.to_owned()))
    }

    /// # Errors
    ///
    /// If the exchange isn't found in the registry
    pub fn interest_rates(&self, xchg: &Exchange) -> Result<Vec<InterestRate>> {
        let exchange_map = self.interest_rates.as_ref();
        exchange_map
            .get(xchg)
            .ok_or(Error::ExchangeNotInPairRegistry)
            .map(|asset_map| asset_map.value().iter().map(|v| v.value().clone()).collect())
    }
}

#[must_use]
pub fn interest_rate(xchg: &Exchange, p: &Pair) -> Option<InterestRate> {
    default_interest_rate_registry().interest_rate(xchg, p)
}

/// # Errors
///
/// See [`InterestRateRegistry::interest_rates`]
pub fn interest_rates(xchg: &Exchange) -> Result<Vec<InterestRate>> {
    default_interest_rate_registry().interest_rates(xchg)
}

/// # Errors
///
/// If the interest rate fails to fetch for the exchange
pub async fn get_interest_rate(xchg: &Exchange, api: &'_ dyn ExchangeApi, asset: &str) -> Result<InterestRate> {
    let registry = default_interest_rate_registry();
    if let Some(rate) = registry.interest_rate(xchg, asset) {
        Ok(rate)
    } else {
        let xchg_string = xchg.to_string();
        FETCH_COUNTER.with_label_values(&[&xchg_string, asset]).inc();
        let interest_rate = api.margin_interest_rate(asset.into()).await.map_err(|e| {
            ERROR_COUNTER.with_label_values(&[&xchg_string, asset]).inc();
            e
        })?;
        registry.register(*xchg, asset, interest_rate.clone());
        Ok(interest_rate)
    }
}

#[cfg(test)]
mod test {
    use super::InterestRateRegistry;
    use crate::exchange::Exchange;
    use crate::types::{InterestRate, InterestRatePeriod};

    #[tokio::test]
    async fn registry_and_interest_rate_fns() {
        let registry = InterestRateRegistry::default();
        let exchange = Exchange::Binance;
        let rate = InterestRate {
            symbol: "BTC".to_string(),
            ts: 0,
            rate: 1.0,
            period: InterestRatePeriod::Daily,
        };
        registry.register(exchange, "BTC", rate.clone());
        assert_eq!(registry.interest_rate(&exchange, "BTC"), Some(rate));
    }
}
