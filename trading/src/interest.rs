use std::fmt::Debug;

use actix::{Actor, Addr, Context, Handler, ResponseActFuture, WrapFuture};
use chrono::Utc;
use prometheus::CounterVec;

use brokers::exchange::Exchange;
use brokers::manager::ExchangeManagerRef;
use brokers::margin_interest_rates::get_interest_rate;
use brokers::types::{InterestRate, InterestRatePeriod};
use ext::ResultExt;

use crate::error::{Error, Result};
use crate::order_manager::types::OrderDetail;

lazy_static! {
    static ref GET_COUNTER: CounterVec =
        register_counter_vec!(opts!("get_interest_rate", "Get interest rate from the actor."), &[
            "xchg", "asset"
        ])
        .unwrap();
}

/// Provides interest rates
#[async_trait]
pub trait InterestRateProvider: Send + Sync + Debug {
    /// Get the interest rate for this exchange and asset
    async fn get_interest_rate(&self, exchange: Exchange, asset: String) -> Result<InterestRate>;

    /// Get the accumulated interest fees since the order was opened on the exchange
    async fn interest_fees_since(&self, exchange: Exchange, order: &OrderDetail) -> Result<f64> {
        let i = if order.asset_type.is_margin() && order.borrowed_amount.is_some() {
            let interest_rate = self.get_interest_rate(exchange, order.base_asset.clone()).await?;
            order.total_interest(&interest_rate)
        } else {
            0.0
        };
        Ok(i)
    }

    /// Get the accumulated interest fees since the order was opened on the exchange in quote asset
    async fn quote_interest_fees_since(&self, exchange: Exchange, order: &OrderDetail) -> Result<f64> {
        let i = if order.asset_type.is_margin() && order.borrowed_amount.is_some() {
            let interest_rate = self.get_interest_rate(exchange, order.base_asset.clone()).await?;
            order.total_quote_interest(&interest_rate)
        } else {
            0.0
        };
        Ok(i)
    }
}

#[derive(Debug)]
pub struct FlatInterestRateProvider {
    rate: f64,
}

impl FlatInterestRateProvider {
    pub fn new(rate: f64) -> Self { Self { rate } }
}

#[async_trait]
impl InterestRateProvider for FlatInterestRateProvider {
    #[allow(clippy::cast_sign_loss)]
    async fn get_interest_rate(&self, _exchange: Exchange, asset: String) -> Result<InterestRate> {
        Ok(InterestRate {
            symbol: asset,
            ts: Utc::now().timestamp_millis() as u64,
            rate: self.rate,
            period: InterestRatePeriod::Daily,
        })
    }
}

#[derive(Debug)]
pub struct MarginInterestRateProviderClient {
    inner: Addr<MarginInterestRateProvider>,
}

impl MarginInterestRateProviderClient {
    pub fn new(inner: Addr<MarginInterestRateProvider>) -> Self { Self { inner } }
}

#[async_trait]
impl InterestRateProvider for MarginInterestRateProviderClient {
    async fn get_interest_rate(&self, exchange: Exchange, asset: String) -> Result<InterestRate> {
        self.inner
            .send(GetInterestRate { exchange, asset })
            .await
            .map_err(|_| Error::InterestRateProviderMailboxError)?
            .err_into()
    }
}

#[derive(actix::Message)]
#[rtype(result = "Result<InterestRate>")]
pub struct GetInterestRate {
    pub exchange: Exchange,
    pub asset: String,
}

#[derive(Debug)]
pub struct MarginInterestRateProvider {
    apis: ExchangeManagerRef,
}

impl MarginInterestRateProvider {
    #[allow(clippy::needless_pass_by_value)]
    pub fn new(apis: ExchangeManagerRef) -> Self { Self { apis } }

    pub fn actor(apis: ExchangeManagerRef) -> Addr<Self> { Self::start(Self { apis }) }
}

impl Actor for MarginInterestRateProvider {
    type Context = Context<Self>;
}

impl Handler<GetInterestRate> for MarginInterestRateProvider {
    type Result = ResponseActFuture<Self, Result<InterestRate>>;

    fn handle(&mut self, msg: GetInterestRate, _ctx: &mut Self::Context) -> Self::Result {
        let apis = self.apis.clone();
        Box::pin(
            async move {
                GET_COUNTER
                    .with_label_values(&[msg.exchange.as_ref(), &msg.asset])
                    .inc();
                let api = apis
                    .get_api(msg.exchange)
                    .ok_or(brokers::error::Error::ExchangeNotLoaded)?;
                get_interest_rate(&msg.exchange, api.clone().as_ref(), &msg.asset)
                    .await
                    .err_into()
            }
            .into_actor(self),
        )
    }
}

pub mod test_util {
    use std::path::Path;
    use std::sync::Arc;

    use actix::{Actor, Addr};
    use brokers::api::{ExchangeApi, MockExchangeApi};

    use brokers::exchange::Exchange;
    use brokers::manager::{ExchangeApiRegistry, ExchangeManager, ExchangeManagerRef};
    use brokers::Brokerages;

    use crate::interest::MarginInterestRateProviderClient;

    use super::MarginInterestRateProvider;

    /// # Panics
    ///
    /// if the exchange cannot be built
    pub async fn it_interest_rate_provider<S: AsRef<Path>, S2: AsRef<Path>>(
        keys_file: S2,
        exchange: Exchange,
    ) -> MarginInterestRateProvider {
        let api = Brokerages::new_manager()
            .build_exchange_api(keys_file.as_ref().to_path_buf(), &exchange, true)
            .await
            .unwrap();
        let apis = ExchangeApiRegistry::new();
        apis.insert(exchange, api);
        let manager = ExchangeManagerRef::new(ExchangeManager::new_with_reg(apis));
        MarginInterestRateProvider::new(manager)
    }

    pub fn new_mock_interest_rate_provider(exchanges: &[Exchange]) -> MarginInterestRateProvider {
        let api: Arc<dyn ExchangeApi> = Arc::new(MockExchangeApi::default());
        let apis = ExchangeApiRegistry::new();
        for exchange in exchanges {
            apis.insert(*exchange, api.clone());
        }
        let manager = ExchangeManagerRef::new(ExchangeManager::new_with_reg(apis));
        MarginInterestRateProvider::new(manager)
    }

    pub fn mock_interest_rate_provider(exchanges: &[Exchange]) -> Addr<MarginInterestRateProvider> {
        let provider = new_mock_interest_rate_provider(exchanges);
        let act = MarginInterestRateProvider::start(provider);
        loop {
            if act.connected() {
                break;
            }
        }
        act
    }

    pub fn mock_interest_rate_client(exchange: Exchange) -> MarginInterestRateProviderClient {
        let provider = mock_interest_rate_provider(&[exchange]);
        MarginInterestRateProviderClient::new(provider)
    }

    pub fn local_provider(api: Arc<dyn ExchangeApi>) -> Addr<MarginInterestRateProvider> {
        let apis = ExchangeApiRegistry::new();
        apis.insert(api.exchange(), api);
        let manager = ExchangeManagerRef::new(ExchangeManager::new_with_reg(apis));
        let provider = MarginInterestRateProvider::new(manager);
        MarginInterestRateProvider::start(provider)
    }
}

#[cfg(test)]
mod test {
    use brokers::exchange::Exchange;
    use brokers::types::{InterestRate, InterestRatePeriod};

    use crate::error::*;
    use crate::interest::test_util::local_provider;
    use crate::interest::GetInterestRate;
    use crate::order_manager::test_util::init;

    #[actix::test]
    #[cfg_attr(not(feature = "live_e2e_tests"), ignore)]
    async fn test_fetch_margin_interest_rate() -> Result<()> {
        init();
        let (_credentials, apis) = crate::test_util::e2e::build_apis().await?;
        let provider = local_provider(apis.get(&Exchange::Binance).unwrap().clone());
        let interest_rate_response = provider
            .send(GetInterestRate {
                exchange: Exchange::Binance,
                asset: "BTC".to_string(),
            })
            .await
            .unwrap();
        assert!(interest_rate_response.is_ok(), "{:?}", interest_rate_response);
        let interest_rate: InterestRate = interest_rate_response.unwrap();
        assert!(interest_rate.rate > 0.0);
        assert_eq!(interest_rate.symbol, "BTC");
        assert_eq!(interest_rate.period, InterestRatePeriod::Daily);
        Ok(())
    }
}
