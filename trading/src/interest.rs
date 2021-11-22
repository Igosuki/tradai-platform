use crate::error::{Error, Result};
use crate::order_manager::types::OrderDetail;
use actix::{Actor, Addr, Context, Handler, ResponseActFuture, WrapFuture};
use coinnect_rt::exchange::margin_interest_rates::get_interest_rate;
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::InterestRate;
use ext::ResultExt;
use prometheus::CounterVec;
use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::Arc;

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
    async fn interest_fees_since(&self, exchange: Exchange, order: &OrderDetail) -> Result<f64>;
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
            .send(GetInterestRate { asset, exchange })
            .await
            .map_err(|_| Error::InterestRateProviderMailboxError)?
            .err_into()
    }

    async fn interest_fees_since(&self, exchange: Exchange, order: &OrderDetail) -> Result<f64> {
        let i = if order.asset_type.is_margin() && order.borrowed_amount.is_some() {
            let interest_rate = self.get_interest_rate(exchange, order.base_asset.clone()).await?;
            order.total_interest(interest_rate)
        } else {
            0.0
        };
        Ok(i)
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
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
}

impl MarginInterestRateProvider {
    pub fn new(apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>) -> Self { Self { apis: apis.clone() } }
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
                    .with_label_values(&[&msg.exchange.to_string(), &msg.asset])
                    .inc();
                let api = apis
                    .get(&msg.exchange)
                    .ok_or(coinnect_rt::error::Error::ExchangeNotLoaded)?;
                get_interest_rate(&msg.exchange, api.clone().as_ref(), &msg.asset)
                    .await
                    .err_into()
            }
            .into_actor(self),
        )
    }
}

pub mod test_util {
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::Arc;

    use crate::interest::MarginInterestRateProviderClient;
    use actix::{Actor, Addr};
    use coinnect_rt::coinnect::Coinnect;
    use coinnect_rt::exchange::{Exchange, ExchangeApi, MockExchangeApi};

    use super::MarginInterestRateProvider;

    pub async fn it_interest_rate_provider<S: AsRef<Path>, S2: AsRef<Path>>(
        keys_file: S2,
        exchange: Exchange,
    ) -> MarginInterestRateProvider {
        let api = Coinnect::new_manager()
            .build_exchange_api(keys_file.as_ref().to_path_buf(), &exchange, true)
            .await
            .unwrap();
        let mut apis = HashMap::new();
        apis.insert(exchange, api);
        let apis = Arc::new(apis);
        MarginInterestRateProvider::new(apis)
    }

    pub fn new_mock_interest_rate_provider(exchange: Exchange) -> MarginInterestRateProvider {
        let api: Arc<dyn ExchangeApi> = Arc::new(MockExchangeApi::default());
        let mut apis = HashMap::new();
        apis.insert(exchange, api);
        let apis = Arc::new(apis);
        MarginInterestRateProvider::new(apis)
    }

    pub fn mock_interest_rate_provider(exchange: Exchange) -> Addr<MarginInterestRateProvider> {
        let provider = new_mock_interest_rate_provider(exchange);
        let act = MarginInterestRateProvider::start(provider);
        loop {
            if act.connected() {
                break;
            }
        }
        act
    }

    pub fn mock_interest_rate_client(exchange: Exchange) -> MarginInterestRateProviderClient {
        let provider = mock_interest_rate_provider(exchange);
        MarginInterestRateProviderClient::new(provider)
    }

    pub fn local_provider(api: Arc<dyn ExchangeApi>) -> Addr<MarginInterestRateProvider> {
        let mut apis = HashMap::new();
        apis.insert(api.exchange(), api);
        let apis = Arc::new(apis);
        let provider = MarginInterestRateProvider::new(apis);
        MarginInterestRateProvider::start(provider)
    }
}

#[cfg(test)]
mod test {
    use crate::error::*;
    use crate::interest::test_util::local_provider;
    use crate::interest::GetInterestRate;
    use crate::order_manager::test_util::init;
    use coinnect_rt::exchange::Exchange;
    use coinnect_rt::types::{InterestRate, InterestRatePeriod};

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
