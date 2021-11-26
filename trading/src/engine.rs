use crate::interest::test_util::mock_interest_rate_provider;
use crate::interest::{InterestRateProvider, MarginInterestRateProvider, MarginInterestRateProviderClient};
use crate::order_manager::test_util::mock_manager;
use crate::order_manager::{OrderExecutor, OrderManager, OrderManagerClient};
use actix::Addr;
use coinnect_rt::exchange::manager::ExchangeManager;
use coinnect_rt::exchange::Exchange;
use std::path::Path;
use std::sync::Arc;

#[derive(Debug, typed_builder::TypedBuilder)]
pub struct TradingEngine {
    pub order_executor: Arc<dyn OrderExecutor>,
    pub interest_rate_provider: Arc<dyn InterestRateProvider>,
    pub exchange_manager: Arc<ExchangeManager>,
}

pub fn new_trading_engine(
    manager: Arc<ExchangeManager>,
    om: Addr<OrderManager>,
    mirp: Addr<MarginInterestRateProvider>,
) -> TradingEngine {
    let executor = Arc::new(OrderManagerClient::new(om));
    let interest_rate_provider = Arc::new(MarginInterestRateProviderClient::new(mirp));
    TradingEngine {
        order_executor: executor,
        interest_rate_provider,
        exchange_manager: manager,
    }
}

pub fn mock_engine<S: AsRef<Path>>(db_path: S, exchanges: &[Exchange]) -> TradingEngine {
    let manager = ExchangeManager::new();
    manager.build_mock_exchange_apis(exchanges);
    let order_manager_addr = mock_manager(db_path);
    let executor = Arc::new(OrderManagerClient::new(order_manager_addr));
    let margin_interest_rate_provider_addr = mock_interest_rate_provider(exchanges);
    let interest_rate_provider = Arc::new(MarginInterestRateProviderClient::new(
        margin_interest_rate_provider_addr,
    ));
    TradingEngine {
        order_executor: executor,
        interest_rate_provider,
        exchange_manager: Arc::new(manager),
    }
}
