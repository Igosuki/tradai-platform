use std::sync::Arc;

use actix::Addr;

use brokers::manager::BrokerageManager;
#[cfg(any(
    test,
    feature = "test_util",
    feature = "live_e2e_tests",
    feature = "manual_e2e_tests"
))]
pub use mock::mock_engine;

use crate::interest::{InterestRateProvider, MarginInterestRateProvider, MarginInterestRateProviderClient};
use crate::order_manager::{OrderExecutor, OrderManager, OrderManagerClient};

#[derive(Debug, typed_builder::TypedBuilder)]
pub struct TradingEngine {
    pub order_executor: Arc<dyn OrderExecutor>,
    pub interest_rate_provider: Arc<dyn InterestRateProvider>,
    pub exchange_manager: Arc<BrokerageManager>,
}

pub fn new_trading_engine(
    manager: Arc<BrokerageManager>,
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

#[cfg(any(
    test,
    feature = "test_util",
    feature = "live_e2e_tests",
    feature = "manual_e2e_tests"
))]
mod mock {
    use std::path::Path;

    use brokers::exchange::Exchange;
    use brokers::manager::BrokerageManager;

    use crate::interest::test_util::mock_interest_rate_provider;
    use crate::interest::MarginInterestRateProviderClient;
    use crate::order_manager::test_util::mock_manager;
    use crate::order_manager::OrderManagerClient;

    use super::*;

    pub fn mock_engine<S: AsRef<Path>>(db_path: S, exchanges: &[Exchange]) -> TradingEngine {
        let manager = BrokerageManager::new();
        manager.build_mock_exchange_apis(exchanges);
        let mut om_db = db_path.as_ref().to_path_buf();
        om_db.push("om");
        let order_manager_addr = mock_manager(om_db);
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
}
