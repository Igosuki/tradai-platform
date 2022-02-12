use std::fmt::Debug;

use crate::error::*;
use crate::exchange::Exchange;
use crate::pair::PairConf;
use crate::types::*;
pub use mock::*;

#[async_trait]
pub trait ExchangeApi: Debug + Send + Sync {
    /// Return a Ticker for the Pair specified.
    async fn ticker(&self, pair: Pair) -> Result<Ticker>;

    /// Return an Orderbook for the specified Pair.
    async fn orderbook(&self, pair: Pair) -> Result<Orderbook>;

    /// Place an order directly to the exchange.
    /// Quantity is in quote currency. So if you want to buy 1 Bitcoin for X€ (pair BTC_EUR),
    /// base currency (right member in the pair) is BTC and quote/counter currency is BTC (left
    /// member in the pair).
    /// So quantity = 1.
    ///
    /// A good practice is to store the return type (OrderInfo) somewhere since it can later be used
    /// to modify or cancel the order.
    #[allow(irrefutable_let_patterns)]
    async fn order(&self, order: OrderQuery) -> Result<OrderSubmission> {
        order.validate()?;
        if let OrderQuery::AddOrder(req) = order {
            return self.add_order(req).await;
        }
        unimplemented!()
    }

    async fn add_order(&self, order: AddOrderRequest) -> Result<OrderSubmission>;

    /// Retrieve the current amounts of all the currencies that the account holds
    /// The amounts returned are available (not used to open an order)
    async fn account_balances(&self) -> Result<AccountPosition>;

    /// Retrieve margin account details
    ///
    /// # Arguments
    ///
    /// * `asset`: if set, will return isolated margin account details, otherwise the cross-margin
    ///
    /// returns: Result<MarginAccountDetails, Error>
    async fn margin_account(&self, _pair: Option<String>) -> Result<MarginAccountDetails> {
        return Err(Error::ExchangeFeatureNotImplemented);
    }

    /// Retrieve the current amounts of all the currencies that the account holds
    /// The amounts returned are available (not used to open an order)
    async fn get_order(&self, id: String, pair: Pair, asset_type: AssetType) -> Result<Order>;

    /// Get all available market configurations
    async fn pairs(&self) -> Result<Vec<PairConf>>;

    /// Retrieve the exchange value for this API
    fn exchange(&self) -> Exchange;

    /// Whether or not the API is using account features
    fn uses_account(&self) -> bool;

    /// Get the current margin interest rate
    ///
    /// # Arguments
    ///
    /// * `symbol`: the asset for which to fetch the interest rate
    ///
    /// returns: Result<InterestRate, Error>
    async fn margin_interest_rate(&self, _symbol: Symbol) -> Result<InterestRate> {
        return Err(Error::ExchangeFeatureNotImplemented);
    }

    async fn trade_history(&self, _pair: Pair) -> Result<Vec<Trade>> {
        return Err(Error::ExchangeFeatureNotImplemented);
    }
}

mod mock {
    use crate::api::ExchangeApi;
    use crate::error::*;
    use crate::exchange::Exchange;
    use crate::pair::PairConf;
    use crate::types::*;
    use chrono::Utc;
    use uuid::Uuid;

    #[derive(Debug)]
    pub struct MockExchangeApi {
        flat_interest_rate: f64,
    }

    const DEFAULT_HOURLY_INTEREST_RATE: f64 = 0.02 / 24.0;

    impl Default for MockExchangeApi {
        fn default() -> Self {
            Self {
                flat_interest_rate: DEFAULT_HOURLY_INTEREST_RATE,
            }
        }
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    pub struct MockOrder {
        order_type: OrderType,
        pair: Pair,
        quantity: f64,
        price: Option<f64>,
    }

    #[async_trait]
    impl ExchangeApi for MockExchangeApi {
        async fn ticker(&self, _pair: Pair) -> Result<Ticker> { unimplemented!() }

        async fn orderbook(&self, _pair: Pair) -> Result<Orderbook> { unimplemented!() }

        async fn add_order(&self, o: AddOrderRequest) -> Result<OrderSubmission> {
            let submission = o.clone().into();
            let order = MockOrder {
                order_type: o.order_type,
                pair: o.pair.clone(),
                quantity: o.quantity.unwrap_or(0.0),
                price: o.price,
            };
            let fake_id = Uuid::new_v4();
            let info = OrderSubmission {
                timestamp: Utc::now().timestamp(),
                id: fake_id.to_string(),
                ..submission
            };
            trace!("order passed : {:?}, answer {:?}", &order, &info);
            Ok(info)
        }

        async fn account_balances(&self) -> Result<AccountPosition> { unimplemented!() }

        async fn get_order(&self, id: String, _pair: Pair, _asset_type: AssetType) -> Result<Order> {
            trace!("get order : {}", &id);
            let info = Order {
                xch: Exchange::default(),
                symbol: "".to_string().into(),
                order_id: "".to_string(),
                orig_order_id: id,
                price: 0.0,
                orig_qty: 0.0,
                executed_qty: 0.0,
                cumulative_quote_qty: 0.0,
                status: OrderStatus::New,
                enforcement: OrderEnforcement::GTC,
                order_type: OrderType::Limit,
                side: TradeType::default(),
                stop_price: 0.0,
                iceberg_qty: 0.0,
                orig_time: 0,
                last_event_time: 0,
                is_in_transaction: false,
                orig_quote_order_qty: 0.0,
                asset_type: AssetType::Spot,
            };
            Ok(info)
        }

        async fn pairs(&self) -> Result<Vec<PairConf>> { todo!() }

        fn exchange(&self) -> Exchange { Exchange::Binance }

        fn uses_account(&self) -> bool { false }

        #[allow(clippy::cast_sign_loss)]
        async fn margin_interest_rate(&self, symbol: Symbol) -> Result<InterestRate> {
            Ok(InterestRate {
                symbol: symbol.to_string(),
                rate: self.flat_interest_rate,
                ts: Utc::now().timestamp_millis() as u64,
                period: InterestRatePeriod::Hourly,
            })
        }
    }
}
