//! Use this module to interact with Bitstamp through a Generic API.
//! This a more convenient and safe way to deal with the exchange since methods return a Result<>
//! but this generic API does not provide all the functionnality that Bitstamp offers.

use async_trait::async_trait;

use super::api::BitstampApi;
use super::utils;
use broker_core::error::*;
use broker_core::json_util::from_json_f64;
use broker_core::pair::PairConf;
use broker_core::prelude::*;
use broker_core::types::*;

#[async_trait]
impl Brokerage for BitstampApi {
    async fn ticker(&self, pair: Pair) -> Result<Ticker> {
        let ticker = self.return_ticker(pair.clone()).await?;
        Ok(Ticker {
            pair: pair.clone(),
            ..ticker.into()
        })
    }

    async fn orderbook(&self, pair: Pair) -> Result<Orderbook> {
        let book = self.return_order_book(pair.clone()).await?;

        Ok(Orderbook {
            pair: pair.clone(),
            ..book.into()
        })
    }

    async fn add_order(&self, order: AddOrderRequest) -> Result<OrderSubmission> {
        let submission = order.clone().into();
        let result = match (order.order_type, order.side) {
            (OrderType::Limit, TradeType::Buy) => {
                if order.price.is_none() {
                    return Err(Error::MissingPrice);
                }
                // Unwrap safe here with the check above.
                self.buy_limit(order.pair, order.quantity.unwrap(), order.price.unwrap(), None, None)
                    .await
            }
            (OrderType::Market, TradeType::Buy) => self.buy_market(order.pair, order.quantity.unwrap()).await,
            (OrderType::Limit, TradeType::Sell) => {
                if order.price.is_none() {
                    return Err(Error::MissingPrice);
                }

                // Unwrap safe here with the check above.
                self.sell_limit(order.pair, order.quantity.unwrap(), order.price.unwrap(), None, None)
                    .await
            }
            (OrderType::Market, TradeType::Sell) => self.sell_market(order.pair, order.quantity.unwrap()).await,
            _ => unimplemented!(),
        }?;
        Ok(OrderSubmission {
            id: result["id"]
                .as_str()
                .ok_or_else(|| Error::MissingField("id".to_string()))?
                .to_string(),
            ..submission
        })
    }

    /// Return the balances for each currency on the account
    async fn account_balances(&self) -> Result<AccountPosition> {
        let raw_response = self.return_balances().await?;
        let result = utils::parse_result(&raw_response)?;

        let mut balances = AccountPosition::new();

        for (key, val) in result.iter() {
            let currency = utils::get_currency(key);

            let amount = from_json_f64(val, "amount")?;

            balances.insert(currency, Balance {
                locked: 0.0,
                free: amount,
            });
        }

        Ok(balances)
    }

    async fn get_order(&self, _id: String, _pair: Pair, _asset_type: AssetType) -> Result<Order> { unimplemented!() }

    async fn pairs(&self) -> Result<Vec<PairConf>> {
        // rep.insert(3, '_');
        // bitstamp pairs are lowercase and concatenated e.g.: 'btcusd'
        todo!()
    }

    fn exchange(&self) -> Exchange { Exchange::Bitstamp }

    fn uses_account(&self) -> bool { !self.api_key.is_empty() && !self.api_secret.is_empty() }

    async fn trade_history(&self, pair: Pair) -> Result<Vec<Trade>> {
        let r = self.return_trade_history(pair.clone()).await?;
        Ok(r.iter()
            .map(|value| Trade {
                event_ms: value.date.parse::<i64>().unwrap() * 1000,
                pair: pair.clone(),
                amount: value.amount.parse::<f64>().unwrap(),
                price: value.price.parse::<f64>().unwrap(),
                tt: if &value.type_r == "1" {
                    TradeType::Buy
                } else {
                    TradeType::Sell
                },
            })
            .collect())
    }
}
