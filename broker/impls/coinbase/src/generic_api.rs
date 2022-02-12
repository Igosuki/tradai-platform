//! Use this module to interact with Coinbase through a Generic API.
//! This a more convenient and safe way to deal with the exchange since methods return a Result<>
//! but this generic API does not provide all the functionnality that Gdax offers.

use async_trait::async_trait;

use broker_core::error::*;
use broker_core::json_util::from_json_f64;
use broker_core::pair::PairConf;
use broker_core::prelude::*;
use broker_core::types::*;
use broker_core::util::get_unix_timestamp_ms;

use super::api::CoinbaseApi;
use super::utils;

#[async_trait]
impl Brokerage for CoinbaseApi {
    async fn ticker(&self, pair: Pair) -> Result<Ticker> {
        let result = self.return_ticker(pair.clone()).await?;

        let price = from_json_f64(&result["price"], "price")?;
        let ask = from_json_f64(&result["ask"], "ask")?;
        let bid = from_json_f64(&result["bid"], "bid")?;
        let vol = from_json_f64(&result["volume"], "volume")?;

        Ok(Ticker {
            timestamp: get_unix_timestamp_ms(),
            pair: pair.clone(),
            last_trade_price: price,
            lowest_ask: ask,
            highest_bid: bid,
            volume: Some(vol),
        })
    }

    async fn orderbook(&self, pair: Pair) -> Result<Orderbook> {
        let raw_response = self.return_order_book(pair.clone()).await?;

        let result = utils::parse_result(&raw_response)?;

        let mut ask_offers = Vec::new();
        let mut bid_offers = Vec::new();

        let ask_array = result["asks"].as_array().ok_or_else(|| Error::InvalidFieldFormat {
            value: format!("{}", result["asks"]),
            source: anyhow!("expected an array"),
        })?;
        let bid_array = result["bids"].as_array().ok_or_else(|| Error::InvalidFieldFormat {
            value: format!("{}", result["asks"]),
            source: anyhow!("expected an array"),
        })?;

        for ask in ask_array {
            let price = from_json_f64(&ask[0], "ask price")?;
            let volume = from_json_f64(&ask[1], "ask volume")?;

            ask_offers.push((price, volume));
        }

        for bid in bid_array {
            let price = from_json_f64(&bid[0], "bid price")?;
            let volume = from_json_f64(&bid[1], "bid volume")?;

            bid_offers.push((price, volume));
        }

        Ok(Orderbook {
            timestamp: get_unix_timestamp_ms(),
            pair: pair.clone(),
            asks: ask_offers,
            bids: bid_offers,
            last_order_id: None,
        })
    }

    async fn add_order(&self, order: AddOrderRequest) -> Result<OrderSubmission> {
        let submission = order.clone().into();
        let price = order.price;
        let quantity = order.quantity.unwrap();
        let pair = order.pair;
        let result = match (order.order_type, order.side) {
            (OrderType::Limit, TradeType::Buy) => {
                if order.price.is_none() {
                    return Err(Error::MissingPrice);
                }

                // Unwrap safe here with the check above.
                self.buy_limit(pair, order.quantity.unwrap(), price.unwrap(), None, None)
                    .await
            }
            (OrderType::Market, TradeType::Buy) => self.buy_market(pair, order.quantity.unwrap()).await,
            (OrderType::Limit, TradeType::Sell) => {
                if price.is_none() {
                    return Err(Error::MissingPrice);
                }

                // Unwrap safe here with the check above.
                self.sell_limit(pair, order.quantity.unwrap(), price.unwrap(), None, None)
                    .await
            }
            (OrderType::Market, TradeType::Sell) => self.sell_market(pair, quantity).await,
            _ => unimplemented!(),
        };

        Ok(OrderSubmission {
            id: result?["id"]
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
            let currency = utils::get_currency_enum(key);

            if let Some(c) = currency {
                let amount = from_json_f64(val, "amount")?;

                balances.insert(c, Balance {
                    free: amount,
                    locked: 0.0,
                });
            }
        }

        Ok(balances)
    }

    async fn get_order(&self, _id: String, _pair: Pair, _asset_type: AssetType) -> Result<Order> { unimplemented!() }

    async fn pairs(&self) -> Result<Vec<PairConf>> {
        // coinbase pairs are separated by '-' and lower case
        todo!()
    }

    fn exchange(&self) -> Exchange { Exchange::Coinbase }

    fn uses_account(&self) -> bool { !self.api_key.is_empty() && !self.api_secret.is_empty() }
}
