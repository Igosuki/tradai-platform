//! Use this module to interact with Bittrex through a Generic API.
//! This a more convenient and safe way to deal with the exchange since methods return a Result<>
//! but this generic API does not provide all the functionnality that Bittrex offers.

use async_trait::async_trait;
use broker_core::error::*;
use broker_core::pair::PairConf;
use broker_core::prelude::*;
use broker_core::types::*;

use super::api::BittrexApi;
use super::utils;

#[async_trait]
impl Brokerage for BittrexApi {
    async fn ticker(&self, pair: Pair) -> Result<Ticker> {
        let pair_name = utils::get_pair_string(&pair)?;

        let raw_response = self.get_market_summary(pair_name.as_ref()).await?;

        let result = utils::parse_result(&raw_response)?;
        let result_array = result.as_array();
        let result_obj = result_array.unwrap()[0].as_object().unwrap();

        let price_str = result_obj.get("Last").unwrap().to_string();
        let price = price_str.parse::<f64>().unwrap();

        let ask_str = result_obj.get("Ask").unwrap().to_string();
        let ask = ask_str.parse::<f64>().unwrap();

        let bid_str = result_obj.get("Bid").unwrap().to_string();
        let bid = bid_str.parse::<f64>().unwrap();

        let volume_str = result_obj.get("Volume").unwrap().to_string();
        let vol = volume_str.parse::<f64>().unwrap();

        Ok(Ticker {
            timestamp: get_unix_timestamp_ms(),
            pair,
            last_trade_price: price,
            lowest_ask: ask,
            highest_bid: bid,
            volume: Some(vol),
        })
    }

    async fn orderbook(&self, pair: Pair) -> Result<Orderbook> {
        let pair_name = utils::get_pair_string(&pair)?;

        let raw_response = self.get_order_book(pair_name.as_ref(), "both").await?;

        let result = utils::parse_result(&raw_response)?;

        let mut ask_offers = Vec::new(); // buy orders
        let mut bid_offers = Vec::new(); // sell orders

        let buy_orders = result["buy"].as_array().ok_or_else(|| Error::InvalidFieldFormat {
            value: format!("{}", result["buy"]),
            source: anyhow!("expected an array"),
        })?;

        let sell_orders = result["sell"].as_array().ok_or_else(|| Error::InvalidFieldFormat {
            value: format!("{}", result["sell"]),
            source: anyhow!("expected an array"),
        })?;

        for ask in buy_orders {
            let ask_obj = ask.as_object().unwrap();

            let price_str = ask_obj.get("Rate").unwrap().to_string();
            let price = price_str.parse::<f64>().unwrap();

            let volume_str = ask_obj.get("Quantity").unwrap().to_string();
            let volume = volume_str.parse::<f64>().unwrap();

            ask_offers.push((price, volume));
        }

        for bid in sell_orders {
            let bid_obj = bid.as_object().unwrap();

            let price_str = bid_obj.get("Rate").unwrap().to_string();
            let price = price_str.parse::<f64>().unwrap();

            let volume_str = bid_obj.get("Quantity").unwrap().to_string();
            let volume = volume_str.parse::<f64>().unwrap();

            bid_offers.push((price, volume));
        }

        Ok(Orderbook {
            timestamp: get_unix_timestamp_ms(),
            pair,
            asks: ask_offers,
            bids: bid_offers,
            last_order_id: None,
        })
    }

    async fn add_order(&self, order: AddOrderRequest) -> Result<OrderSubmission> {
        let submission = order.simulate_submission(0.001);
        let symbol = utils::get_pair_string(&order.pair)?;
        let pair_name = symbol.as_ref();

        let raw_response = match (order.order_type, order.side) {
            (OrderType::Limit, TradeType::Buy) => {
                if order.price.is_none() {
                    return Err(Error::MissingPrice);
                }
                self.buy_limit(
                    pair_name,
                    &order.quantity.unwrap().to_string(),
                    &order.price.unwrap().to_string(),
                )
                .await
            }
            (OrderType::Market, TradeType::Buy) => {
                let min_price = "0.000000001";
                self.buy_limit(pair_name, &order.quantity.unwrap().to_string(), min_price)
                    .await
            }
            (OrderType::Limit, TradeType::Sell) => {
                if order.price.is_none() {
                    return Err(Error::MissingPrice);
                }
                self.sell_limit(
                    pair_name,
                    &order.quantity.unwrap().to_string(),
                    &order.price.unwrap().to_string(),
                )
                .await
            }
            (OrderType::Market, TradeType::Sell) => {
                let max_price = "999999999.99";
                self.sell_limit(pair_name, &order.quantity.unwrap().to_string(), max_price)
                    .await
            }
            _ => unimplemented!(),
        }?;

        let result = utils::parse_result(&raw_response)?;

        let result_obj = result.as_object().unwrap();

        Ok(OrderSubmission {
            id: result_obj.get("uuid").unwrap().as_str().unwrap().to_string(),
            ..submission
        })
    }

    async fn account_balances(&self) -> Result<AccountPosition> {
        let raw_response = self.get_balances().await?;

        let result = utils::parse_result(&raw_response)?;

        let result_array = result.as_array().unwrap();

        let mut balances = AccountPosition::new();

        for currency in result_array {
            let currency_obj = currency.as_object().unwrap();
            let currency_str = currency_obj.get("Currency").unwrap().as_str().unwrap();
            let currency = utils::get_asset(currency_str);

            if let Some(c) = currency {
                let amount_str = currency_obj.get("Available").unwrap().to_string();
                let amount = amount_str.parse::<f64>().unwrap();
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
        // bittrex pairs are separated with '-' and uppercase
        todo!()
    }

    fn exchange(&self) -> Exchange { Exchange::Bittrex }

    fn uses_account(&self) -> bool { !self.api_key.is_empty() && !self.api_secret.is_empty() }
}
