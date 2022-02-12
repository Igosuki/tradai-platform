//! Use this module to interact with Poloniex through a Generic API.
//! This a more convenient and safe way to deal with the exchange since methods return a Result<>
//! but this generic API does not provide all the functionnality that Poloniex offers.

use broker_core::error::*;
use broker_core::json_util::*;
use broker_core::pair::PairConf;
use broker_core::prelude::*;
use broker_core::types::*;
use broker_core::util::*;

use super::api::PoloniexApi;
use super::utils;

#[async_trait]
impl ExchangeApi for PoloniexApi {
    async fn ticker(&self, pair: Pair) -> Result<Ticker> {
        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();
        let raw_response = self.return_ticker().await?;

        let result = utils::parse_result(&raw_response)?;
        println!("{:?}", result);
        let price = from_json_f64(&result[pair_name]["last"], "last")?;
        let ask = from_json_f64(&result[pair_name]["lowestAsk"], "lowestAsk")?;
        let bid = from_json_f64(&result[pair_name]["highestBid"], "highestBid")?;
        let vol = from_json_f64(&result[pair_name]["quoteVolume"], "quoteVolume")?;

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
        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();
        let raw_response = self.return_order_book(pair_name, "1000").await?; // 1000 entries max

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
            let volume_str = ask[1].to_string();
            let volume = volume_str.parse::<f64>().unwrap();

            ask_offers.push((price, volume));
        }

        for bid in bid_array {
            let price = from_json_f64(&bid[0], "bid price")?;
            let volume_str = bid[1].to_string();
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
        let submission = order.clone().into();
        let price = order.price;
        let quantity = order.quantity.unwrap();
        let pair = order.pair;

        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();

        // The trick is to use minimal (0.0) and "maximum" (999..) price to simulate market order
        let raw_response = match (order.order_type, order.side) {
            // Unwrap safe here with the check above.
            (OrderType::Limit, TradeType::Buy) => {
                if price.is_none() {
                    return Err(Error::MissingPrice);
                }

                self.buy(pair_name, &price.unwrap().to_string(), &quantity.to_string(), None)
                    .await
            }
            (OrderType::Market, TradeType::Buy) => {
                self.buy(pair_name, "9999999999999999999", &quantity.to_string(), None)
                    .await
            }
            (OrderType::Limit, TradeType::Sell) => {
                if price.is_none() {
                    return Err(Error::MissingPrice);
                }

                self.sell(pair_name, &price.unwrap().to_string(), &quantity.to_string(), None)
                    .await
            }
            (OrderType::Market, TradeType::Sell) => self.sell(pair_name, "0.0", &quantity.to_string(), None).await,
            _ => unimplemented!(),
        }?;

        let result = utils::parse_result(&raw_response)?;

        Ok(OrderSubmission {
            id: result["orderNumber"]
                .as_f64()
                .ok_or_else(|| Error::MissingField("orderNumber".to_string()))?
                .to_string(),
            ..submission
        })
    }

    async fn account_balances(&self) -> Result<AccountPosition> {
        let raw_response = self.return_balances().await?;
        let result = utils::parse_result(&raw_response)?;

        let mut balances = AccountPosition::new();

        for (key, val) in result.iter() {
            let currency = utils::get_currency_enum(key);

            if let Some(currency) = currency {
                let amount = from_json_f64(val, "amount")?;
                balances.insert(currency, Balance {
                    free: amount,
                    locked: 0.0,
                });
            }
        }
        Ok(balances)
    }

    async fn get_order(&self, _id: String, _pair: Pair, _asset_type: AssetType) -> Result<Order> { unimplemented!() }

    async fn pairs(&self) -> Result<Vec<PairConf>> {
        // Poloniex pairs are already separated by underscore and uppercase
        todo!()
    }

    fn exchange(&self) -> Exchange { Exchange::Poloniex }

    fn uses_account(&self) -> bool { !self.api_key.is_empty() && !self.api_secret.is_empty() }
}
