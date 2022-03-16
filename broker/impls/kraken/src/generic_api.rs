//! Use this module to interact with Kraken through a Generic API.
//! This a more convenient and safe way to deal with the exchange since methods return a Result<>
//! but this generic API does not provide all the functionnality that Kraken offers.

use broker_core::error::*;
use broker_core::json_util::from_json_f64;
use broker_core::pair::PairConf;
use broker_core::prelude::*;
use broker_core::types::*;
use broker_core::util::get_unix_timestamp_ms;

use super::api::KrakenApi;
use super::model::StandardOrder;
use super::utils;

#[async_trait]
impl Brokerage for KrakenApi {
    async fn ticker(&self, pair: Pair) -> Result<Ticker> {
        let symbol = utils::get_pair_string(&pair)?;
        let pair_name = symbol.as_ref();

        let raw_response = self.get_ticker_information(pair_name).await?;

        let result = utils::parse_result(&raw_response)?;
        let ticker = result
            .get(pair_name)
            .ok_or_else(|| Error::MissingField(pair_name.to_string()))?;
        Ok(Ticker {
            timestamp: get_unix_timestamp_ms(),
            pair,
            last_trade_price: ticker.price()?,
            lowest_ask: ticker.ask()?,
            highest_bid: ticker.bid()?,
            volume: Some(ticker.volume()?),
        })
    }

    async fn orderbook(&self, pair: Pair) -> Result<Orderbook> {
        let symbol = utils::get_pair_string(&pair)?;
        let pair_name = symbol.as_ref();

        let raw_response = self.get_order_book(pair_name, "1000").await?; // 1000 entries max

        let result = utils::parse_result(&raw_response)?;
        let orderbook = result
            .get(pair_name)
            .ok_or_else(|| Error::MissingField(pair_name.to_string()))?;

        Ok(Orderbook {
            timestamp: get_unix_timestamp_ms(),
            pair,
            asks: orderbook.asks()?,
            bids: orderbook.bids()?,
            last_order_id: None,
        })
    }

    async fn add_order(&self, order: AddOrderRequest) -> Result<OrderSubmission> {
        let submission = order.simulate_submission(0.001);
        let price = order.price;
        let quantity = order.quantity.unwrap();
        let pair = order.pair;

        let symbol = utils::get_pair_string(&pair)?;
        let pair_name = symbol.as_ref();

        let direction = match order.side {
            TradeType::Buy => "buy",
            TradeType::Sell => "sell",
        };

        let order_type_str = match order.order_type {
            OrderType::Limit => "limit",
            OrderType::Market => "market",
            _ => unimplemented!(),
        };

        let mut price_str = "".to_string();
        if let Some(price) = price {
            price_str = price.to_string();
        }

        let st = StandardOrder {
            type_order: direction,         // type : buy/sell
            ordertype: order_type_str,     // order type : market/limit/...
            price: &price_str,             // price 1
            price2: "",                    // price 2
            volume: &quantity.to_string(), // volume
            leverage: "",                  // leverage
            oflags: "",                    // oflags (see doc)
            starttm: "",                   // starttm
            expiretm: "",                  // expiretm
            userref: "",                   // userref
            validate: "",
        };

        let raw_response = self.add_standard_order(pair_name, st).await?;

        let result = utils::parse_result(&raw_response)?;

        Ok(OrderSubmission {
            id: result.txids.join(","),
            ..submission
        })
    }

    async fn account_balances(&self) -> Result<AccountPosition> {
        let raw_response = self.get_account_balance().await?;
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
        // Following code was used to load pairs :
        //         let xbt_re = Regex::new(r"(XXBT|XBT)+").unwrap();
        //         let re = Regex::new(r"X([A-Z]{3,4})").unwrap();
        //         let z_re = Regex::new(r"Z([A-Z]{3,4})").unwrap();
        //         for pair in &*ALL_KRAKEN_PAIRS {
        //             let no_xbt = xbt_re.replace_all(pair, "BTC");
        //             let no_xx = re.replace_all(&no_xbt, "$1");
        //             let no_z = z_re.replace_all(&no_xx, "$1");
        //             let sanitized = no_z.replace(".d", "_d");
        //             if sanitized.contains('_') {
        //                 m.insert(Pair::from(sanitized), pair);
        //             } else {
        //                 let mut rep = pair.to_string();
        //                 rep.insert(3, '_');
        //                 let rep = pair.replace('.', "_");
        //                 m.insert(Pair::from(rep), pair);
        //             }
        //         }
        todo!()
    }

    fn exchange(&self) -> Exchange { Exchange::Kraken }

    fn uses_account(&self) -> bool { !self.api_key.is_empty() && !self.api_secret.is_empty() }
}
