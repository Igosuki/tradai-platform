//! Use this module to interact with Bitstamp exchange.
//! Please see examples for more informations.

use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use awc::http::StatusCode;
use futures::TryFutureExt;
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde_json::value::Map;
use serde_json::Value;

use broker_core::error::*;
use broker_core::prelude::*;
use broker_core::types::*;
use broker_core::url_util::strip_empties;

use super::models::{LiveOrderBook, PublicQuery, Ticker as BitstampTicker, Transaction};
use super::utils;

#[derive(Debug, Clone)]
pub struct BitstampApi {
    last_request: i64,
    // unix timestamp in ms, to avoid ban
    pub(super) api_key: String,
    pub(super) api_secret: String,
    customer_id: String,
    client: Client,
    burst: bool,
}

impl BitstampApi {
    /// Create a new BitstampApi by providing an API key & API secret
    pub fn new(creds: &dyn Credentials) -> Result<BitstampApi> {
        if creds.exchange() != Exchange::Bitstamp {
            return Err(Error::InvalidConfigType {
                expected: Exchange::Bitstamp,
                find: creds.exchange(),
            });
        }

        Ok(BitstampApi {
            last_request: 0,
            api_key: creds.get("api_key").unwrap_or_default(),
            api_secret: creds.get("api_secret").unwrap_or_default(),
            customer_id: creds.get("customer_id").unwrap_or_default(),
            client: reqwest::Client::new(),
            burst: false, // No burst by default
        })
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
    fn block_or_continue(&self) {
        if !self.burst {
            let threshold: u64 = 1000; // 600 requests per 10 mins = 1 request per second
            let offset: u64 = get_unix_timestamp_ms() as u64 - self.last_request as u64;
            if offset < threshold {
                let wait_ms = Duration::from_millis(threshold - offset);
                thread::sleep(wait_ms);
            }
        }
    }

    async fn public_query<T: DeserializeOwned>(&self, params: &PublicQuery) -> Result<T> {
        let url = utils::build_url(params.method, params.pair.as_ref());
        self.block_or_continue();
        let buf = self.client.get(url).send().await?;
        buf.json().err_into().await
    }

    ///
    ///
    /// #Examples
    ///
    /// ```json
    /// extern crate broker_bitstamp;
    /// use broker_bitstamp::api::BitstampApi;
    /// let mut api = BitstampApi::new("", "");
    /// let  result = api.private_query("balance", "btcusd");
    /// assert_eq!(true, true);
    /// ```
    async fn private_query(&self, params: &HashMap<&str, &str>) -> Result<Map<String, Value>> {
        let method: &str = params.get("method").ok_or(Error::Msg("Missing \"method\" field."))?;
        let pair: &str = params.get("pair").ok_or(Error::Msg("Missing \"pair\" field."))?;
        let url: String = utils::build_url(method, pair);

        let nonce = utils::generate_nonce(None);
        let signature = utils::build_signature(&nonce, &self.customer_id, &self.api_key, &self.api_secret)?;

        let copy_api_key = self.api_key.clone();
        let post_params: &mut HashMap<&str, &str> = &mut HashMap::new();
        post_params.insert("key", &copy_api_key);
        post_params.insert("signature", &signature);
        post_params.insert("nonce", &nonce);

        // copy params into post_params .... bit of a hack but will do for now
        for (k, v) in params.iter() {
            post_params.insert(k, v);
        }

        strip_empties(post_params);

        let req = self.client.request(Method::POST, url).form(post_params).build()?;
        let resp = self.client.execute(req).await?;
        let code = resp.status();
        if code.is_client_error() && code == StatusCode::FORBIDDEN {
            return Err(Error::BadCredentials);
        }
        resp.json().err_into().await
    }

    /// Sample output :
    ///
    /// ```json
    /// {
    /// "BTC_LTC":{
    /// "last":"0.0251","lowestAsk":"0.02589999","highestBid":"0.0251",
    /// "percentChange":"0.02390438","baseVolume":"6.16485315","quoteVolume":"245.82513926"},
    /// "BTC_NXT":{
    /// "last":"0.00005730","lowestAsk":"0.00005710","highestBid":"0.00004903",
    /// "percentChange":"0.16701570","baseVolume":"0.45347489","quoteVolume":"9094"},
    /// ... }
    /// ```
    pub async fn return_ticker(&self, pair: Pair) -> Result<BitstampTicker> {
        let symbol = utils::get_pair_string(&pair)?;

        self.public_query(&PublicQuery {
            pair: symbol,
            method: "ticker",
        })
        .await
    }

    /// Sample output :
    ///
    /// ```json
    /// {"asks":[[0.00007600,1164],[0.00007620,1300], ... ], "bids":[[0.00006901,200],
    /// [0.00006900,408], ... ], "timestamp": "1234567890"}
    /// ```
    pub async fn return_order_book(&self, pair: Pair) -> Result<LiveOrderBook> {
        let symbol = utils::get_pair_string(&pair)?;

        self.public_query(&PublicQuery {
            method: "order_book",
            pair: symbol,
        })
        .await
    }

    /// Sample output :
    ///
    /// ```json
    /// [{"date":"2014-02-10 04:23:23","type":"buy","rate":"0.00007600","amount":"140",
    /// "total":"0.01064"},
    /// {"date":"2014-02-10 01:19:37","type":"buy","rate":"0.00007600","amount":"655",
    /// "total":"0.04978"}, ... ]
    /// ```
    pub(crate) async fn return_trade_history(&self, pair: Pair) -> Result<Vec<Transaction>> {
        let symbol = utils::get_pair_string(&pair)?;

        self.public_query(&PublicQuery {
            pair: symbol,
            method: "transactions",
        })
        .await
    }

    /// Returns all of your available balances.
    ///
    /// Sample output:
    ///
    /// ```json
    /// {"BTC":"0.59098578","LTC":"3.31117268", ... }
    /// ```
    pub async fn return_balances(&self) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("method", "balance");
        params.insert("pair", "");
        self.private_query(&params).await
    }

    /// Add a buy limit order to the exchange
    /// limit_price: If the order gets executed, a new sell order will be placed,
    /// with "limit_price" as its price.
    /// daily_order (Optional) : Opens buy limit order which will be canceled
    /// at 0:00 UTC unless it already has been executed. Possible value: True
    pub async fn buy_limit(
        &self,
        pair: Pair,
        amount: Volume,
        price: Price,
        price_limit: Option<Price>,
        daily_order: Option<bool>,
    ) -> Result<Map<String, Value>> {
        let pair_name = utils::get_pair_string(&pair)?;

        let amount_string = amount.to_string();
        let price_string = price.to_string();
        let price_limit_string = match price_limit {
            Some(limit) => limit.to_string(),
            None => "".to_string(),
        };

        let mut params = HashMap::new();
        params.insert("method", "buy");
        params.insert("pair", pair_name.as_ref());

        params.insert("amount", &amount_string);
        params.insert("price", &price_string);
        params.insert("limit_price", &price_limit_string);
        if let Some(order) = daily_order {
            let daily_order_str = if order { "True" } else { "" }; // False is not a possible value
            params.insert("daily_order", daily_order_str);
        }

        self.private_query(&params).await
    }

    /// Add a sell limit order to the exchange
    /// limit_price: If the order gets executed, a new sell order will be placed,
    /// with "limit_price" as its price.
    /// daily_order (Optional) : Opens sell limit order which will be canceled
    /// at 0:00 UTC unless it already has been executed. Possible value: True
    pub async fn sell_limit(
        &self,
        pair: Pair,
        amount: Volume,
        price: Price,
        price_limit: Option<Price>,
        daily_order: Option<bool>,
    ) -> Result<Map<String, Value>> {
        let pair_name = utils::get_pair_string(&pair)?;

        let amount_string = format!("{:.8}", amount);

        let price_string = format!("{:.10}", price);
        let price_limit_string = match price_limit {
            Some(limit) => limit.to_string(),
            None => "".to_string(),
        };

        let mut params = HashMap::new();
        params.insert("method", "sell");
        params.insert("pair", pair_name.as_ref());

        params.insert("amount", &amount_string);
        params.insert("price", &price_string);
        params.insert("limit_price", &price_limit_string);
        if let Some(order) = daily_order {
            let daily_order_str = if order { "True" } else { "" }; // False is not a possible value
            params.insert("daily_order", daily_order_str);
        }

        self.private_query(&params).await
    }

    /// Add a market buy order to the exchange
    /// By placing a market order you acknowledge that the execution of your order depends
    /// on the market conditions and that these conditions may be subject to sudden changes
    /// that cannot be foreseen.
    pub async fn buy_market(&self, pair: Pair, amount: Volume) -> Result<Map<String, Value>> {
        let pair_name = utils::get_pair_string(&pair)?;

        let amount_string = amount.to_string();

        let mut params = HashMap::new();
        params.insert("method", "buy/market");
        params.insert("pair", pair_name.as_ref());

        params.insert("amount", &amount_string);

        self.private_query(&params).await
    }

    /// Add a market sell order to the exchange
    /// By placing a market order you acknowledge that the execution of your order depends
    /// on the market conditions and that these conditions may be subject to sudden changes
    /// that cannot be foreseen.
    pub async fn sell_market(&self, pair: Pair, amount: Volume) -> Result<Map<String, Value>> {
        let pair_name = utils::get_pair_string(&pair)?;

        let amount_string = amount.to_string();

        let mut params = HashMap::new();
        params.insert("method", "sell/market");
        params.insert("pair", pair_name.as_ref());

        params.insert("amount", &amount_string);

        self.private_query(&params).await
    }
}

#[cfg(test)]
mod bitstamp_api_tests {
    //    #[test]
    //    fn should_block_or_not_block_when_enabled_or_disabled() {
    //        let mut api = BitstampApi {
    //            last_request: get_unix_timestamp_ms(),
    //            api_key: "".to_string(),
    //            api_secret: "".to_string(),
    //            customer_id: "".to_string(),
    //            http_client: Client::new(),
    //            burst: false,
    //        };
    //
    //        let mut counter = 0;
    //        loop {
    //            api.set_burst(false);
    //            let start = get_unix_timestamp_ms();
    //            api.block_or_continue();
    //            api.last_request = get_unix_timestamp_ms();
    //
    //            let difference = api.last_request - start;
    //            assert!(difference >= 999);
    //            assert!(difference < 10000);
    //
    //
    //            api.set_burst(true);
    //            let start = get_unix_timestamp_ms();
    //            api.block_or_continue();
    //            api.last_request = get_unix_timestamp_ms();
    //
    //            let difference = api.last_request - start;
    //            assert!(difference < 10);
    //
    //            counter = counter + 1;
    //            if counter >= 3 { break; }
    //        }
    //    }
}
