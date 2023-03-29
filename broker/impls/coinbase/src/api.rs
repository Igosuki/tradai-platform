//! Use this module to interact with Gdax exchange.
//! Please see examples for more informations.

use std::collections::HashMap;
use std::thread;
use std::time::Duration;

use futures::TryFutureExt;
use reqwest::header::{ACCEPT, USER_AGENT};
use reqwest::{Client, Method};
use serde_json::value::Map;
use serde_json::Value;

use broker_core::error::*;
use broker_core::prelude::*;
use broker_core::types::*;
use broker_core::url_util::strip_empties;

use super::utils;

#[allow(dead_code)]
#[derive(Debug, Clone)]
struct CoinbaseServerConfig<'a> {
    rest: &'a str,
    websocket: &'a str,
    fix: &'a str,
}

impl CoinbaseServerConfig<'_> {
    fn prod() -> Self {
        Self {
            fix: "tcp+ssl://fix.pro.coinbase.com:4198",
            rest: "https://api.pro.coinbase.com",
            websocket: "wss://ws-feed.pro.coinbase.com",
        }
    }
    fn test() -> Self {
        Self {
            fix: " tcp+ssl://fix-public.sandbox.pro.coinbase.com:4198",
            rest: " https://api-public.sandbox.pro.coinbase.com",
            websocket: " wss://ws-feed-public.sandbox.pro.coinbase.com",
        }
    }
}

#[derive(Debug, Clone)]
pub struct CoinbaseApi {
    last_request: i64,
    // unix timestamp in ms, to avoid ban
    pub(super) api_key: String,
    pub(super) api_secret: String,
    customer_id: String,
    client: Client,
    burst: bool,
    config: CoinbaseServerConfig<'static>,
}

impl CoinbaseApi {
    /// Create a new GdaxApi by providing an API key & API secret
    pub fn new(creds: &dyn Credentials) -> Result<CoinbaseApi> { Self::new_with_options(creds, false) }

    pub fn new_with_options(creds: &dyn Credentials, use_test: bool) -> Result<CoinbaseApi> {
        if creds.exchange() != Exchange::Coinbase {
            return Err(Error::InvalidConfigType {
                expected: Exchange::Coinbase,
                find: creds.exchange(),
            });
        }

        Ok(CoinbaseApi {
            last_request: 0,
            api_key: creds.get("api_key").unwrap_or_default(),
            api_secret: creds.get("api_secret").unwrap_or_default(),
            customer_id: creds.get("customer_id").unwrap_or_default(),
            client: reqwest::Client::new(),
            burst: false, // No burst by default
            config: if use_test {
                CoinbaseServerConfig::test()
            } else {
                CoinbaseServerConfig::prod()
            },
        })
    }

    /// The number of calls in a given period is limited. In order to avoid a ban we limit
    /// by default the number of api requests.
    /// This function sets or removes the limitation.
    /// Burst false implies no block.
    /// Burst true implies there is a control over the number of calls allowed to the exchange
    pub fn set_burst(&mut self, burst: bool) { self.burst = burst }

    pub fn build_url(&self, method: &str, pair: &str) -> String {
        match method {
            "ticker" => format!("{}/products/{}/ticker", self.config.rest, pair),
            "order_book" => format!("{}/products/{}/book", self.config.rest, pair),
            "transactions" => format!("{}/accounts/{}/ledger", self.config.rest, pair),
            _ => "not implemented yet".to_string(),
        }
    }

    #[allow(clippy::cast_sign_loss)]
    fn block_or_continue(&self) {
        if !self.burst {
            let threshold: u64 = 334; // 3 requests/sec = 1/3*1000
            let offset: u64 = get_unix_timestamp_ms() as u64 - self.last_request as u64;
            if offset < threshold {
                let wait_ms = Duration::from_millis(threshold - offset);
                thread::sleep(wait_ms);
            }
        }
    }

    async fn public_query(&self, params: &HashMap<&str, &str>) -> Result<Map<String, Value>> {
        let method: &str = params
            .get("method")
            .ok_or_else(|| Error::MissingField("method".to_string()))?;
        let pair: &str = params
            .get("pair")
            .ok_or_else(|| Error::MissingField("pair".to_string()))?;
        let url = self.build_url(method, pair);

        self.block_or_continue();
        let req = self
            .client
            .request(Method::GET, url)
            .header(USER_AGENT, "tradai_broker")
            .header(ACCEPT, "application/json")
            .build()?;
        let resp = self.client.execute(req).await?;
        resp.json().err_into().await
    }

    ///
    ///
    /// #Examples
    ///
    /// ```rust
    /// extern crate broker_coinbase;
    /// use broker_coinbase::CoinbaseApi;
    /// let mut api = GdaxApi::new("", "");
    /// let  result = api.private_query("balance", "btcusd");
    /// assert_eq!(true, true);
    /// ```
    async fn private_query(&self, params: &HashMap<&str, &str>) -> Result<Map<String, Value>> {
        let method: &str = params
            .get("method")
            .ok_or_else(|| Error::MissingField("method".to_string()))?;
        let pair: &str = params
            .get("pair")
            .ok_or_else(|| Error::MissingField("pair".to_string()))?;
        let url = self.build_url(method, pair);

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
    pub async fn return_ticker(&self, pair: Pair) -> Result<Map<String, Value>> {
        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();

        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("pair", pair_name);
        params.insert("method", "ticker");
        self.public_query(&params).await
    }

    /// Sample output :
    ///
    /// ```json
    /// {"asks":[[0.00007600,1164],[0.00007620,1300], ... ], "bids":[[0.00006901,200],
    /// [0.00006900,408], ... ], "timestamp": "1234567890"}
    /// ```
    pub async fn return_order_book(&self, pair: Pair) -> Result<Map<String, Value>> {
        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();
        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("method", "order_book");
        params.insert("pair", pair_name);
        self.public_query(&params).await
    }

    /// Sample output :
    ///
    /// ```json
    /// [{"date":"2014-02-10 04:23:23","type":"buy","rate":"0.00007600","amount":"140",
    /// "total":"0.01064"},
    /// {"date":"2014-02-10 01:19:37","type":"buy","rate":"0.00007600","amount":"655",
    /// "total":"0.04978"}, ... ]
    /// ```
    pub async fn return_trade_history(&self, pair: Pair) -> Result<Map<String, Value>> {
        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();
        let mut params: HashMap<&str, &str> = HashMap::new();
        params.insert("pair", pair_name);
        params.insert("method", "transactions");
        self.public_query(&params).await
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
        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();
        let amount_string = amount.to_string();
        let price_string = price.to_string();
        let price_limit_string = match price_limit {
            Some(limit) => limit.to_string(),
            None => "".to_string(),
        };

        let mut params = HashMap::new();
        params.insert("method", "buy");
        params.insert("pair", pair_name);

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
        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();

        let amount_string = amount.to_string();
        let price_string = price.to_string();
        let price_limit_string = match price_limit {
            Some(limit) => limit.to_string(),
            None => "".to_string(),
        };

        let mut params = HashMap::new();
        params.insert("method", "sell");
        params.insert("pair", pair_name);

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
        let symbol = utils::get_symbol(&pair)?;
        let amount_string = amount.to_string();

        let mut params = HashMap::new();
        params.insert("method", "buy/market");
        params.insert("pair", symbol.as_ref());

        params.insert("amount", &amount_string);

        self.private_query(&params).await
    }

    /// Add a market sell order to the exchange
    /// By placing a market order you acknowledge that the execution of your order depends
    /// on the market conditions and that these conditions may be subject to sudden changes
    /// that cannot be foreseen.
    pub async fn sell_market(&self, pair: Pair, amount: Volume) -> Result<Map<String, Value>> {
        let symbol = utils::get_symbol(&pair)?;
        let pair_name = symbol.as_ref();

        let amount_string = amount.to_string();

        let mut params = HashMap::new();
        params.insert("method", "sell/market");
        params.insert("pair", pair_name);

        params.insert("amount", &amount_string);

        self.private_query(&params).await
    }
}

#[cfg(test)]
mod coinbase_api_tests {
    //    #[test]
    //    fn should_block_or_not_block_when_enabled_or_disabled() {
    //        let mut api = GdaxApi {
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
    //            assert!(difference >= 334);
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
