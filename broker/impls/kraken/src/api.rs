//! Use this module to interact with the raw-original API provided by Kraken.
//! WARNING: Special attention should be paid to error management: parsing number, etc.

use data_encoding::BASE64;
use std::collections::HashMap;
use std::str;
use std::thread;
use std::time::Duration;

use futures::TryFutureExt;
use hmac::digest::Digest;
use hmac::{Hmac, Mac};
use reqwest::{Client, Method};
use serde::de::DeserializeOwned;
use serde_json::value::Map;
use serde_json::Value;
use sha2::{Sha256, Sha512};

use broker_core::error::*;
use broker_core::prelude::*;
use broker_core::url_util::{strip_empties, url_encode_hashmap};

use super::model::{OrderResult, Orderbooks, StandardOrder, TickerInfo};
use super::utils::KrakenResponse;

const KEY_HEADER: &str = "API-Key";
const SIGN_HEADER: &str = "API-Sign";

#[derive(Debug, Clone)]
pub struct KrakenApi {
    last_request: i64,
    // unix timestamp in ms, to avoid ban
    pub(super) api_key: String,
    pub(super) api_secret: String,
    otp: Option<String>,
    // two-factor password (if two-factor enabled, otherwise not required)
    client: Client,
    burst: bool,
}

impl KrakenApi {
    /// Create a new KrakenApi by providing an API key & API secret
    pub fn new(creds: &dyn Credentials) -> Result<KrakenApi> {
        if creds.exchange() != Exchange::Kraken {
            return Err(Error::InvalidConfigType {
                expected: Exchange::Kraken,
                find: creds.exchange(),
            });
        }

        Ok(KrakenApi {
            last_request: 0,
            api_key: creds.get("api_key").unwrap_or_default(),
            api_secret: creds.get("api_secret").unwrap_or_default(),
            otp: None,
            client: reqwest::Client::new(),
            burst: false,
        })
    }

    /// Use to provide your two-factor password (if two-factor enabled, otherwise not required)
    pub async fn set_two_pass_auth(&mut self, otp: String) { self.otp = Some(otp); }

    /// The number of calls in a given period is limited. In order to avoid a ban we limit
    /// by default the number of api requests.
    /// This function sets or removes the limitation.
    /// Burst false implies no block.
    /// Burst true implies there is a control over the number of calls allowed to the exchange
    pub async fn set_burst(&mut self, burst: bool) { self.burst = burst }

    #[allow(clippy::cast_sign_loss)]
    pub async fn block_or_continue(&self) {
        if !self.burst {
            let threshold: u64 = 2000; // 1 request/2sec
            let offset: u64 = get_unix_timestamp_ms() as u64 - self.last_request as u64;
            if offset < threshold {
                let wait_ms = Duration::from_millis(threshold - offset);
                thread::sleep(wait_ms);
            }
        }
    }

    async fn public_query<T: DeserializeOwned>(&self, method: &str, mut params: HashMap<&str, &str>) -> Result<T> {
        strip_empties(&mut params);
        let url = "https://api.kraken.com/0/public/".to_string() + method + "?" + &url_encode_hashmap(&params);

        self.block_or_continue().await;
        let resp = self.client.get(url).send().await?;
        resp.json().err_into().await
    }

    async fn private_query<T: DeserializeOwned>(&self, method: &str, mut params: HashMap<&str, &str>) -> Result<T> {
        let url = "https://api.kraken.com/0/private/".to_string() + method;

        let urlpath = "/0/private/".to_string() + method;

        let nonce = get_unix_timestamp_ms().to_string();
        strip_empties(&mut params);

        params.insert("nonce", &nonce);

        if let Some(ref password) = self.otp {
            params.insert("otp", password);
        }

        let post_data = url_encode_hashmap(&params);

        let signature = self.create_signature(urlpath.as_str(), &post_data, &nonce)?;

        let req = self
            .client
            .request(Method::POST, url)
            .header(KEY_HEADER, self.api_key.as_str())
            .header(SIGN_HEADER, signature)
            .body(post_data)
            .build()?;
        let resp = self.client.execute(req).await?;
        resp.json().err_into().await
    }

    fn create_signature(&self, urlpath: &str, postdata: &str, nonce: &str) -> Result<String> {
        let message_presha256 = nonce.to_string() + postdata;

        let mut sha256 = Sha256::default();
        sha256.update(message_presha256.as_bytes());

        let output = sha256.finalize();

        let mut concatenated = urlpath.as_bytes().to_vec();
        for elem in output {
            concatenated.push(elem);
        }

        let hmac_key = BASE64.decode(self.api_secret.as_bytes())?;
        let mut mac = Hmac::<Sha512>::new_from_slice(&hmac_key[..]).expect("HMAC can take key of any size");
        mac.update(&concatenated);
        Ok(BASE64.encode(mac.finalize().into_bytes().as_slice()))
    }

    /// Result: Server's time
    ///
    /// ```json
    /// unixtime =  as unix timestamp
    /// rfc1123 = as RFC 1123 time format
    /// ```
    /// Note: This is to aid in approximating the skew time between the server and client.
    pub async fn get_server_time(&mut self) -> Result<Map<String, Value>> {
        let params = HashMap::new();
        self.public_query("Time", params).await
    }

    /// Input:
    ///
    /// ```json
    /// info = info to retrieve (optional):
    ///     info = all info (default)
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = comma delimited list of assets to get info on (optional.  default = all for
    /// given asset class)
    /// ```
    /// Result: array of asset names and their info:
    ///
    /// ```json
    /// <asset_name> = asset name
    /// altname = alternate name
    /// aclass = asset class
    /// decimals = scaling decimal places for record keeping
    /// display_decimals = scaling decimal places for output display
    /// ```
    pub async fn get_asset_info(&self, info: &str, aclass: &str, asset: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("info", info);
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        self.public_query("Assets", params).await
    }

    /// Input:
    ///
    /// ```json
    /// info = info to retrieve (optional):
    ///     info = all info (default)
    ///     leverage = leverage info
    ///     fees = fees schedule
    ///     margin = margin info
    /// pair = comma delimited list of asset pairs to get info on (optional.  default = all)
    /// ```
    ///
    /// Result: array of pair names and their info
    ///
    /// ```json
    /// <pair_name> = pair name
    ///     altname = alternate pair name
    ///     aclass_base = asset class of base component
    ///     base = asset id of base component
    ///     aclass_quote = asset class of quote component
    ///     quote = asset id of quote component
    ///     lot = volume lot size
    ///     pair_decimals = scaling decimal places for pair
    ///     lot_decimals = scaling decimal places for volume
    ///     lot_multiplier = amount to multiply lot volume by to get currency volume
    ///     leverage_buy = array of leverage amounts available when buying
    ///     leverage_sell = array of leverage amounts available when selling
    ///     fees = fee schedule array in [volume, percent fee] tuples
    ///     fees_maker = maker fee schedule array in [volume, percent fee] tuples (if on
    ///     maker/taker)
    ///     fee_volume_currency = volume discount currency
    ///     margin_call = margin call level
    ///     margin_stop = stop-out/liquidation margin level
    /// ```
    pub async fn get_tradable_asset_pairs(&self, info: &str, pair: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("info", info);
        params.insert("pair", pair);
        self.public_query("AssetPairs", params).await
    }

    /// Input:
    ///
    /// ```json
    /// pair = comma delimited list of asset pairs to get info on
    /// ```
    ///
    /// Result: array of pair names and their ticker info
    ///
    /// ```json
    /// <pair_name> = pair name
    /// a = ask array(<price>, <whole lot volume>, <lot volume>),
    /// b = bid array(<price>, <whole lot volume>, <lot volume>),
    /// c = last trade closed array(<price>, <lot volume>),
    /// v = volume array(<today>, <last 24 hours>),
    /// p = volume weighted average price array(<today>, <last 24 hours>),
    /// t = number of trades array(<today>, <last 24 hours>),
    /// l = low array(<today>, <last 24 hours>),
    /// h = high array(<today>, <last 24 hours>),
    /// o = today's opening price
    /// ```
    pub(super) async fn get_ticker_information(
        &self,
        pair: &str,
    ) -> Result<KrakenResponse<HashMap<String, TickerInfo>>> {
        let mut params = HashMap::new();
        params.insert("pair", pair);
        self.public_query("Ticker", params).await
    }

    /// Input:
    ///
    /// ```json
    /// pair = asset pair to get OHLC data for
    /// interval = time frame interval in minutes (optional):
    ///     1 (default), 5, 15, 30, 60, 240, 1440, 10080, 21600
    /// since = return committed OHLC data since given id (optional.  exclusive)
    /// ```
    ///
    /// Result: array of pair name and OHLC data
    ///
    /// ```json
    /// <pair_name> = pair name
    ///     array of array entries(<time>, <open>, <high>, <low>, <close>, <vwap>, <volume>,
    ///     <count>)
    /// last = id to be used as since when polling for new, committed OHLC data
    /// ```
    ///
    /// Note: the last entry in the OHLC array is for the current, not-yet-committed frame and will
    /// always be present, regardless of the value of "since".
    pub async fn get_ohlc_data(&self, pair: &str, interval: &str, since: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("pair", pair);
        params.insert("interval", interval);
        params.insert("since", since);
        self.public_query("OHLC", params).await
    }

    /// Input:
    ///
    /// ```json
    /// pair = asset pair to get market depth for
    /// count = maximum number of asks/bids (optional)
    /// ```
    /// Result: array of pair name and market depth
    ///
    /// ```json
    /// <pair_name> = pair name
    ///     asks = ask side array of array entries(<price>, <volume>, <timestamp>)
    ///     bids = bid side array of array entries(<price>, <volume>, <timestamp>)
    /// ```
    pub(super) async fn get_order_book(&self, pair: &str, count: &str) -> Result<KrakenResponse<Orderbooks>> {
        let mut params = HashMap::new();
        params.insert("pair", pair);
        params.insert("count", count);
        self.public_query("Depth", params).await
    }

    /// Input:
    ///
    /// ```json
    /// pair = asset pair to get trade data for
    /// since = return trade data since given id (optional.  exclusive)
    /// ```
    /// Result: array of pair name and recent trade data
    ///
    /// ```json
    /// <pair_name> = pair name
    ///     array of array entries(<price>, <volume>, <time>, <buy/sell>, <market/limit>,
    /// <miscellaneous>)
    /// last = id to be used as since when polling for new trade data
    /// ```
    pub async fn get_recent_trades(&self, pair: &str, since: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("pair", pair);
        params.insert("since", since);
        self.public_query("Trades", params).await
    }

    /// Input:
    ///
    /// ```json
    /// pair = asset pair to get spread data for
    /// since = return spread data since given id (optional.  inclusive)
    /// ```
    ///
    /// Result: array of pair name and recent spread data
    ///
    /// ```json
    /// <pair_name> = pair name
    ///     array of array entries(<time>, <bid>, <ask>)
    /// last = id to be used as since when polling for new spread data
    /// ```
    /// Note: "since" is inclusive so any returned data with the same time as the previous set
    /// should overwrite all of the previous set's entries at that time
    pub async fn get_recent_spread_data(&self, pair: &str, since: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("pair", pair);
        params.insert("since", since);
        self.public_query("Spread", params).await
    }

    /// Result: array of asset names and balance amount
    pub async fn get_account_balance(&self) -> Result<KrakenResponse<Map<String, Value>>> {
        let params = HashMap::new();
        self.private_query("Balance", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = base asset used to determine balance (default = ZUSD)
    /// ```
    /// Result: array of trade balance info
    ///
    /// ```json
    /// eb = equivalent balance (combined balance of all currencies)
    /// tb = trade balance (combined balance of all equity currencies)
    /// m = margin amount of open positions
    /// n = unrealized net profit/loss of open positions
    /// c = cost basis of open positions
    /// v = current floating valuation of open positions
    /// e = equity = trade balance + unrealized net profit/loss
    /// mf = free margin = equity - initial margin (maximum margin available to open new positions)
    /// ml = margin level = (equity / initial margin) * 100
    /// ```
    /// Note: Rates used for the floating valuation is the midpoint of the best bid and ask prices
    pub async fn get_trade_balance(&self, aclass: &str, asset: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        self.private_query("TradeBalance", params).await
    }

    /// Input:
    ///
    /// ```json
    /// trades = whether or not to include trades in output (optional.  default = false)
    /// userref = restrict results to given user reference id (optional)
    /// ```
    ///
    /// Result: array of order info in open array with txid as the key
    ///
    /// ```json
    /// refid = Referral order transaction id that created this order
    /// userref = user reference id
    /// status = status of order:
    ///     pending = order pending book entry
    ///     open = open order
    ///     closed = closed order
    ///     canceled = order canceled
    ///     expired = order expired
    /// opentm = unix timestamp of when order was placed
    /// starttm = unix timestamp of order start time (or 0 if not set)
    /// expiretm = unix timestamp of order end time (or 0 if not set)
    /// descr = order description info
    ///     pair = asset pair
    ///     type = type of order (buy/sell)
    ///     ordertype = order type (See Add standard order)
    ///     price = primary price
    ///     price2 = secondary price
    ///     leverage = amount of leverage
    ///     order = order description
    ///     close = conditional close order description (if conditional close set)
    /// vol = volume of order (base currency unless viqc set in oflags)
    /// vol_exec = volume executed (base currency unless viqc set in oflags)
    /// cost = total cost (quote currency unless unless viqc set in oflags)
    /// fee = total fee (quote currency)
    /// price = average price (quote currency unless viqc set in oflags)
    /// stopprice = stop price (quote currency, for trailing stops)
    /// limitprice = triggered limit price (quote currency, when limit based order type triggered)
    /// misc = comma delimited list of miscellaneous info
    ///     stopped = triggered by stop price
    ///     touched = triggered by touch price
    ///     liquidated = liquidation
    ///     partial = partial fill
    /// oflags = comma delimited list of order flags
    ///     viqc = volume in quote currency
    ///     fcib = prefer fee in base currency (default if selling)
    ///     fciq = prefer fee in quote currency (default if buying)
    ///     nompp = no market price protection
    /// trades = array of trade ids related to order (if trades info requested and data available)
    /// ```
    ///
    /// Note: Unless otherwise stated, costs, fees, prices, and volumes are in the asset pair's
    /// scale, not the currency's scale. For example, if the asset pair uses a lot size that has
    /// a scale of 8, the volume will use a scale of 8, even if the currency it represents only has
    /// a scale of 2.
    /// Similarly, if the asset pair's pricing scale is 5, the scale will remain as 5, even if the
    /// underlying currency has a scale of 8.
    pub async fn get_open_orders(&self, trades: &str, userref: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("trades", trades);
        params.insert("userref", userref);
        self.private_query("OpenOrders", params).await
    }

    /// Input:
    ///
    /// ```json
    /// trades = whether or not to include trades in output (optional.  default = false)
    /// userref = restrict results to given user reference id (optional)
    /// start = starting unix timestamp or order tx id of results (optional.  exclusive)
    /// end = ending unix timestamp or order tx id of results (optional.  inclusive)
    /// ofs = result offset
    /// closetime = which time to use (optional)
    ///     open
    ///     close
    ///     both (default)
    /// ```
    ///
    /// Result: array of order info
    ///
    /// ```json
    /// closed = array of order info.  See Get open orders.  Additional fields:
    ///     closetm = unix timestamp of when order was closed
    ///     reason = additional info on status (if any)
    /// count = amount of available order info matching criteria
    /// ```
    /// Note: Times given by order tx ids are more accurate than unix timestamps. If an order tx id
    /// is given for the time, the order's open time is used
    pub async fn get_closed_orders(
        &self,
        trades: &str,
        userref: &str,
        start: &str,
        end: &str,
        ofs: &str,
        closetime: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("trades", trades);
        params.insert("userref", userref);
        params.insert("start", start);
        params.insert("end", end);
        params.insert("ofs", ofs);
        params.insert("closetime", closetime);
        self.private_query("OpenOrders", params).await
    }

    /// Input:
    ///
    /// ```json
    /// trades = whether or not to include trades in output (optional.  default = false)
    /// userref = restrict results to given user reference id (optional)
    /// txid = comma delimited list of transaction ids to query info about (20 maximum)
    /// ```
    /// Result: associative array of orders info
    ///
    /// ```json
    /// <order_txid> = order info.  See Get open orders/Get closed orders
    /// ```
    pub async fn query_orders_info(&self, trades: &str, userref: &str, txid: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("trades", trades);
        params.insert("userref", userref);
        params.insert("txid", txid);
        self.private_query("QueryOrders", params).await
    }

    /// Input:
    ///
    /// ```json
    /// type = type of trade (optional)
    ///     all = all types (default)
    ///     any position = any position (open or closed)
    ///     closed position = positions that have been closed
    ///     closing position = any trade closing all or part of a position
    ///     no position = non-positional trades
    /// trades = whether or not to include trades related to position in output (optional.
    /// default = false)
    /// start = starting unix timestamp or trade tx id of results (optional.  exclusive)
    /// end = ending unix timestamp or trade tx id of results (optional.  inclusive)
    /// ofs = result offset
    /// ```
    /// Result: array of trade info
    ///
    /// ```json
    /// trades = array of trade info with txid as the key
    ///     ordertxid = order responsible for execution of trade
    ///     pair = asset pair
    ///     time = unix timestamp of trade
    ///     type = type of order (buy/sell)
    ///     ordertype = order type
    ///     price = average price order was executed at (quote currency)
    ///     cost = total cost of order (quote currency)
    ///     fee = total fee (quote currency)
    ///     vol = volume (base currency)
    ///     margin = initial margin (quote currency)
    ///     misc = comma delimited list of miscellaneous info
    ///         closing = trade closes all or part of a position
    /// count = amount of available trades info matching criteria
    /// If the trade opened a position, the follow fields are also present in the trade info:
    ///
    ///     posstatus = position status (open/closed)
    ///     cprice = average price of closed portion of position (quote currency)
    ///     ccost = total cost of closed portion of position (quote currency)
    ///     cfee = total fee of closed portion of position (quote currency)
    ///     cvol = total fee of closed portion of position (quote currency)
    ///     cmargin = total margin freed in closed portion of position (quote currency)
    ///     net = net profit/loss of closed portion of position (quote currency, quote currency
    ///     scale)
    ///     trades = list of closing trades for position (if available)
    /// ```
    ///
    /// Note:
    ///
    /// Unless otherwise stated, costs, fees, prices, and volumes are in the asset pair's scale,
    /// not the currency's scale.
    /// Times given by trade tx ids are more accurate than unix timestamps.
    pub async fn get_trades_history(
        &self,
        type_trade: &str,
        trades: &str,
        start: &str,
        end: &str,
        ofs: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("type", type_trade);
        params.insert("trades", trades);
        params.insert("start", start);
        params.insert("end", end);
        params.insert("ofs", ofs);
        self.private_query("TradesHistory", params).await
    }

    /// Input:
    ///
    /// ```json
    /// txid = comma delimited list of transaction ids to query info about (20 maximum)
    /// trades = whether or not to include trades related to position in output (optional.
    /// default = false)
    /// ```
    // Result: associative array of trades info
    ///
    /// ```json
    /// <trade_txid> = trade info.  See Get trades history
    /// ```
    pub async fn query_trades_info(&self, txid: &str, trades: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("txid", txid);
        params.insert("trades", trades);
        self.private_query("QueryTrades", params).await
    }
    /// Input:
    ///
    /// ```json
    /// txid = comma delimited list of transaction ids to restrict output to
    /// docalcs = whether or not to include profit/loss calculations (optional.  default = false)
    /// ```
    /// Result: associative array of open position info
    ///
    /// ```json
    /// <position_txid> = open position info
    ///     ordertxid = order responsible for execution of trade
    ///     pair = asset pair
    ///     time = unix timestamp of trade
    ///     type = type of order used to open position (buy/sell)
    ///     ordertype = order type used to open position
    ///     cost = opening cost of position (quote currency unless viqc set in oflags)
    ///     fee = opening fee of position (quote currency)
    ///     vol = position volume (base currency unless viqc set in oflags)
    ///     vol_closed = position volume closed (base currency unless viqc set in oflags)
    ///     margin = initial margin (quote currency)
    ///     value = current value of remaining position (if docalcs requested.  quote currency)
    ///     net = unrealized profit/loss of remaining position (if docalcs requested.  quote
    ///           currency, quote currency scale)
    ///     misc = comma delimited list of miscellaneous info
    ///     oflags = comma delimited list of order flags
    ///         viqc = volume in quote currency
    /// ```
    ///
    /// Note: Unless otherwise stated, costs, fees, prices, and volumes are in the asset pair's
    /// scale, not the currency's scale.
    pub async fn get_open_positions(&self, txid: &str, docalcs: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("txid", txid);
        params.insert("docalcs", docalcs);
        self.private_query("OpenPositions", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = comma delimited list of assets to restrict output to (optional.  default = all)
    /// type = type of ledger to retrieve (optional):
    ///     all (default)
    ///     deposit
    ///     withdrawal
    ///     trade
    ///     margin
    /// start = starting unix timestamp or ledger id of results (optional.  exclusive)
    /// end = ending unix timestamp or ledger id of results (optional.  inclusive)
    /// ofs = result offset
    /// ```
    /// Result: associative array of ledgers info
    ///
    /// ```json
    /// <ledger_id> = ledger info
    ///     refid = reference id
    ///     time = unx timestamp of ledger
    ///     type = type of ledger entry
    ///     aclass = asset class
    ///     asset = asset
    ///     amount = transaction amount
    ///     fee = transaction fee
    ///     balance = resulting balance
    /// ```
    /// Note: Times given by ledger ids are more accurate than unix timestamps.
    pub async fn get_ledgers_info(
        &self,
        aclass: &str,
        asset: &str,
        type_ledger: &str,
        start: &str,
        end: &str,
        ofs: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        params.insert("type_ledger", type_ledger);
        params.insert("start", start);
        params.insert("end", end);
        params.insert("ofs", ofs);
        self.private_query("Ledgers", params).await
    }

    /// Input:
    ///
    /// ```json
    /// id = comma delimited list of ledger ids to query info about (20 maximum)
    /// ```
    /// Result: associative array of ledgers info
    ///
    /// ```json
    /// <ledger_id> = ledger info.  See Get ledgers info
    /// ```
    pub async fn query_ledgers(&self, id: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("id", id);
        self.private_query("QueryLedgers", params).await
    }

    /// Input:
    ///
    /// ```json
    /// pair = comma delimited list of asset pairs to get fee info on (optional)
    /// fee-info = whether or not to include fee info in results (optional)
    /// ```
    /// Result: associative array
    ///
    /// ```json
    /// currency = volume currency
    /// volume = current discount volume
    /// fees = array of asset pairs and fee tier info (if requested)
    ///     fee = current fee in percent
    ///     minfee = minimum fee for pair (if not fixed fee)
    ///     maxfee = maximum fee for pair (if not fixed fee)
    ///     nextfee = next tier's fee for pair (if not fixed fee.  nil if at lowest fee tier)
    ///     nextvolume = volume level of next tier (if not fixed fee.  nil if at lowest fee tier)
    ///     tiervolume = volume level of current tier (if not fixed fee.  nil if at lowest fee tier)
    /// fees_maker = array of asset pairs and maker fee tier info (if requested) for any pairs on
    ///             maker/taker schedule
    ///     fee = current fee in percent
    ///     minfee = minimum fee for pair (if not fixed fee)
    ///     maxfee = maximum fee for pair (if not fixed fee)
    ///     nextfee = next tier's fee for pair (if not fixed fee.  nil if at lowest fee tier)
    ///     nextvolume = volume level of next tier (if not fixed fee.  nil if at lowest fee tier)
    ///     tiervolume = volume level of current tier (if not fixed fee.  nil if at lowest fee tier)
    /// ```
    /// Note: If an asset pair is on a maker/taker fee schedule, the taker side is given in "fees"
    /// and maker side in "fees_maker". For pairs not on maker/taker, they will only be given in
    /// "fees".
    pub async fn get_trade_volume(&self, pair: &str, fee_info: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("pair", pair);
        params.insert("fee-info", fee_info);
        self.private_query("TradeVolume", params).await
    }

    // TODO: add optional closing order
    /// Input:
    ///
    /// ```json
    /// pair = asset pair
    /// type = type of order (buy/sell)
    /// ordertype = order type:
    ///     market
    ///     limit (price = limit price)
    ///     stop-loss (price = stop loss price)
    ///     take-profit (price = take profit price)
    ///     stop-loss-profit (price = stop loss price, price2 = take profit price)
    ///     stop-loss-profit-limit (price = stop loss price, price2 = take profit price)
    ///     stop-loss-limit (price = stop loss trigger price, price2 = triggered limit price)
    ///     take-profit-limit (price = take profit trigger price, price2 = triggered limit price)
    ///     trailing-stop (price = trailing stop offset)
    ///     trailing-stop-limit (price = trailing stop offset, price2 = triggered limit offset)
    ///     stop-loss-and-limit (price = stop loss price, price2 = limit price)
    ///     settle-position
    /// price = price (optional.  dependent upon ordertype)
    /// price2 = secondary price (optional.  dependent upon ordertype)
    /// volume = order volume in lots
    /// leverage = amount of leverage desired (optional.  default = none)
    /// oflags = comma delimited list of order flags (optional):
    ///     viqc = volume in quote currency (not available for leveraged orders)
    ///     fcib = prefer fee in base currency
    ///     fciq = prefer fee in quote currency
    ///     nompp = no market price protection
    ///     post = post only order (available when ordertype = limit)
    /// starttm = scheduled start time (optional):
    ///     0 = now (default)
    ///     +<n> = schedule start time <n> seconds from now
    ///     <n> = unix timestamp of start time
    /// expiretm = expiration time (optional):
    ///     0 = no expiration (default)
    ///     +<n> = expire <n> seconds from now
    ///     <n> = unix timestamp of expiration time
    /// userref = user reference id.  32-bit signed number.  (optional)
    /// validate = validate inputs only.  do not submit order (optional)
    ///
    /// optional closing order to add to system when order gets filled:
    ///     close[ordertype] = order type
    ///     close[price] = price
    ///     close[price2] = secondary price
    /// ```
    /// Result:
    ///
    /// ```json
    /// descr = order description info
    ///     order = order description
    ///     close = conditional close order description (if conditional close set)
    /// txid = array of transaction ids for order (if order was added successfully)
    /// Errors: errors include (but are not limited to):
    ///
    /// EGeneral:Invalid arguments
    /// EService:Unavailable
    /// ETrade:Invalid request
    /// EOrder:Cannot open position
    /// EOrder:Cannot open opposing position
    /// EOrder:Margin allowance exceeded
    /// EOrder:Margin level too low
    /// EOrder:Insufficient margin (exchange does not have sufficient funds to allow margin trading)
    /// EOrder:Insufficient funds (insufficient user funds)
    /// EOrder:Order minimum not met (volume too low)
    /// EOrder:Orders limit exceeded
    /// EOrder:Positions limit exceeded
    /// EOrder:Rate limit exceeded
    /// EOrder:Scheduled orders limit exceeded
    /// EOrder:Unknown position
    /// ```
    /// Note:
    ///
    /// See Get tradable asset pairs for specifications on asset pair prices, lots, and leverage.
    /// Prices can be preceded by +, -, or # to signify the price as a relative amount (with the
    ///     exception of trailing stops, which are always relative). + adds the amount to the
    ///     current offered price. - subtracts the amount from the current offered price. # will
    ///     either add or subtract the amount to the current offered price, depending on the type
    ///     and order type used. Relative prices can be suffixed with a % to signify the relative
    ///     amount as a percentage of the offered price.
    /// For orders using leverage, 0 can be used for the volume to auto-fill the volume needed to
    /// close out your position.
    /// If you receive the error "EOrder:Trading agreement required", refer to your API key
    /// management page for further details.
    pub(super) async fn add_standard_order(
        &self,
        pair: &str,
        o: StandardOrder<'_>,
    ) -> Result<KrakenResponse<OrderResult>> {
        let mut params = HashMap::new();
        params.insert("pair", pair);
        params.insert("type", o.type_order);
        params.insert("ordertype", o.ordertype);
        params.insert("price", o.price);
        params.insert("price2", o.price2);
        params.insert("volume", o.volume);
        params.insert("leverage", o.leverage);
        params.insert("oflags", o.oflags);
        params.insert("starttm", o.starttm);
        params.insert("expiretm", o.expiretm);
        params.insert("userref", o.userref);
        params.insert("validate", o.validate);
        self.private_query("AddOrder", params).await
    }

    /// Input:
    ///
    /// ```json
    /// txid = transaction id
    /// ```
    /// Result:
    ///
    /// ```json
    /// count = number of orders canceled
    /// pending = if set, order(s) is/are pending cancellation
    /// ```
    /// Note: txid may be a user reference id.
    pub async fn cancel_open_order(&self, txid: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("txid", txid);
        self.private_query("CancelOrder", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = asset being deposited
    /// ```
    /// Result: associative array of deposit methods:
    ///
    /// ```json
    /// method = name of deposit method
    /// limit = maximum net amount that can be deposited right now, or false if no limit
    /// fee = amount of fees that will be paid
    /// address-setup-fee = whether or not method has an address setup fee (optional)
    /// ```
    pub async fn get_deposit_methods(&self, aclass: &str, asset: &str) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        self.private_query("DepositMethods", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = asset being deposited
    /// method = name of the deposit method
    /// new = whether or not to generate a new address (optional.  default = false)
    /// ```
    /// Result: associative array of deposit addresses:
    ///
    /// ```json
    /// address = deposit address
    /// expiretm = expiration time in unix timestamp, or 0 if not expiring
    /// new = whether or not address has ever been used
    /// ```
    pub async fn get_deposit_addresses(
        &self,
        aclass: &str,
        asset: &str,
        method: &str,
        new: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        params.insert("method", method);
        params.insert("new", new);
        self.private_query("DepositAddresses", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = asset being deposited
    /// method = name of the deposit method
    /// ```
    /// Result: array of array deposit status information:
    ///
    /// ```json
    /// method = name of the deposit method used
    /// aclass = asset class
    /// asset = asset X-ISO4217-A3 code
    /// refid = reference id
    /// txid = method transaction id
    /// info = method transaction information
    /// amount = amount deposited
    /// fee = fees paid
    /// time = unix timestamp when request was made
    /// status = status of deposit
    /// status-prop = additional status properties (if available)
    ///     return = a return transaction initiated by Kraken
    ///     onhold = deposit is on hold pending review
    /// ```
    /// For information about the status, please refer to the IFEX financial transaction states.
    pub async fn get_status_of_recent_deposits(
        &self,
        aclass: &str,
        asset: &str,
        method: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        params.insert("method", method);
        self.private_query("DepositStatus", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = asset being withdrawn
    /// key = withdrawal key name, as set up on your account
    /// amount = amount to withdraw
    /// ```
    /// Result: associative array of withdrawal info:
    ///
    /// ```json
    /// method = name of the withdrawal method that will be used
    /// limit = maximum net amount that can be withdrawn right now
    /// fee = amount of fees that will be paid
    /// ```
    pub async fn get_withdrawal_information(
        &self,
        aclass: &str,
        asset: &str,
        key: &str,
        amount: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        params.insert("key", key);
        params.insert("amount", amount);
        self.private_query("WithdrawInfo", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = asset being withdrawn
    /// key = withdrawal key name, as set up on your account
    /// amount = amount to withdraw, including fees
    /// ```
    /// Result: associative array of withdrawal transaction:
    ///
    /// ```json
    /// refid = reference id
    /// ```
    pub async fn withdraw_funds(
        &self,
        aclass: &str,
        asset: &str,
        key: &str,
        amount: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        params.insert("key", key);
        params.insert("amount", amount);
        self.private_query("Withdraw", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = asset being withdrawn
    /// method = withdrawal method name (optional)
    /// ```
    /// Result: array of array withdrawal status information:
    ///
    /// ```json
    /// method = name of the withdrawal method used
    /// aclass = asset class
    /// asset = asset X-ISO4217-A3 code
    /// refid = reference id
    /// txid = method transaction id
    /// info = method transaction information
    /// amount = amount withdrawn
    /// fee = fees paid
    /// time = unix timestamp when request was made
    /// status = status of withdrawal
    /// status-prop = additional status properties (if available)
    ///     cancel-pending = cancelation requested
    ///     canceled = canceled
    ///     cancel-denied = cancelation requested but was denied
    ///     return = a return transaction initiated by Kraken; it cannot be canceled
    ///     onhold = withdrawal is on hold pending review
    /// ```
    /// For information about the status, please refer to the IFEX financial transaction states.
    pub async fn get_status_of_recent_withdrawals(
        &self,
        aclass: &str,
        asset: &str,
        method: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        params.insert("method", method);
        self.private_query("WithdrawStatus", params).await
    }

    /// Input:
    ///
    /// ```json
    /// aclass = asset class (optional):
    ///     currency (default)
    /// asset = asset being withdrawn
    /// refid = withdrawal reference id
    /// ```
    /// Result:
    /// ```json
    /// true on success
    /// ```
    ///
    /// Note: Cancelation cannot be guaranteed. This will put in a cancelation request. Depending
    /// upon how far along the withdrawal process is, it may not be possible to cancel the
    /// withdrawal.
    pub async fn request_withdrawal_cancelation(
        &self,
        aclass: &str,
        asset: &str,
        refid: &str,
    ) -> Result<Map<String, Value>> {
        let mut params = HashMap::new();
        params.insert("aclass", aclass);
        params.insert("asset", asset);
        params.insert("refid", refid);
        self.private_query("WithdrawCancel", params).await
    }
}

#[cfg(test)]
mod kraken_api_tests {
    //    #[test]
    //    fn should_block_or_not_block_when_enabled_or_disabled() {
    //        let mut api = KrakenApi {
    //            last_request: get_unix_timestamp_ms(),
    //            api_key: "".to_string(),
    //            api_secret: "".to_string(),
    //            otp: None,
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
    //            assert!(difference >= 1999);
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
