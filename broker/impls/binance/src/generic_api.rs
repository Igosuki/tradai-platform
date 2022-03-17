//! Use this module to interact with Binance through a Generic API.
//! This a more convenient and safe way to deal with the exchange since methods return a Result<>
//! but this generic API does not provide all the functionnality that Binance offers.

use std::time::Duration;

use async_trait::async_trait;
use backoff::ExponentialBackoffBuilder;
use chrono::{TimeZone, Utc};
use itertools::Itertools;

use binance::account::{OrderRequest, OrderStatusRequest};
use binance::rest_model::{Filters, InterestRateHistoryQuery, MarginOrder, MarginOrderQuery};
use futures::TryFutureExt;

use super::adapters::is_isolated_margin_str;
use super::api::BinanceApi;

use crate::adapters::{from_binance_balance, from_binance_error, from_binance_isolated_margin_account_details,
                      from_binance_margin_account_details, from_binance_margin_order_result,
                      from_binance_margin_order_state, from_binance_order, from_binance_transaction,
                      to_binance_margin_order, to_binance_order_request};
use broker_core::error::*;
use broker_core::pair::{pair_string, symbol_to_pair, PairConf};
use broker_core::prelude::*;
use broker_core::types::*;

#[async_trait]
impl Brokerage for BinanceApi {
    async fn ticker(&self, pair: Pair) -> Result<Ticker> {
        let market = self.market();

        let pair_str = pair_string(Exchange::Binance, &pair)?;
        let result = market
            .get_24h_price_stats(pair_str.to_string())
            .await
            .map_err(from_binance_error)?;
        Ok(Ticker {
            timestamp: get_unix_timestamp_ms(),
            pair,
            last_trade_price: result.last_price,
            lowest_ask: result.low_price,
            highest_bid: result.high_price,
            volume: Some(result.volume),
        })
    }

    async fn orderbook(&self, pair: Pair) -> Result<Orderbook> {
        let market = self.market();
        let pair_str = pair_string(Exchange::Binance, &pair)?;

        let book_ticker = market.get_depth(pair_str).await.map_err(from_binance_error)?;

        Ok(Orderbook {
            timestamp: get_unix_timestamp_ms(),
            pair,
            last_order_id: Some(book_ticker.last_update_id.to_string()),
            asks: book_ticker.asks.iter().map(|a| (a.price, a.qty)).collect(),
            bids: book_ticker.bids.iter().map(|a| (a.price, a.qty)).collect(),
        })
    }

    /// Return the balances for each currency on the account
    async fn account_balances(&self) -> Result<AccountPosition> {
        let result = self.account().get_account().await.map_err(from_binance_error)?;

        let mut balances = AccountPosition::new();

        balances.update_time = Utc.timestamp_millis(result.update_time);

        for balance in result.balances {
            balances.insert(balance.asset.clone().into(), from_binance_balance(balance));
        }

        Ok(balances)
    }

    async fn margin_account(&self, asset: Option<String>) -> Result<MarginAccountDetails> {
        let details: MarginAccountDetails = match asset {
            Some(pair) => {
                let pair_str = pair_string(Exchange::Binance, &pair.into())?;
                let f = self
                    .margin()
                    .isolated_details(Some(vec![pair_str]))
                    .await
                    .map_err(from_binance_error)?;
                from_binance_isolated_margin_account_details(f)
            }
            None => {
                let f = self.margin().details().await.map_err(from_binance_error)?;
                from_binance_margin_account_details(f)
            }
        };
        Ok(details)
    }

    async fn add_order(&self, order: AddOrderRequest) -> Result<OrderSubmission> {
        let pair_conf = broker_core::pair::pair_conf(&Exchange::Binance, &order.pair)?;
        let is_dry_run = &order.dry_run;
        if order.order_type == OrderType::Limit && order.price.is_none() {
            return Err(Error::MissingPrice);
        }
        match order.asset_type {
            None | Some(AssetType::Spot) => {
                let order_request: OrderRequest = to_binance_order_request(&order, &pair_conf);

                let account = self.account();
                if *is_dry_run {
                    let _tr = account
                        .place_test_order(order_request)
                        .await
                        .map_err(from_binance_error)?;
                    let submission = order.simulate_submission(0.001);
                    Ok(submission)
                } else {
                    let tr = account.place_order(order_request).await.map_err(from_binance_error)?;
                    let symbol = tr.symbol.clone().into();
                    Ok(OrderSubmission {
                        pair: symbol_to_pair(&Exchange::Binance, &symbol)?,
                        ..from_binance_transaction(tr)
                    })
                }
            }
            Some(t @ (AssetType::Margin | AssetType::IsolatedMargin)) => {
                let margin = self.margin();
                let new_order: MarginOrder = to_binance_margin_order(&order, &pair_conf, t);
                let margin_order_result = margin.new_order(new_order).await.map_err(from_binance_error)?;
                let symbol = margin_order_result.symbol.clone().into();
                Ok(OrderSubmission {
                    pair: symbol_to_pair(&Exchange::Binance, &symbol)?,
                    ..from_binance_margin_order_result(margin_order_result)
                })
            }
            _ => unimplemented!(),
        }
    }

    async fn get_order(&self, id: String, pair: Pair, asset_type: AssetType) -> Result<Order> {
        let res = match asset_type {
            AssetType::Spot => self
                .account()
                .order_status(OrderStatusRequest {
                    orig_client_order_id: Some(id),
                    symbol: pair_string(Exchange::Binance, &pair.clone())?,
                    ..OrderStatusRequest::default()
                })
                .await
                .map(from_binance_order),
            t @ (AssetType::Margin | AssetType::IsolatedMargin) => self
                .margin()
                .order(MarginOrderQuery {
                    orig_client_order_id: Some(id),
                    symbol: pair_string(Exchange::Binance, &pair.clone())?,
                    is_isolated: Some(is_isolated_margin_str(t)),
                    ..MarginOrderQuery::default()
                })
                .await
                .map(from_binance_margin_order_state),
            _ => return Err(Error::BrokerFeatureNotImplemented),
        };
        match res {
            Ok(os) => Ok(os),
            Err(e) => Err(from_binance_error(e)),
        }
    }

    #[allow(clippy::cast_possible_truncation)]
    async fn pairs(&self) -> Result<Vec<PairConf>> {
        let general = self.general();
        let retry_policy = ExponentialBackoffBuilder::new()
            .with_multiplier(2.0)
            .with_max_interval(Duration::from_secs(2))
            .with_initial_interval(Duration::from_millis(10))
            .with_max_elapsed_time(Some(Duration::from_secs(2)))
            .build();
        let mut symbols: Vec<PairConf> = Vec::new();
        let exchange_info = backoff::future::retry(retry_policy, || general.exchange_info().err_into())
            .await
            .map_err(from_binance_error)?;
        exchange_info.symbols.into_iter().for_each(|symbol| {
            let mut conf = PairConf {
                base: symbol.base_asset.clone(),
                quote: symbol.quote_asset.clone(),
                symbol: symbol.symbol.into(),
                pair: format!("{}_{}", symbol.base_asset, symbol.quote_asset).into(),
                base_precision: Some(symbol.base_asset_precision as u32),
                quote_precision: Some(symbol.quote_precision as u32),
                spot_allowed: symbol.is_spot_trading_allowed,
                isolated_margin_allowed: symbol.is_margin_trading_allowed,
                ..PairConf::default()
            };
            for filter in symbol.filters {
                match filter {
                    Filters::PriceFilter {
                        max_price,
                        min_price,
                        tick_size,
                    } => {
                        conf.max_price = Some(max_price);
                        conf.min_price = Some(min_price);
                        conf.step_price = Some(tick_size);
                    }
                    Filters::LotSize {
                        min_qty,
                        max_qty,
                        step_size,
                    } => {
                        conf.min_qty = Some(min_qty);
                        conf.max_qty = Some(max_qty);
                        conf.step_qty = Some(step_size);
                    }
                    Filters::MarketLotSize {
                        min_qty,
                        max_qty,
                        step_size,
                    } => {
                        conf.min_market_qty = Some(min_qty);
                        conf.max_market_qty = Some(max_qty);
                        conf.step_market_qty = Some(step_size);
                    }
                    Filters::MinNotional { min_notional, .. } => conf.min_size = Some(min_notional),
                    _ => (),
                }
            }
            symbols.push(conf);
        });
        Ok(symbols)
    }

    fn exchange(&self) -> Exchange { Exchange::Binance }

    fn uses_account(&self) -> bool { self.api_key.is_some() && self.api_secret.is_some() }

    #[allow(clippy::cast_possible_truncation)]
    async fn margin_interest_rate(&self, symbol: MarketSymbol) -> Result<InterestRate> {
        let margin = self.margin();
        let history = margin
            .interest_rate_history(InterestRateHistoryQuery {
                asset: symbol.to_string(),
                vip_level: Some(self.vip_level),
                start_time: None,
                end_time: None,
                limit: None,
            })
            .await
            .map_err(from_binance_error)?;
        let most_recent_rate = history
            .into_iter()
            .sorted_by(|h1, h2| h1.timestamp.cmp(&h2.timestamp))
            .last();
        most_recent_rate
            .map(|h| InterestRate {
                symbol: h.asset,
                ts: h.timestamp as u64,
                rate: h.daily_interest_rate,
                period: InterestRatePeriod::Daily,
            })
            .ok_or(Error::NotFound)
    }
}
