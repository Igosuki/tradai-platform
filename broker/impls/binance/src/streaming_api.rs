use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use actix::io::SinkWrite;
use async_trait::async_trait;
use awc::ws::Message;
use binance::config::Config;
use binance::ws_model::{CombinedStreamEvent, QueryResult, WebsocketEvent, WebsocketEventUntag};
use broker_core::bot::{BotWrapper, DefaultWsActor, WsFramedSink, WsHandler};
use broker_core::broker::MarketEventEnvelopeRef;
use broker_core::metrics::ExchangeMetrics;
use bstr::ByteSlice;
use bytes::Bytes;
use chrono::{TimeZone, Utc};
use dashmap::DashMap;
use futures::stream::{FuturesUnordered, StreamExt};
use itertools::Itertools;
use tokio::sync::mpsc::{self, Receiver, UnboundedSender};
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;

use super::BinanceApi;
use broker_core::error::*;
use broker_core::json_util::deserialize_json_s;
use broker_core::pair::{pair_to_symbol, symbol_to_pair};
use broker_core::prelude::*;
use broker_core::streaming_api::StreamingApi;
use broker_core::types::*;

use super::adapters::*;

#[derive(Clone)]
pub struct BinanceStreamingApi {
    books: Arc<DashMap<Pair, LiveAggregatedOrderBook>>,
    channels: Vec<MarketChannel>,
    sink: UnboundedSender<MarketEventEnvelopeRef>,
    api: Arc<BinanceApi>,
    metrics: Arc<ExchangeMetrics>,
    orderbook_depths: HashMap<Pair, u16>,
}

impl BinanceStreamingApi {
    /// Create a new binance exchange bot, unavailable channels and currencies are ignored
    pub async fn try_new(
        creds: &dyn Credentials,
        channels: Vec<MarketChannel>,
        use_test: bool,
    ) -> Result<BotWrapper<DefaultWsActor, UnboundedReceiverStream<MarketEventEnvelopeRef>>> {
        let metrics = ExchangeMetrics::for_exchange(Exchange::Binance);
        let conf = if use_test { Config::testnet() } else { Config::default() };
        let exchange_api = BinanceApi::new_with_config(creds, conf.clone()).await?;
        let url = Self::streams_url(&channels, &conf.ws_endpoint)?;
        let (tx, rx) = mpsc::unbounded_channel();
        let orderbook_depths: HashMap<Pair, u16> = channels
            .iter()
            .filter(|c| c.orderbook.is_some())
            .map(|c| (c.pair().clone(), c.orderbook.unwrap().depth.unwrap()))
            .collect();
        let api = Arc::new(Self {
            sink: tx.clone(),
            books: Arc::new(DashMap::new()),
            channels,
            api: Arc::new(exchange_api),
            metrics: Arc::new(metrics),
            orderbook_depths,
        });

        let addr = DefaultWsActor::new(
            "BinanceStream",
            url,
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(60)),
            api,
        )
        .await?;

        Ok(BotWrapper::new(addr, UnboundedReceiverStream::new(rx)))
    }

    fn streams_url(channels: &[MarketChannel], endpoint: &str) -> Result<Url> {
        let mut url = Url::parse(endpoint)?;
        url.path_segments_mut()
            .map_err(|_| Error::ParseUrl(url::ParseError::RelativeUrlWithoutBase))?
            .push(binance::websockets::STREAM_ENDPOINT);
        let stream_str = channels
            .iter()
            .flat_map(|c| {
                pair_to_symbol(&Exchange::Binance, c.pair()).and_then(|pair| {
                    let sub = &subscription(c, &[pair.to_string()], 0, c.orderbook.and_then(|oc| oc.depth));
                    Ok(sub.params.join("/"))
                })
            })
            .join("/");
        url.set_query(Some(&format!("streams={}", stream_str)));
        debug!("Binance connecting to the following streams : {}", stream_str);
        Ok(url)
    }

    // TODO: this is generic
    #[allow(dead_code)]
    async fn refresh_order_books(&self) {
        let order_book_pairs: Vec<Pair> = self
            .channels
            .iter()
            .filter(|c| c.r#type == MarketChannelType::Orderbooks)
            .map(|c| c.symbol.value.clone())
            .collect();
        if order_book_pairs.is_empty() {
            return;
        }
        let mut orderbooks_futs: Vec<Receiver<Result<Orderbook>>> = order_book_pairs
            .iter()
            .map(|pair| {
                let api_c = self.api.clone();
                let (tx, rx) = mpsc::channel::<Result<Orderbook>>(100);
                let p = pair.clone();
                tokio::spawn(async move {
                    let r = api_c.orderbook(p.clone()).await.map_err(|e| {
                        info!("Binance : error fetching order book for {:?} : {:?}", p.clone(), e);
                        e
                    });
                    if tx.send(r).await.is_err() {
                        trace!(
                            "BinanceStreamingApi: failed to send orderbook back in channel because received dropped"
                        );
                    };
                });
                rx
            })
            .collect();

        let recv_futs: FuturesUnordered<_> = orderbooks_futs.iter_mut().map(Receiver::recv).collect();
        let received: Vec<Option<Result<Orderbook>>> = recv_futs.collect().await;
        for ob in received.iter().flatten().flatten() {
            let books = &self.books;
            let default_book = LiveAggregatedOrderBook::default_with_depth(
                ob.pair.clone(),
                self.orderbook_depths.get(&ob.pair).copied(),
            );
            let mut agg = books.entry(ob.pair.clone()).or_insert(default_book);
            agg.set_last_order_id(ob.last_order_id.clone());
            agg.set_ts(ob.timestamp);
            agg.reset_asks(ob.asks.iter());
            agg.reset_bids(ob.bids.iter());
            let latest_order_book: Orderbook = agg.order_book();
            self.broadcast(MarketEvent::Orderbook(latest_order_book.clone()));
        }
    }

    fn get_pair(&self, symbol: &str) -> Result<Pair> {
        symbol_to_pair(&Self::EXCHANGE, &MarketSymbol::from(symbol)).map_err(|e| {
            self.metrics.in_unsupported_pair(symbol, "order_books");
            e
        })
    }

    // TODO: this is generic
    fn latest_book<F>(&self, pair: &Pair, f: F) -> Option<Orderbook>
    where
        F: Fn(&mut LiveAggregatedOrderBook),
    {
        // Plumbing, init the book for the pair if it didn't exist
        let books = self.books.as_ref();
        let mut agg = books.entry(pair.clone()).or_insert_with(|| {
            LiveAggregatedOrderBook::default_with_depth(pair.clone(), self.orderbook_depths.get(pair).copied())
        });
        f(&mut agg);
        agg.latest_order_book().map(|b| self.log_book(b))
    }

    // TODO: this is generic
    pub(crate) fn log_book(&self, ob: Orderbook) -> Orderbook {
        // If the book has changed, flush metrics and broadcast the change
        if let Some(lowest_ask) = ob.asks.first() {
            self.metrics
                .lowest_ask(lowest_ask.0, lowest_ask.1, &ob.pair, "order_books");
        }
        if let Some(highest_bid) = ob.bids.first() {
            self.metrics
                .top_bid(highest_bid.0, highest_bid.1, &ob.pair, "order_books");
        }

        ob
    }

    // TODO: this is generic
    fn broadcast(&self, v: MarketEvent) {
        let (pair, channel) = (&v.pair(), v.chan());
        self.metrics.event_broadcasted(pair, channel);
        let msg = Arc::new(MarketEventEnvelope::new(
            Symbol::new(pair.clone(), SecurityType::Crypto, Self::EXCHANGE),
            v,
        ));
        if let Err(e) = self.sink.send(msg) {
            self.metrics.broadcast_failure(e.0.symbol.value.as_ref(), e.0.e.chan())
        }
    }

    #[allow(clippy::cast_possible_wrap, clippy::cast_sign_loss)]
    fn parse_websocket_event(&self, event: CombinedStreamEvent<WebsocketEventUntag>) -> Result<Option<MarketEvent>> {
        let r = match event.data {
            WebsocketEventUntag::Orderbook(ref ob) => {
                let symbol = &event.parse_stream().0;
                let pair = self.get_pair(symbol.to_uppercase().as_str())?;
                self.latest_book(&pair, |agg| {
                    agg.reset_asks_n(ob.asks.iter().map(|a| (a.price, a.qty)));
                    agg.reset_bids_n(ob.bids.iter().map(|a| (a.price, a.qty)));
                })
                .map(MarketEvent::Orderbook)
            }
            WebsocketEventUntag::WebsocketEvent(WebsocketEvent::DepthOrderBook(ob)) => {
                let pair = self.get_pair(&ob.symbol)?;

                self.latest_book(&pair, |agg| {
                    // Update the book with latest asks and bids
                    // TODO: cannot use async code here for now because SinkWrite is not send
                    //self.refresh_order_books();
                    //need to use a mutex in the strat
                    agg.update_asks(ob.asks.iter().map(|a| (a.price, a.qty)));
                    agg.update_bids(ob.bids.iter().map(|a| (a.price, a.qty)));
                })
                .map(MarketEvent::Orderbook)
            }
            WebsocketEventUntag::WebsocketEvent(WebsocketEvent::Trade(e)) => {
                let pair = self.get_pair(e.symbol.as_str())?;
                Some(MarketEvent::Trade(Trade {
                    amount: e.qty.parse::<f64>().unwrap(),
                    event_ms: e.event_time as i64,
                    price: e.price.parse::<f64>().unwrap(),
                    tt: TradeType::Sell,
                    pair,
                }))
            }
            WebsocketEventUntag::WebsocketEvent(WebsocketEvent::Kline(ke)) => {
                let pair = self.get_pair(ke.symbol.as_str())?;
                Some(MarketEvent::TradeCandle(Candle {
                    event_time: Utc.timestamp_millis_opt(ke.event_time as i64).unwrap(),
                    pair,
                    start_time: Utc.timestamp_millis_opt(ke.kline.start_time).unwrap(),
                    end_time: Utc.timestamp_millis_opt(ke.kline.end_time).unwrap(),
                    open: ke.kline.open,
                    high: ke.kline.close,
                    low: ke.kline.low,
                    close: ke.kline.close,
                    volume: ke.kline.volume,
                    quote_volume: ke.kline.quote_volume,
                    trade_count: ke.kline.number_of_trades as u64,
                    is_final: ke.kline.is_final_bar,
                }))
            }
            _ => None,
        };
        Ok(r)
    }
}

#[async_trait]
impl WsHandler for BinanceStreamingApi {
    #[cfg_attr(feature = "flame", flame)]
    fn handle_in(&self, _w: &mut SinkWrite<Message, WsFramedSink>, msg: Bytes) {
        if msg.contains_str("result") {
            let v: Result<QueryResult> = serde_json::from_slice(msg.as_ref()).map_err(Error::Json);
            match v {
                Ok(r) => info!("Got result for id {} : {:?}", r.id, r.result),
                Err(e) => {
                    trace!(err = ?e, msg = ?msg, "binance error deserializing result");
                    return;
                }
            }
        }
        let v: Result<CombinedStreamEvent<WebsocketEventUntag>> = deserialize_json_s(msg.as_ref());
        if let Err(err) = v {
            trace!(err = ?err, msg = ?msg, "binance error deserializing");
            return;
        }
        if let Ok(se) = v {
            if let Ok(Some(e)) = self.parse_websocket_event(se) {
                self.broadcast(e);
            }
        }
    }

    fn handle_started(&self, _w: &mut SinkWrite<Message, WsFramedSink>) {
        // Do nothing, connections handled in combined stream
        self.metrics.stream_reconnected();
    }
}

impl StreamingApi for BinanceStreamingApi {
    const NAME: &'static str = "binance";
    const EXCHANGE: Exchange = Exchange::Binance;
}
