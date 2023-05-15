use std::collections::{HashMap, HashSet};
use std::io::Read;
use std::sync::{Arc, RwLock};

use broker_core::bot::BotWrapper;
use broker_core::broker::MarketEventEnvelopeRef;
use broker_core::json_util::deserialize_json_s;
use libflate::deflate::Decoder;
use serde::de::DeserializeOwned;
use serde_json::Value;
use signalr_rs::hub::client::{HubClient, HubClientError, HubClientHandler, HubQuery, PendingQuery, RestartPolicy};
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;

use broker_core::error::*;
use broker_core::prelude::*;
use broker_core::types::*;

use super::models::*;

#[derive(Debug)]
pub struct BittrexStreamingApi {
    sink: UnboundedSender<MarketEventEnvelopeRef>,
    books: Arc<RwLock<HashMap<Pair, LiveAggregatedOrderBook>>>,
    order_book_pairs: HashSet<Pair>,
    trade_pairs: HashSet<Pair>,
}

const BITTREX_HUB: &str = "c2";

impl BittrexStreamingApi {
    /// Create a new bittrex exchange bot, unavailable channels and currencies are ignored
    /// # Panics
    ///
    /// If currency symbols of channels cannot be found in the registry
    pub async fn new_bot(
        _creds: &dyn Credentials,
        channels: Vec<MarketChannel>,
    ) -> Result<BotWrapper<HubClient, UnboundedReceiverStream<MarketEventEnvelopeRef>>> {
        // Live order book pairs
        let order_book_pairs: HashSet<Pair> = channels
            .iter()
            .filter(|c| c.r#type == MarketChannelType::Orderbooks)
            .map(|c| c.symbol.value.clone())
            .collect();
        // Live trade pairs
        let trade_pairs: HashSet<Pair> = channels
            .iter()
            .filter(|c| c.r#type == MarketChannelType::Trades)
            .map(|c| c.symbol.value.clone())
            .collect();

        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();

        let api: Box<dyn HubClientHandler + Send> = Box::new(BittrexStreamingApi {
            sink: tx.clone(),
            books: Arc::new(RwLock::new(HashMap::new())),
            order_book_pairs: order_book_pairs.clone(),
            trade_pairs,
        });
        // SignalR Client
        let client = HubClient::new(
            BITTREX_HUB,
            "https://socket.bittrex.com/signalr/",
            20,
            RestartPolicy::Always,
            api,
        )
        .await;
        let stream = UnboundedReceiverStream::new(rx);
        match client {
            Ok(addr) => {
                if !order_book_pairs.is_empty() {
                    for pair in order_book_pairs {
                        let currency = super::utils::get_pair_string(&pair).unwrap();
                        addr.do_send(HubQuery::new(
                            BITTREX_HUB.to_string(),
                            "QueryExchangeState".to_string(),
                            vec![currency.to_string()],
                            "QE2".to_string(),
                        ));
                    }
                }
                Ok(BotWrapper::new(addr, stream))
            }
            Err(e) => Err(Error::ExchangeError(format!("{}", e))),
        }
    }

    fn deflate<T>(binary: &str) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let decoded = base64::decode(binary)
            .map_err(|e| Error::ExchangeError(format!("{}", HubClientError::Base64DecodeError(e))))?;
        let mut decoder = Decoder::new(&decoded[..]);
        let mut decoded_data: Vec<u8> = Vec::new();
        decoder.read_to_end(&mut decoded_data).map_err(|_| {
            Error::ExchangeError(format!("{}", HubClientError::InvalidData {
                data: vec!["cannot deflate".to_string()],
            }))
        })?;
        let v: &[u8] = &decoded_data;
        deserialize_json_s::<T>(v)
    }

    fn deflate_array<T>(a: &Value) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let data: Vec<String> = serde_json::from_value(a.clone())?;
        let binary = data
            .first()
            .ok_or_else(|| Error::ExchangeError(format!("{}", HubClientError::MissingData)))?;
        Self::deflate::<T>(binary)
    }

    fn deflate_string<T>(a: &Value) -> Result<T>
    where
        T: DeserializeOwned,
    {
        let binary: String = serde_json::from_value(a.clone())?;
        Self::deflate::<T>(&binary)
    }
}

impl HubClientHandler for BittrexStreamingApi {
    fn on_connect(&self) -> Vec<Box<dyn PendingQuery>> {
        {
            let mut books = self.books.write().unwrap();
            for pair in self.order_book_pairs.iter() {
                books.insert(pair.clone(), LiveAggregatedOrderBook::default(pair.clone()));
            }
        }
        let mut conn_queries: Vec<Box<dyn PendingQuery>> = vec![];
        if !self.trade_pairs.is_empty() || !self.order_book_pairs.is_empty() {
            let all_pairs: HashSet<Pair> = self.trade_pairs.union(&self.order_book_pairs).cloned().collect();
            let currencies: Vec<String> = all_pairs
                .iter()
                .map(|p| (*super::utils::get_pair_string(p).unwrap()).to_string())
                .collect();
            info!("Bittrex : connecting to ExchangeDeltas for {:?}", &currencies);
            for currency in currencies {
                conn_queries.push(Box::new(HubQuery::new(
                    BITTREX_HUB.to_string(),
                    "SubscribeToExchangeDeltas".to_string(),
                    vec![currency],
                    "1".to_string(),
                )));
            }
        }
        conn_queries
    }

    fn error(&self, _: Option<&str>, _: &Value) {}

    #[allow(clippy::cast_possible_wrap)]
    fn handle(&mut self, method: &str, message: &Value) {
        let live_events = match method {
            "uE" => {
                let delta = Self::deflate_array::<MarketDelta>(message).unwrap();
                let pair = super::utils::get_pair_enum(delta.MarketName.as_str());
                if pair.is_err() {
                    return;
                }
                let mut events = vec![];
                let current_pair = pair.unwrap();
                if self.order_book_pairs.contains(&current_pair) {
                    let mut books = self.books.write().unwrap();
                    let default_book = LiveAggregatedOrderBook::default(current_pair.clone());
                    let agg = books.entry(current_pair.clone()).or_insert(default_book);
                    let asks = delta.Sells.into_iter().map(|op| (op.Rate, op.Quantity));
                    agg.update_asks(asks);
                    let buys = delta.Buys.into_iter().map(|op| (op.Rate, op.Quantity));
                    agg.update_bids(buys);
                    if let Some(ob) = agg.latest_order_book() {
                        events.push(MarketEvent::Orderbook(ob));
                    }
                }
                if self.trade_pairs.contains(&current_pair) {
                    for fill in delta.Fills {
                        let lt = Trade {
                            event_ms: fill.TimeStamp as i64,
                            pair: format!("{}", current_pair).into(),
                            amount: fill.Quantity,
                            price: fill.Rate,
                            tt: fill.OrderType.into(),
                        };
                        events.push(MarketEvent::Trade(lt));
                    }
                }
                if events.is_empty() {
                    Err(Error::UnhandledEventType)
                } else {
                    Ok(events)
                }
            }
            "uS" => Self::deflate_array::<SummaryDeltaResponse>(message)
                .map_err(|_| Error::BadParse)
                .and(Err(Error::UnhandledEventType)),
            s if s.starts_with("QE") => {
                let state = Self::deflate_string::<ExchangeState>(message).unwrap();
                let pair = super::utils::get_pair_enum(state.MarketName.as_str());
                if pair.is_err() {
                    return;
                }
                let mut books = self.books.write().unwrap();
                let current_pair = pair.unwrap();
                let default_book = LiveAggregatedOrderBook::default(current_pair.clone());
                let agg = books.entry(current_pair).or_insert(default_book);
                let asks = state.Sells.iter().map(|op| (op.R, op.Q));
                agg.reset_asks_n(asks);
                let bids = state.Buys.iter().map(|op| (op.R, op.Q));
                agg.reset_bids_n(bids);
                let latest_order_book: Orderbook = agg.order_book();
                Ok(vec![MarketEvent::Orderbook(latest_order_book)])
            }
            _ => {
                trace!("Unknown message : method {:?} message {:?}", method, message);
                Err(Error::UnhandledEventType)
            }
        };
        if let Ok(les) = live_events {
            for le in les {
                if let Err(e) = self.sink.send(Arc::new(MarketEventEnvelope::new(
                    Symbol::new(le.pair(), SecurityType::Crypto, Exchange::Bittrex),
                    le,
                ))) {
                    error!("broadcast failure for {}, {}", e.0.symbol.value.as_ref(), e.0.e.chan());
                }
            }
        }
    }
}
