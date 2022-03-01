use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use actix::io::SinkWrite;
use async_trait::async_trait;
use awc::ws::Message;
use broker_core::bot::{BotWrapper, DefaultWsActor, WsFramedSink, WsHandler};
use broker_core::broker::MarketEventEnvelopeRef;
use broker_core::metrics::ExchangeMetrics;
use bytes::Bytes;
use derivative::Derivative;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;

use broker_core::error::*;
use broker_core::json_util::deserialize_json_s;
use broker_core::prelude::*;
use broker_core::streaming_api::StreamingApi;
use broker_core::types::*;

use super::models::*;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct BitstampStreamingApi {
    sink: UnboundedSender<MarketEventEnvelopeRef>,
    channels: HashMap<StreamChannel, HashSet<Pair>>,
    #[derivative(Debug = "ignore")]
    metrics: Arc<ExchangeMetrics>,
}

impl BitstampStreamingApi {
    #[allow(clippy::missing_panics_doc)]
    pub async fn new_bot(
        _creds: &dyn Credentials,
        channels: HashMap<StreamChannel, HashSet<Pair>>,
    ) -> Result<BotWrapper<DefaultWsActor, UnboundedReceiverStream<MarketEventEnvelopeRef>>> {
        let metrics = Arc::new(ExchangeMetrics::for_exchange(Exchange::Binance));
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let api = BitstampStreamingApi {
            sink: tx,
            channels,
            metrics,
        };
        let addr = DefaultWsActor::new(
            "BitstampStream",
            Url::from_str("wss://ws.bitstamp.net").unwrap(),
            Some(Duration::from_secs(5)),
            Some(Duration::from_secs(60)),
            Arc::new(api),
        )
        .await?;
        Ok(BotWrapper::new(addr, UnboundedReceiverStream::new(rx)))
    }

    // TODO : this is generic
    fn broadcast(&self, v: MarketEvent) {
        let (pair, channel) = (&v.pair(), v.chan());
        self.metrics.event_broadcasted(pair, channel);
        let msg = Arc::new(MarketEventEnvelope::new(
            Symbol::new(pair.clone(), SecurityType::Crypto, Self::EXCHANGE),
            v,
        ));
        if let Err(e) = self.sink.send(msg) {
            self.metrics.broadcast_failure(e.0.symbol.value.as_ref(), e.0.e.chan());
        }
    }
}

#[async_trait]
impl WsHandler for BitstampStreamingApi {
    #[cfg_attr(feature = "flame", flame)]
    fn handle_in(&self, w: &mut SinkWrite<Message, WsFramedSink>, msg: Bytes) {
        let v: Result<Event> = deserialize_json_s(msg.as_ref());
        if v.is_err() {
            return;
        }
        match v.unwrap() {
            Event::ReconnectRequest(_) => {
                self.handle_started(w);
            }
            Event::SubSucceeded(_) => (),
            o => {
                if let Ok(le) = o.try_into() {
                    self.broadcast(le);
                }
            }
        };
    }

    #[cfg_attr(feature = "flame", flame)]
    fn handle_started(&self, w: &mut SinkWrite<Message, WsFramedSink>) {
        for (k, v) in self.channels.clone() {
            for pair in v {
                let result = serde_json::to_string(&subscription(
                    k,
                    broker_core::pair::pair_to_symbol(&Self::EXCHANGE, &pair)
                        .unwrap()
                        .as_ref(),
                ))
                .unwrap();
                match w.write(Message::Binary(result.into())) {
                    Ok(_) => {}
                    Err(_) => self.metrics.subscription_failure(pair.as_ref(), k.as_ref()),
                }
            }
        }
    }
}

impl StreamingApi for BitstampStreamingApi {
    const NAME: &'static str = "bitstamp";
    const EXCHANGE: Exchange = Exchange::Bitstamp;
}
