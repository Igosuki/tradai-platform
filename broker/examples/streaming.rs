// This example shows how to use the streaming API provided by Broker.
// The example is complete and shows how to forward data stream events to actor recipients
#![allow(clippy::must_use_candidate)]
#![allow(
    clippy::missing_panics_doc,
    clippy::missing_errors_doc,
    clippy::wildcard_imports,
    clippy::enum_glob_use
)]

#[macro_use]
extern crate tokio;

use actix::{Actor, Context, Handler};
use broker_core::exchange::Exchange::Binance;
use broker_core::types::{MarketChannel, MarketChannelType, SecurityType, Symbol};
use std::sync::Arc;

use brokers::broker::{ActixMessageBroker, Broker, MarketEventEnvelopeRef, Subject};
use brokers::credential::BasicCredentials;
use brokers::exchange::Exchange;
use brokers::settings::*;

use brokers::Brokerages;

// This is the channel type, used by the broker to register recipients for events and broadcast messages
#[derive(PartialEq, Eq, Hash, Debug, Copy, Clone)]
pub enum ExchangeChannel {
    No,
}

impl Subject<MarketEventEnvelopeRef> for ExchangeChannel {}

impl From<MarketEventEnvelopeRef> for ExchangeChannel {
    fn from(_: MarketEventEnvelopeRef) -> Self { Self::No }
}

struct LoggingActor;

impl Actor for LoggingActor {
    type Context = Context<Self>;
}

impl Handler<MarketEventEnvelopeRef> for LoggingActor {
    type Result = Result<(), anyhow::Error>;

    fn handle(&mut self, msg: MarketEventEnvelopeRef, _ctx: &mut Self::Context) -> Self::Result {
        println!("Logging {msg:?}");
        Ok(())
    }
}

#[actix::main]
async fn main() {
    env_logger::init();
    async {
        let my_creds = Box::new(BasicCredentials::empty(Exchange::Binance));
        let market_chan = vec![MarketChannel {
            symbol: Symbol::new("BTC_USDT".into(), SecurityType::Crypto, Exchange::Binance),
            r#type: MarketChannelType::Trades,
            tick_rate: None,
            resolution: None,
            only_final: None,
            orderbook: None,
        }];
        let settings = BrokerSettings {
            fees: 0.01,
            use_account: false,
            use_margin_account: false,
            use_isolated_margin_account: false,
            isolated_margin_account_pairs: vec![],
            use_test: false,
            market_channels: vec![],
        };

        // Initialize the broker and a simple logging actor
        let mut broker = ActixMessageBroker::<ExchangeChannel, MarketEventEnvelopeRef>::new();
        let actor = LoggingActor::create(|_| LoggingActor);
        broker.register(ExchangeChannel::No, actor.recipient());
        let broker = Arc::new(broker);

        // Initialize broker
        let apis = Brokerages::public_apis(&[Binance]).await;
        let _ = Brokerages::load_pair_registries(&apis).await;
        let mut bot = Brokerages::new_market_bot(Binance, my_creds, settings, market_chan.as_slice())
            .await
            .expect("stream connected");

        // Read from the stream
        select! {
            _ = bot.add_sink(Box::new(move |msg| {broker.broadcast(msg); Ok(()) })) => {
                println!("broker stream closed on its own");
            }
            _ = tokio::signal::ctrl_c() => {
                println!("broker stream interrupted, closing...")
            }
        }
    }
    .await;
}
