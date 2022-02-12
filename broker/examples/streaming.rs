// This example shows how to use the streaming API provided by Coinnect.
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
use std::sync::Arc;

use brokers::broker::{ActixMessageBroker, Broker, MarketEventEnvelopeMsg, Subject};
use brokers::credential::BasicCredentials;
use brokers::exchange::Exchange;
use brokers::settings::*;

use brokers::Brokerages;

// This is the channel type, used by the broker to register recipients for events and broadcast messages
#[derive(PartialEq, Eq, Hash, Debug, Copy, Clone)]
pub enum ExchangeChannel {
    No,
}

impl Subject<MarketEventEnvelopeMsg> for ExchangeChannel {}

impl From<MarketEventEnvelopeMsg> for ExchangeChannel {
    fn from(_: MarketEventEnvelopeMsg) -> Self { Self::No }
}

struct LoggingActor;

impl Actor for LoggingActor {
    type Context = Context<Self>;
}

impl Handler<MarketEventEnvelopeMsg> for LoggingActor {
    type Result = Result<(), anyhow::Error>;

    fn handle(&mut self, msg: MarketEventEnvelopeMsg, _ctx: &mut Self::Context) -> Self::Result {
        println!("Logging {msg:?}");
        Ok(())
    }
}

#[actix::main]
async fn main() {
    env_logger::init();
    async {
        let my_creds = Box::new(BasicCredentials::empty(Exchange::Binance));
        let settings = ExchangeSettings {
            orderbook: None,
            trades: Some(TradesSettings {
                symbols: vec!["BTC_USDT".to_string()],
            }),
            fees: 0.01,
            use_account: false,
            use_margin_account: false,
            use_isolated_margin_account: false,
            isolated_margin_account_pairs: vec![],
            use_test: false,
            orderbook_depth: Some(5),
        };

        // Initialize the broker and a simple logging actor
        let mut broker = ActixMessageBroker::<ExchangeChannel, MarketEventEnvelopeMsg>::new();
        let actor = LoggingActor::create(|_| LoggingActor);
        broker.register(ExchangeChannel::No, actor.recipient());
        let broker = Arc::new(broker);

        // Initialize coinnect
        let apis = Brokerages::public_apis(&[Binance]).await;
        let _ = Brokerages::load_pair_registries(&apis).await;
        let mut bot = Brokerages::new_market_bot(Binance, my_creds, settings)
            .await
            .expect("stream connected");

        // Read from the stream
        select! {
            _ = bot.add_sink(Box::new(move |msg| {broker.broadcast(msg); Ok(()) })) => {
                println!("coinnect stream closed on its own");
            }
            _ = tokio::signal::ctrl_c() => {
                println!("coinnect stream interrupted, closing...")
            }
        }
    }
    .await;
}
