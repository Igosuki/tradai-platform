use broker_core::broker::{AsyncBroker, Broker, ChannelMessageBroker, Subject};
use broker_core::exchange::Exchange;
use broker_core::types::{MarketEvent, MarketEventEnvelope, Pair, SecurityType, Symbol, Trade, TradeType};
use criterion::{criterion_group, criterion_main, Criterion};
use std::sync::Arc;
use tokio::runtime::Runtime;
use tokio::sync::mpsc::Sender;

#[derive(Debug, PartialEq, Eq, Hash)]
struct TestSubject {
    name: &'static str,
}

impl Subject<Arc<MarketEventEnvelope>> for TestSubject {}

impl From<Arc<MarketEventEnvelope>> for TestSubject {
    fn from(_: Arc<MarketEventEnvelope>) -> Self { Self { name: "subject" } }
}

impl Subject<MarketEventEnvelope> for TestSubject {}

impl From<MarketEventEnvelope> for TestSubject {
    fn from(_: MarketEventEnvelope) -> Self { Self { name: "subject" } }
}

fn spawn_channels<M: 'static + Send + Sync>(tokio_rt: &Runtime, channels: usize) -> Vec<Sender<M>> {
    let mut senders = vec![];
    for _ in 0..10 {
        let (sender, mut receiver) = tokio::sync::mpsc::channel::<M>(channels);
        senders.push(sender);
        tokio_rt.spawn(async move {
            #[allow(unused_variables)]
            let mut count = 0;
            loop {
                if receiver.recv().await.is_some() {
                    count += 1;
                }
            }
        });
    }
    senders
}

fn default_event() -> MarketEventEnvelope {
    let pair: Pair = "BTC_USDT".into();
    MarketEventEnvelope::new(
        Symbol::new(pair.clone(), SecurityType::Crypto, Exchange::default()),
        MarketEvent::Trade(Trade {
            event_ms: 0,
            pair,
            amount: 100.0,
            price: 1000.0,
            tt: TradeType::default(),
        }),
    )
}

/// # Panics
///
/// If the tokio runtime cannot be acquired
pub fn criterion_benchmark_arc(c: &mut Criterion) {
    let num_receivers = 10000;
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let mut broker = ChannelMessageBroker::new();
    let senders = spawn_channels(&tokio_rt, num_receivers);
    for sender in senders {
        Broker::register(&mut broker, TestSubject { name: "subject" }, sender.clone());
    }
    c.bench_function("async broker with Arc<M>", |b| {
        b.to_async(&tokio_rt)
            .iter(|| AsyncBroker::broadcast(&broker, Arc::new(default_event())));
    });
    c.bench_function("sync broker with BoxFuture with Arc<M>", |b| {
        b.to_async(&tokio_rt)
            .iter(|| Broker::broadcast(&broker, Arc::new(default_event())));
    });
}

/// # Panics
///
/// If the tokio runtime cannot be acquired
pub fn criterion_benchmark_no_arc(c: &mut Criterion) {
    let num_receivers = 10000;
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    let mut broker = ChannelMessageBroker::new();
    let senders = spawn_channels(&tokio_rt, num_receivers);
    for sender in senders {
        Broker::register(&mut broker, TestSubject { name: "subject" }, sender.clone());
    }
    c.bench_function("async broker with M + Clone", |b| {
        b.to_async(&tokio_rt)
            .iter(|| AsyncBroker::broadcast(&broker, default_event()));
    });
    c.bench_function("sync broker with BoxFuture with M + Clone", |b| {
        b.to_async(&tokio_rt)
            .iter(|| Broker::broadcast(&broker, default_event()));
    });
}

criterion_group!(benches, criterion_benchmark_arc, criterion_benchmark_no_arc);
criterion_main!(benches);
