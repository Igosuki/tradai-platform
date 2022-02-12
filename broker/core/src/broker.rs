use std::fmt::{Debug, Formatter};
use std::hash::Hash;
use std::sync::Arc;

use actix::Recipient;
use futures::future::BoxFuture;
use futures::{stream, StreamExt};
use multimap::MultiMap;
use prometheus::IntCounter;
use tokio::sync::mpsc::{Sender, UnboundedSender};

use crate::types::MarketEventEnvelope;

pub type MarketEventEnvelopeMsg = Arc<MarketEventEnvelope>;

pub type MarketEventBroker<C> = ActixMessageBroker<C, MarketEventEnvelopeMsg>;

pub trait Subject<M>: 'static + Eq + Hash + Sync + Send + From<M> {}

//TODO: just split the broadcast method between a SyncBroker and AsyncBroker
/// Broker for Recipients R of Messages M received over Subject S
pub trait Broker<'a, S: 'a, M, R>
where
    S: From<M>,
{
    type BroadcastResult;
    type Iter: Iterator<Item = &'a S>;

    /// Broadcast a message for the subject
    fn broadcast(&self, msg: M) -> Self::BroadcastResult;
    /// Register a recipient for the subject, multiple recipients can be registered
    fn register(&mut self, subject: S, recipient: R);
    /// Return all registered subjects
    fn subjects(&'a self) -> Self::Iter;
}

/// Broker for Recipients R of Messages M received over Subject S
#[async_trait]
pub trait AsyncBroker<'a, S: 'a, M, R>
where
    S: From<M>,
{
    type BroadcastResult;
    type Iter: Iterator<Item = &'a S>;

    /// Broadcast a message for the subject
    async fn broadcast(&self, msg: M) -> Self::BroadcastResult;
    /// Register a recipient for the subject, multiple recipients can be registered
    async fn register(&mut self, subject: S, recipient: R);
    /// Return all registered subjects
    fn subjects(&'a self) -> Self::Iter;
}

lazy_static! {
    static ref BROADCAST_FAILURE_COUNTER: IntCounter = register_int_counter!(
        "broadcast_failures",
        "Total number of times the stream failed to broadcast a live event.",
    )
    .unwrap();
}

pub struct ActixMessageBroker<S, M: actix::Message + Send>
where
    <M as actix::Message>::Result: std::marker::Send,
{
    registry: MultiMap<S, Recipient<M>>,
}

impl<S, M: actix::Message + Send> Debug for ActixMessageBroker<S, M>
where
    <M as actix::Message>::Result: std::marker::Send,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "ActixMessageBroker") }
}

impl<S: Subject<M>, M: actix::Message + Send> Default for ActixMessageBroker<S, M>
where
    <M as actix::Message>::Result: std::marker::Send,
{
    fn default() -> Self {
        Self {
            registry: MultiMap::new(),
        }
    }
}

impl<S, M> ActixMessageBroker<S, M>
where
    S: Subject<M>,
    M: actix::Message + Send + Clone + Debug,
    <M as actix::Message>::Result: std::marker::Send,
{
    pub fn new() -> Self { Self::default() }
}

#[async_trait]
impl<'a, S, M> Broker<'a, S, M, Recipient<M>> for ActixMessageBroker<S, M>
where
    S: Subject<M>,
    M: Clone + actix::Message + Send + Debug,
    <M as actix::Message>::Result: Send,
{
    type BroadcastResult = ();
    type Iter = impl Iterator<Item = &'a S>;

    fn broadcast(&self, msg: M) {
        let subject: S = msg.clone().into();
        if let Some(recipients) = self.registry.get_vec(&subject) {
            for recipient in recipients {
                if recipient.do_send(msg.clone()).is_err() {
                    BROADCAST_FAILURE_COUNTER.inc();
                }
            }
        } else {
            trace!("{:?}", msg);
        }
    }

    fn register(&mut self, subject: S, recipient: Recipient<M>) { self.registry.insert(subject, recipient); }

    fn subjects(&'a self) -> Self::Iter { self.registry.keys() }
}

pub struct UnboundedChannelMessageBroker<S, M>
where
    M: Send,
{
    registry: MultiMap<S, UnboundedSender<M>>,
}

impl<S, M: Send> Debug for UnboundedChannelMessageBroker<S, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "UnboundedChannelMessageBroker") }
}

impl<S: Subject<M>, M: Send> Default for UnboundedChannelMessageBroker<S, M> {
    fn default() -> Self {
        Self {
            registry: MultiMap::new(),
        }
    }
}

impl<S: Subject<M>, M: Send> UnboundedChannelMessageBroker<S, M> {
    pub fn new() -> Self { Self::default() }
}

impl<'a, S, M: 'static> Broker<'a, S, M, UnboundedSender<M>> for UnboundedChannelMessageBroker<S, M>
where
    S: Subject<M>,
    M: Clone + Send + Debug + Sync,
{
    type BroadcastResult = ();
    type Iter = impl Iterator<Item = &'a S>;

    fn broadcast(&self, msg: M) -> Self::BroadcastResult {
        let subject: S = msg.clone().into();
        if let Some(recipients) = self.registry.get_vec(&subject) {
            for recipient in recipients {
                if recipient.send(msg.clone()).is_err() {
                    BROADCAST_FAILURE_COUNTER.inc();
                }
            }
        } else {
            trace!("{:?}", msg);
        }
    }

    fn register(&mut self, subject: S, sink: UnboundedSender<M>) { self.registry.insert(subject, sink); }

    fn subjects(&'a self) -> Self::Iter { self.registry.keys() }
}

pub struct ChannelMessageBroker<S, M>
where
    M: Send,
{
    registry: MultiMap<S, Sender<M>>,
}

impl<S, M: Send> Debug for ChannelMessageBroker<S, M> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result { write!(f, "ChannelMessageBroker") }
}

impl<S: Subject<M>, M: Send> Default for ChannelMessageBroker<S, M> {
    fn default() -> Self {
        Self {
            registry: MultiMap::new(),
        }
    }
}

impl<S: Subject<M>, M: Send> ChannelMessageBroker<S, M> {
    pub fn new() -> Self { Self::default() }
}

impl<'a, S, M: 'static> Broker<'a, S, M, Sender<M>> for ChannelMessageBroker<S, M>
where
    S: Subject<M>,
    M: Clone + Send + Debug + Sync,
{
    type BroadcastResult = BoxFuture<'static, ()>;
    type Iter = impl Iterator<Item = &'a S>;

    #[allow(clippy::implicit_clone)]
    fn broadcast(&self, msg: M) -> Self::BroadcastResult {
        let subject: S = msg.clone().into();
        let f: Self::BroadcastResult = if let Some(recipients) = self.registry.get_vec(&subject) {
            Box::pin(stream::iter(recipients.to_owned()).for_each(move |r| {
                let msg = msg.clone();
                async move {
                    if r.send(msg).await.is_err() {
                        BROADCAST_FAILURE_COUNTER.inc();
                    }
                }
            }))
        } else {
            Box::pin(async move {
                trace!("{:?}", msg);
            })
        };
        f
    }

    fn register(&mut self, subject: S, sink: Sender<M>) { self.registry.insert(subject, sink); }

    fn subjects(&'a self) -> Self::Iter { self.registry.keys() }
}

#[async_trait]
impl<'a, S, M: 'static> AsyncBroker<'a, S, M, Sender<M>> for ChannelMessageBroker<S, M>
where
    S: Subject<M>,
    M: Clone + Send + Debug + Sync,
{
    type BroadcastResult = ();
    type Iter = impl Iterator<Item = &'a S>;

    async fn broadcast(&self, msg: M) -> Self::BroadcastResult {
        let subject: S = msg.clone().into();
        if let Some(recipients) = self.registry.get_vec(&subject) {
            for recipient in recipients {
                if recipient.send(msg.clone()).await.is_err() {
                    BROADCAST_FAILURE_COUNTER.inc();
                }
            }
        } else {
            trace!("{:?}", msg);
        }
    }

    async fn register(&mut self, subject: S, sink: Sender<M>) { self.registry.insert(subject, sink); }

    fn subjects(&'a self) -> Self::Iter { self.registry.keys() }
}
