use std::fmt::Debug;

use futures::{Sink, StreamExt};
use tokio::sync::broadcast;
use tokio_stream::wrappers::errors::BroadcastStreamRecvError;
use tokio_stream::wrappers::BroadcastStream;

use strategy::EventLogger;

#[derive(Debug)]
pub struct StreamWriterLogger<T> {
    events_tx: broadcast::Sender<T>,
}

impl<T: Clone> Default for StreamWriterLogger<T> {
    fn default() -> Self {
        let (events_tx, _events_rx) = broadcast::channel(100);
        Self { events_tx }
    }
}

impl<T: 'static + Clone + Send> StreamWriterLogger<T> {
    /// Broadcasts logs to all its subscribers
    pub fn new() -> Self { Self::default() }

    /// Return a new stream subscription
    pub fn subscription(&self) -> BroadcastStream<T> { BroadcastStream::new(self.events_tx.subscribe()) }

    /// Subscribe a sink to this stream writer's stream
    #[allow(dead_code)]
    pub async fn subscribe<S>(&self, sink: S)
    where
        S: Sink<T, Error = BroadcastStreamRecvError>,
    {
        let stream = BroadcastStream::new(self.events_tx.subscribe());
        stream.forward(sink).await.unwrap();
    }
}

#[async_trait]
impl<T: Debug + Send> EventLogger<T> for StreamWriterLogger<T> {
    async fn log(&self, event: T) { self.events_tx.send(event).unwrap(); }
}
