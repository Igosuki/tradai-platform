use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use crate::broker::MarketEventEnvelopeRef;
use actix::{io::SinkWrite, Actor, ActorContext, ActorFutureExt, ActorTryFutureExt, Addr, AsyncContext, Context,
            ContextFutureSpawner, Handler, StreamHandler, Supervisor, WrapFuture};
use actix_codec::Framed;
use async_trait::async_trait;
use awc::{error::WsProtocolError,
          ws::{Codec, Frame, Message},
          BoxedSocket, Client};
use backoff::backoff::Backoff;
use backoff::ExponentialBackoff;
use bytes::Bytes;
use chrono::{DateTime, TimeZone, Utc};
use futures::stream::{SplitSink, StreamExt};
use futures::Stream;
use prometheus::default_registry;
use tokio::time;
use url::Url;

use crate::error::*;
use crate::types::AccountEventEnveloppe;

use super::metrics::{WsCommEvent, WsStreamLifecycleEvent, WsStreamMetrics};

pub type WsFramedSink = SplitSink<Framed<BoxedSocket, Codec>, Message>;

pub struct DefaultWsActor {
    inner: SinkWrite<Message, WsFramedSink>,
    handler: Arc<dyn WsHandler>,
    conn_backoff: ExponentialBackoff,
    pub url: Url,
    pub name: String,
    metrics: WsStreamMetrics,
    stale_after: Option<Duration>,
    last_msg_at: DateTime<Utc>,
}

#[async_trait(?Send)]
pub trait WsHandler {
    /// Handle incoming messages
    fn handle_in(&self, w: &mut SinkWrite<Message, WsFramedSink>, msg: Bytes);
    /// Additional actions after the stream has started
    fn handle_started(&self, w: &mut SinkWrite<Message, WsFramedSink>);
    /// Additional actions to be done upon closing the socket
    async fn handle_closed(&self) {}
    /// An opportunity to make a scheduled action to keep the socket alive
    async fn handle_keep_alive(&self) -> Result<()> { Ok(()) }
}

#[derive(Message)]
#[rtype(result = "()")]
struct ClientCommand(String);

impl Actor for DefaultWsActor {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.metrics.lifecycle_event(WsStreamLifecycleEvent::Started);
        // start heartbeats otherwise server will disconnect after 10 seconds
        self.hb(ctx);
        self.stale_check(ctx);
    }

    fn stopped(&mut self, ctx: &mut Self::Context) {
        info!(name = %self.name, "websocket stopped");
        self.metrics.lifecycle_event(WsStreamLifecycleEvent::Stopped);
        let handler = self.handler.clone();
        async move { handler.handle_closed().await }.into_actor(self).spawn(ctx);
    }
}

impl actix::Supervised for DefaultWsActor {
    fn restarting(&mut self, ctx: &mut <Self as Actor>::Context) {
        let url = self.url.clone();
        let client = new_ws_client(url.to_string());
        info!(name = %self.name, "websocket restarting");
        self.metrics.lifecycle_event(WsStreamLifecycleEvent::Restarting);
        client
            .into_actor(self)
            .map(move |res, act, ctx| match res {
                Ok(client) => {
                    error!(name = %act.name, url = %url, "websocket reconnected ");
                    let (sink, stream) = client.split();
                    DefaultWsActor::add_stream(stream, ctx);
                    act.conn_backoff.reset();
                    act.inner = SinkWrite::new(sink, ctx);
                }
                Err(err) => {
                    error!(name = %act.name, url = %url, err = %err, "websocket failed to connect");
                    // re-connect with backoff time.
                    // we stop current context, supervisor will restart it.
                    if let Some(timeout) = act.conn_backoff.next_backoff() {
                        act.metrics.conn_backoff(timeout.as_secs_f64());
                        ctx.run_later(timeout, |_, ctx| ctx.stop());
                    }
                }
            })
            .wait(ctx);
    }
}

impl DefaultWsActor {
    /// # Errors
    ///
    /// If the websocket connection cannot be established
    pub async fn new(
        name: &'static str,
        wss_url: Url,
        conn_timeout: Option<Duration>,
        stale_after: Option<Duration>,
        handler: Arc<dyn WsHandler>,
    ) -> Result<Addr<DefaultWsActor>> {
        let name = name.to_string();
        let mut conn_backoff = ExponentialBackoff {
            max_elapsed_time: conn_timeout,
            ..ExponentialBackoff::default()
        };

        let c;
        loop {
            match new_ws_client(wss_url.to_string()).await {
                Ok(frames) => {
                    c = Some(frames);
                    break;
                }
                Err(e) => {
                    let option = conn_backoff.next_backoff();
                    if let Some(backoff) = option {
                        info!(
                            "Failed to connect to {}, retrying in {}ms",
                            wss_url,
                            backoff.as_millis()
                        );
                        time::sleep(backoff).await;
                        continue;
                    }
                    return Err(Error::BackoffConnectionTimeout(format!("{}", e)));
                }
            }
        }
        conn_backoff.max_elapsed_time = None;
        conn_backoff.reset();
        let (sink, stream) = c
            .ok_or_else(|| Error::BackoffConnectionTimeout("loop broke without frames".to_string()))?
            .split();
        Ok(Supervisor::start(move |ctx| {
            DefaultWsActor::add_stream(stream, ctx);
            DefaultWsActor {
                inner: SinkWrite::new(sink, ctx),
                handler,
                url: wss_url.clone(),
                conn_backoff,
                name: name.clone(),
                metrics: WsStreamMetrics::for_name(default_registry(), &name),
                stale_after,
                last_msg_at: Utc.timestamp_millis(i64::MAX),
            }
        }))
    }

    /// The websocket will be considered 'stale' after
    fn stale_check(&self, ctx: &mut Context<Self>) {
        if self.stale_after.is_none() {
            return;
        }
        let stale_after = self.stale_after.unwrap();
        ctx.run_later(stale_after, move |act, ctx| {
            act.metrics.stale(
                (act.last_msg_at.timestamp_millis() + stale_after.as_millis() as i64) < Utc::now().timestamp_millis(),
            );
            act.stale_check(ctx);
        });
    }

    fn hb(&self, ctx: &mut Context<Self>) {
        ctx.run_later(Duration::new(25, 0), |act, ctx| {
            if act.inner.write(Message::Ping(Bytes::from_static(b""))).is_err() {
                act.metrics.comm_event(WsCommEvent::ConnClosed);
            }
            act.metrics.comm_event(WsCommEvent::PingSend);
            act.hb(ctx);
            // client should also check for a timeout here, similar to the
            // server code
        });
        let handler = self.handler.clone();
        let keep_alive = async move { handler.handle_keep_alive().await }
            .into_actor(self)
            .map_err(|e, act, _ctx| {
                error!(name = %act.name, "restarting stocket because it failed to stay alive {}", e);
                //ctx.stop();
            })
            .map(|_, _, _| ());

        ctx.spawn(keep_alive);
    }
}

/// Handle stdin commands
impl Handler<ClientCommand> for DefaultWsActor {
    type Result = ();

    fn handle(&mut self, msg: ClientCommand, _ctx: &mut Context<Self>) {
        self.inner.write(Message::Text(msg.0.into())).unwrap();
    }
}

impl Handler<Ping> for DefaultWsActor {
    type Result = ();

    fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self>) {}
}

/// Handle server websocket messages
#[allow(clippy::single_match_else)]
impl StreamHandler<std::result::Result<Frame, WsProtocolError>> for DefaultWsActor {
    fn handle(&mut self, msg: std::result::Result<Frame, WsProtocolError>, _ctx: &mut Context<Self>) {
        self.last_msg_at = Utc::now();
        match msg {
            Ok(Frame::Ping(msg)) => {
                self.metrics.comm_event(WsCommEvent::PingRecv);
                match self.inner.write(Message::Pong(Bytes::copy_from_slice(&msg))) {
                    Ok(_) => {
                        self.metrics.comm_event(WsCommEvent::PongSend);
                    }
                    Err(_) => {
                        self.metrics.comm_event(WsCommEvent::PongSendFail);
                        trace!("Failed to send pong back to ws");
                    }
                }
            }
            Ok(Frame::Text(txt)) => {
                self.metrics.comm_event(WsCommEvent::MsgRecv);
                self.handler.handle_in(&mut self.inner, txt);
            }
            Ok(Frame::Close(reason)) => {
                self.metrics.comm_event(WsCommEvent::CloseRecv);
                info!(
                    name = %self.name, reason = ?reason,
                    "websocket remote server asked for close",
                );
            }
            Ok(Frame::Pong(_msg)) => {
                self.metrics.comm_event(WsCommEvent::PongRecv);
            }
            _ => {
                self.metrics.comm_event(WsCommEvent::Unhandled);
            }
        }
    }

    fn started(&mut self, _ctx: &mut Context<Self>) {
        info!(name = %self.name, "websocket connected");
        self.metrics.lifecycle_event(WsStreamLifecycleEvent::Connected);
        //self.handler.write().unwrap().handle_started(&mut self.inner);
    }

    fn finished(&mut self, ctx: &mut Context<Self>) {
        info!(name = %self.name, "websocket finished");
        self.metrics.lifecycle_event(WsStreamLifecycleEvent::Finished);
        ctx.stop();
    }
}

impl actix::io::WriteHandler<WsProtocolError> for DefaultWsActor {}

#[derive(Message)]
#[rtype(result = "()")]
pub struct Ping;

pub type MarketDataStreamer = dyn DataStreamer<MarketEventEnvelopeRef>;
pub type BrokerageAccountDataStreamer = dyn DataStreamer<AccountEventEnveloppe>;

#[async_trait]
pub trait DataStreamer<E>: Send + Sync {
    /// Returns the address of the exchange actor
    fn is_connected(&self) -> bool;

    fn ping(&self);

    async fn add_sink(&mut self, f: Box<dyn Fn(E) -> std::result::Result<(), Infallible> + Send>);
}

pub struct BotWrapper<T: Actor, S: Stream> {
    addr: Addr<T>,
    pub(crate) stream: Pin<Box<S>>,
}

impl<T: Actor, S: Stream> BotWrapper<T, S> {
    #[must_use]
    pub fn new(addr: Addr<T>, stream: S) -> Self {
        Self {
            addr,
            stream: Box::pin(stream),
        }
    }
}

#[async_trait]
impl<T, S, E> DataStreamer<E> for BotWrapper<T, S>
where
    T: Actor,
    S: Stream<Item = E> + 'static + Send + Sync + Unpin,
    E: Send + 'static,
{
    fn is_connected(&self) -> bool { true }

    fn ping(&self) { assert!(self.addr.connected()) }

    async fn add_sink(&mut self, f: Box<dyn Fn(E) -> std::result::Result<(), Infallible> + Send>) {
        let stream = &mut self.stream;
        stream.map(f).forward(futures::sink::drain()).await.unwrap();
    }
}

pub async fn new_ws_client(url: String) -> Result<Framed<BoxedSocket, Codec>> {
    new_ws_client_version(url, awc::http::Version::HTTP_11).await
}

pub async fn new_ws_client_version(url: String, version: awc::http::Version) -> Result<Framed<BoxedSocket, Codec>> {
    let connector = awc::Connector::new().timeout(Duration::from_secs(1)).limit(200);
    let client = Client::builder()
        .connector(connector)
        .max_http_version(version)
        .finish();

    let result = client.ws(url.as_str()).connect().await;
    let (response, framed) = result.map_err(|e| {
        debug!("Error: {}", e);
        let e: Error = Error::WsError(format!("{}", e));
        e
    })?;
    debug!("WS Client Response {:?}", response);
    Ok(framed)
}
