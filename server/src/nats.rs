use actix::{Actor, Context, Handler, Message, Recipient};
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe};
use nats::Connection;
use serde::de::DeserializeOwned;
use std::sync::Arc;
use strategies::Channel;

type Result<T> = anyhow::Result<T>;

fn nats_conn(nats_host: &str, username: &str, password: &str) -> Result<Connection> {
    let nats_connection = nats::Options::with_user_pass(username, password)
        .with_name("bitcoins_feeder")
        .connect(nats_host)?;
    Ok(nats_connection)
}

pub trait Subject {
    fn subject(&self) -> String;

    fn glob() -> String;

    fn from_channel(channel: &Channel) -> String;
}

impl Subject for LiveEventEnveloppe {
    fn subject(&self) -> String {
        format!("live_event.{}.{}", self.xch.to_string(), match &self.e {
            LiveEvent::LiveOrder(lo) => format!("{}.orders", lo.pair),
            LiveEvent::LiveTrade(lt) => format!("{}.trades", lt.pair),
            LiveEvent::LiveOrderbook(ob) => format!("{}.obs", ob.pair),
            _ => "noop".to_string(),
        })
    }

    fn glob() -> String { "live_event.>".to_string() }

    fn from_channel(channel: &Channel) -> String {
        match channel {
            Channel::Orderbooks { xch, pair } => format!("live_event.{}.{}.obs", xch.to_string(), pair),
            Channel::Orders { xch, pair } => format!("live_event.{}.{}.orders", xch.to_string(), pair),
            Channel::Trades { xch, pair } => format!("live_event.{}.{}.trades", xch.to_string(), pair),
            _ => "noop".to_string(),
        }
    }
}

pub struct NatsProducer {
    nats_conn: Connection,
}

impl NatsProducer {
    pub fn new(nats_host: &str, username: &str, password: &str) -> Result<Self> {
        let nats_connection = nats_conn(nats_host, username, password)?;
        Ok(NatsProducer {
            nats_conn: nats_connection,
        })
    }
}

impl Actor for NatsProducer {
    type Context = Context<Self>;
}

impl Handler<LiveEventEnveloppe> for NatsProducer {
    type Result = <LiveEventEnveloppe as Message>::Result;

    fn handle(&mut self, msg: LiveEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        let string = serde_json::to_string(&msg).unwrap();
        self.nats_conn.publish(&msg.subject(), string)?;
        Ok(())
    }
}

pub struct NatsConsumer {
    nats_conn: Connection,
}

impl NatsConsumer {
    pub fn new<T: 'static>(
        nats_host: &str,
        username: &str,
        password: &str,
        topics: Vec<String>,
        recipients: Vec<Recipient<T>>,
    ) -> Result<Self>
    where
        T: DeserializeOwned + Message + Send + Clone,
        <T as Message>::Result: Send,
    {
        let connection = nats_conn(nats_host, username, password)?;
        for topic in topics {
            let recipients = Arc::new(recipients.clone());
            connection.subscribe(&topic)?.with_handler(move |msg| {
                let v: T = serde_json::from_slice(msg.data.as_slice())?;
                for recipient in recipients.as_ref() {
                    recipient.do_send(v.clone()).unwrap();
                }
                Ok(())
            });
        }
        Ok(Self { nats_conn: connection })
    }
}

impl Actor for NatsConsumer {
    type Context = Context<Self>;
}
