use actix::{Actor, Context, Handler, Message};
use coinnect_rt::types::{LiveEvent, LiveEventEnveloppe};
use nats::Connection;

type Result<T> = anyhow::Result<T>;

fn nats_conn(nats_host: &str, username: &str, password: &str) -> Result<Connection> {
    let nats_connection = nats::Options::with_user_pass(username, password)
        .with_name("bitcoins_feeder")
        .connect(nats_host)?;
    Ok(nats_connection)
}

pub fn subject(msg: &LiveEventEnveloppe) -> String {
    format!("live_event.{}.{}", msg.0.to_string(), match msg.1 {
        LiveEvent::LiveOrder(_) => "live_orders",
        _ => "live",
    })
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
        let string = serde_json::to_string(&msg.1).unwrap();
        self.nats_conn.publish(&subject(&msg), string)?;
        Ok(())
    }
}

// pub struct NatsConsumer {
//     nats_conn: Connection,
// }
//
// impl NatsConsumer {
//     pub fn new<T>(
//         nats_host: &str,
//         username: &str,
//         password: &str,
//         topics: Vec<String>,
//         sender: std::sync::mpsc::Sender<T>,
//     ) -> Result<Self>
//     where
//         T: DeserializeOwned,
//     {
//         let connection = nats_conn(nats_host, username, password)?;
//         for topic in topics {
//             connection.subscribe(&topic)?.with_handler(|msg| {
//                 let v: T = serde_json::from_slice(msg.data.as_slice()).unwrap();
//                 sender.send(v);
//                 Ok(())
//             });
//         }
//         Ok(Self { nats_conn: connection })
//     }
// }
