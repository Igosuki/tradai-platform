use actix::{Actor, Context, Handler, ResponseActFuture, WrapFuture};
use actix_web::http::Uri;
use awc::Client;
use futures::Future;
use serde::Serialize;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

#[derive(actix::Message)]
#[rtype(result = "anyhow::Result<()>")]
pub struct Notification {
    msg: String,
}

#[derive(Debug, serde::Deserialize)]
pub struct DiscordNotifierOptions {
    webhook: String,
}

pub struct DiscordNotifier {
    client: Arc<Client>,
    webhook: Uri,
}

#[derive(Serialize, Default)]
pub struct DiscordMessage {
    pub username: Option<String>,
    pub avatar_url: Option<String>,
    pub content: String,
}

impl DiscordNotifier {
    #[allow(dead_code)]
    fn new(options: &DiscordNotifierOptions) -> Self {
        let ssl = {
            let mut ssl = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
            let _ = ssl.set_alpn_protos(b"\x08http/1.1");
            ssl.build()
        };
        let connector = awc::Connector::new()
            .ssl(ssl)
            .timeout(Duration::from_secs(1))
            .limit(200);
        let client = Client::builder().connector(connector).finish();
        Self {
            client: Arc::new(client),
            webhook: Uri::from_str(&options.webhook).unwrap(),
        }
    }

    fn notify(&self, msg: String) -> impl Future<Output = anyhow::Result<()>> + 'static {
        let client = self.client.clone();
        let webhook = self.webhook.clone();
        async move {
            client
                .post(&webhook)
                .send_json(&DiscordMessage {
                    content: msg,
                    ..DiscordMessage::default()
                })
                .await
                .map(|_| ())
                .map_err(|e| anyhow!("{}", e))
        }
    }
}

impl Handler<Notification> for DiscordNotifier {
    type Result = ResponseActFuture<Self, anyhow::Result<()>>;

    fn handle(&mut self, notification: Notification, _ctx: &mut Self::Context) -> Self::Result {
        Box::pin(self.notify(notification.msg).into_actor(self))
    }
}

impl Actor for DiscordNotifier {
    type Context = Context<Self>;
}
