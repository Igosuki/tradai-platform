use std::sync::Arc;
use std::time::Duration;

use actix::io::SinkWrite;
use actix_http::ws::Message;
use async_trait::async_trait;
use binance::api::Binance;
use binance::config::Config;
use binance::margin::Margin;
use binance::rest_model::Success;
use binance::userstream::UserStream;
use binance::ws_model::WebsocketEvent;
use broker_core::account_metrics::AccountMetrics;
use broker_core::bot::{BotWrapper, DefaultWsActor, WsFramedSink, WsHandler};
use bytes::Bytes;
use tokio::sync::mpsc::UnboundedSender;
use tokio_stream::wrappers::UnboundedReceiverStream;
use url::Url;

use crate::adapters::{from_binance_account_event, from_binance_error};
use broker_core::error::*;
use broker_core::json_util::deserialize_json_s;
use broker_core::prelude::*;

#[derive(Clone)]
pub struct BinanceStreamingAccountApi {
    sink: UnboundedSender<AccountEventEnveloppe>,
    user_stream: UserStream,
    margin_stream: Margin,
    listen_key: Option<String>,
    //api: Arc<BinanceApi>,
    metrics: Arc<AccountMetrics>,
    pub account_type: AccountType,
}

impl BinanceStreamingAccountApi {
    /// Create a new binance exchange bot, unavailable channels and currencies are ignored
    pub async fn new_bot(
        creds: Box<dyn Credentials>,
        use_test: bool,
        account_type: AccountType,
    ) -> Result<BotWrapper<DefaultWsActor, UnboundedReceiverStream<AccountEventEnveloppe>>> {
        let metrics = AccountMetrics::for_exchange(Exchange::Binance);
        let api_key = creds.get("api_key");
        let api_secret = creds.get("api_secret");
        let config = if use_test { Config::testnet() } else { Config::default() };
        let stream = Binance::new_with_config(api_key.clone(), api_secret.clone(), &config);
        let margin_stream = Binance::new_with_config(api_key, api_secret, &config);
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let mut api = BinanceStreamingAccountApi {
            sink: tx,
            user_stream: stream,
            margin_stream,
            listen_key: None,
            //api: Arc::new(BinanceApi::new(Box::new(*creds)).unwrap()),
            metrics: Arc::new(metrics),
            account_type: account_type.clone(),
        };
        let listen_key = api.listen_key().await?;
        let ws_url = match account_type {
            AccountType::Spot | AccountType::Margin | AccountType::IsolatedMargin(_) => config.ws_endpoint.as_ref(),
            AccountType::CoinFutures | AccountType::UsdtFutures => config.futures_ws_endpoint.as_ref(),
        };

        let mut url = Url::parse(ws_url)?;
        url.path_segments_mut()
            .map_err(|_| Error::ParseUrl(url::ParseError::RelativeUrlWithoutBase))?
            .push(binance::websockets::WS_ENDPOINT)
            .push(&listen_key);
        info!("Binance connecting with the following key : {}", &listen_key);
        api.listen_key = Some(listen_key.clone());
        let addr = DefaultWsActor::new(
            "BinanceAccountStream",
            url,
            Some(Duration::from_secs(30)),
            Some(Duration::from_secs(60)),
            Arc::new(api),
        )
        .await?;

        Ok(BotWrapper::new(addr, UnboundedReceiverStream::new(rx)))
    }

    async fn listen_key(&self) -> Result<String> {
        let answer = match self.account_type {
            AccountType::Spot => self.user_stream.start().await.map_err(from_binance_error)?,
            AccountType::Margin => self.margin_stream.start().await.map_err(from_binance_error)?,
            AccountType::IsolatedMargin(ref pair) => self
                .margin_stream
                .start_isolated(pair)
                .await
                .map_err(from_binance_error)?,
            _ => return Err(Error::UnsupportedAccountType),
        };
        Ok(answer.listen_key)
    }

    async fn keep_alive(&self) -> Result<Success> {
        let listen_key = &self.listen_key.clone().unwrap();
        let keep_alive = match self.account_type {
            AccountType::Spot => self.user_stream.keep_alive(listen_key).await,
            AccountType::Margin => self.margin_stream.keep_alive(listen_key).await,
            AccountType::IsolatedMargin(ref pair) => self.margin_stream.keep_alive_isolated(listen_key, pair).await,
            _ => return Err(Error::UnsupportedAccountType),
        };
        match keep_alive {
            Err(e @ binance::errors::Error::InvalidListenKey(_)) => {
                //self.listen_key = self.listen_key().await.ok();
                Err(Error::ExchangeError(format!("{:?}", e)))
            }
            Ok(s) => Ok(s),
            _ => Ok(Success {}),
        }
    }
}

#[async_trait(?Send)]
impl WsHandler for BinanceStreamingAccountApi {
    #[cfg_attr(feature = "flame", flame)]
    fn handle_in(&self, _w: &mut SinkWrite<Message, WsFramedSink>, msg: Bytes) {
        match deserialize_json_s::<WebsocketEvent>(msg.as_ref()) {
            Err(err) => {
                debug!(err = ?err, msg = ?msg, "binance stream deserialization error");
            }
            Ok(we) => {
                debug!("{:?}", &we);
                let ae: AccountEvent = from_binance_account_event(we);
                if !matches!(ae, AccountEvent::Noop) {
                    self.broadcast(&ae);
                }
            }
        }
    }

    fn handle_started(&self, _w: &mut SinkWrite<Message, WsFramedSink>) {
        // Do nothing, connections handled in combined stream
        self.metrics.stream_reconnected();
    }

    async fn handle_closed(&self) {}

    async fn handle_keep_alive(&self) -> Result<()> {
        self.keep_alive().await?;
        Ok(())
    }
}

impl BinanceStreamingAccountApi {
    fn broadcast(&self, v: &AccountEvent) {
        if self
            .sink
            .send(AccountEventEnveloppe {
                xchg: Exchange::Binance,
                event: v.clone(),
                account_type: self.account_type.clone(),
            })
            .is_err()
        {
            self.metrics.send_error();
        }
    }
}
