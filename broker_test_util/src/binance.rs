use std::io;
use std::sync::Arc;

use actix_http::ws;
use bytestring::ByteString;
use futures::Future;
use httpmock::MockServer;

use binance::config::Config;
use brokers::api::Brokerage;
use brokers::broker_binance::BinanceApi;
use brokers::credential::{BasicCredentials, Credentials};
use brokers::exchange::Exchange;

type WSResponse = impl Future<Output = Result<ws::Message, io::Error>>;

type WSEndpoint = impl Fn(ws::Frame) -> WSResponse + Clone;

pub fn account_ws() -> Box<WSEndpoint> {
    let _responses_vec = vec!["hello".to_string()];
    // Create a channel to receive the events.
    let closure = async move |req: ws::Frame| unsafe {
        // let input = input.to_owned();
        // let msg = input.recv().unwrap();
        let result: Result<ws::Message, io::Error> = match req {
            ws::Frame::Ping(msg) => Ok(ws::Message::Pong(msg)),
            ws::Frame::Text(text) => Ok(ws::Message::Text(ByteString::from_bytes_unchecked(text))),
            ws::Frame::Binary(bin) => Ok(ws::Message::Binary(bin)),
            ws::Frame::Close(reason) => Ok(ws::Message::Close(reason)),
            _ => Ok(ws::Message::Close(None)),
        };
        result
    };
    Box::new(closure)
}

/// # Panics
///
/// If the api cannot be created
pub async fn local_api() -> (MockServer, Arc<dyn Brokerage>) {
    let server = MockServer::start();
    let creds: Box<dyn Credentials> = Box::new(BasicCredentials::empty(Exchange::Binance));
    let mock_server_address = server.address().to_string();
    let conf = Config::default()
        .set_rest_api_endpoint(&format!("http://{}", mock_server_address))
        .set_ws_endpoint(&format!("ws://{}", mock_server_address));
    let api = BinanceApi::new_with_config(creds.as_ref(), conf).await.unwrap();
    (server, Arc::new(api))
}
