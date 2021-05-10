#[cfg(test)]
pub mod binance_test_util {
    use std::io;

    use actix_http::ws;
    use coinnect_rt::binance::{BinanceApi, BinanceCreds};
    use coinnect_rt::exchange::ExchangeApi;
    use futures::Future;
    use httpmock::MockServer;
    use bytestring::ByteString;

    // use std::sync::mpsc::Receiver;

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
                ws::Frame::Text(text) => Ok(ws::Message::Text(
                    ByteString::from_bytes_unchecked(text)
                )),
                ws::Frame::Binary(bin) => Ok(ws::Message::Binary(bin)),
                ws::Frame::Close(reason) => Ok(ws::Message::Close(reason)),
                _ => Ok(ws::Message::Close(None)),
            };
            result
        };
        Box::new(closure)
    }

    pub async fn local_api() -> (MockServer, Box<dyn ExchangeApi>) {
        let server = MockServer::start();
        let creds = BinanceCreds::empty();
        let api = BinanceApi::new_with_host(Box::new(creds), Some(format!("{}", server.address())))
            .await
            .unwrap();
        (server, Box::new(api))
    }
}
