#[cfg(test)]
pub mod binance_test_util {
    use std::io;

    use actix_http::ws;
    use futures::Future;

    type WSResponse = impl Future<Output = Result<ws::Message, io::Error>>;

    type WSEndpoint = impl Fn(ws::Frame) -> WSResponse;

    fn binance_it_account_ws() -> Box<WSEndpoint> {
        let responses_vec = vec!["hello".to_string()];
        let closure = async move |req: ws::Frame| {
            let result: Result<ws::Message, io::Error> = match req {
                ws::Frame::Ping(msg) => Ok(ws::Message::Pong(msg)),
                ws::Frame::Text(text) => Ok(ws::Message::Text(
                    String::from_utf8(Vec::from(text.as_ref())).unwrap(),
                )),
                ws::Frame::Binary(bin) => Ok(ws::Message::Binary(bin)),
                ws::Frame::Close(reason) => Ok(ws::Message::Close(reason)),
                _ => Ok(ws::Message::Close(None)),
            };
            result
        };
        Box::new(closure)
    }
}
