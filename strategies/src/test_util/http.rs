#[cfg(test)]
pub mod http {
    use actix_http::body::BodySize;
    use actix_http::error::Error;
    use actix_http::{h1, ws, HttpService, Request, Response};
    use actix_http_test::{test_server, TestServer};
    use futures::future::ok;
    use futures_util::SinkExt;
    use std::io;

    use actix_codec::Framed;
    use futures::Future;
    use std::ops::Deref;

    /*type WebsocketClosure = Box<
        dyn Fn(ws::Frame) -> Box<dyn Future<Output = Result<ws::Message, io::Error>>> + Send + Sync,
    >;*/

    pub async fn ws_it_server<F: Sized, R: Sized>(service: Box<F>) -> TestServer
    where
        F: Fn(ws::Frame) -> R + Send + Sync + Clone + Sized + 'static,
        R: Future<Output = Result<ws::Message, io::Error>> + Sized,
    {
        test_server(move || {
            let service = service.clone();
            HttpService::build()
                .upgrade(move |(req, mut framed): (Request, Framed<_, _>)| {
                    let service = service.clone();
                    async move {
                        let res = ws::handshake_response(req.head()).finish();
                        // send handshake response
                        framed
                            .send(h1::Message::Item((res.drop_body(), BodySize::None)))
                            .await?;

                        // start websocket service
                        let framed = framed.replace_codec(ws::Codec::new());
                        let x = service.clone().deref().to_owned();
                        let service_fn = x.clone();
                        ws::Dispatcher::with(framed, service_fn).await
                    }
                })
                .finish(|_| ok::<_, Error>(Response::not_found()))
                .tcp()
        })
        .await
    }
}
