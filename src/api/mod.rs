use serde::{Serialize, Deserialize};
use actix_web::{web, Error, HttpResponse, ResponseError};
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use std::collections::HashMap;
use coinnect_rt::types::{OrderType, Pair, Price, Volume};

use crate::api::ApiError::ExchangeNotFound;
use derive_more::Display;
use coinnect_rt::bitstamp::BitstampCreds;
use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::bittrex::BittrexCreds;
use std::path::PathBuf;

use futures::lock::Mutex;
use std::fs::File;
use std::ops::Try;

pub struct ExchangeConfig {
    key_file: String,
    exchanges: Vec<Exchange>
}

fn build_exchanges(cfg: ExchangeConfig) -> HashMap<Exchange, Box<dyn ExchangeApi>> {
    let path = PathBuf::from(cfg.key_file.as_str());
    let mut exchg_map : HashMap<Exchange, Box<dyn ExchangeApi>> = HashMap::new();
    for exchange in cfg.exchanges {
        match exchange {
            Exchange::Bitstamp => {
                let my_creds = BitstampCreds::new_from_file("account_bitstamp", path.clone()).unwrap();
                exchg_map.insert(exchange, Coinnect::new(exchange, Box::new(my_creds)).unwrap());
            },
            Exchange::Bittrex => {
                let my_creds = BittrexCreds::new_from_file("account_bittrex", path.clone()).unwrap();
                exchg_map.insert(exchange, Coinnect::new(exchange, Box::new(my_creds)).unwrap());
            },
            _ => ()
        }
    }
    exchg_map
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    exchg: Exchange,
    #[serde(rename(serialize = "type", deserialize = "type"))]
    t: OrderType,
    pair: Pair,
    qty: Volume,
    price: Price,
}

#[derive(Debug, Display)]
pub enum ApiError {
    #[display(fmt = "Exchange not found")]
    ExchangeNotFound(Exchange),
    Coinnect(coinnect_rt::error::Error),
    IoError(std::io::Error)
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ExchangeNotFound(_e) => HttpResponse::NotFound().finish(),
            ApiError::Coinnect(e) => HttpResponse::InternalServerError().body(e.to_string()),
            ApiError::IoError(e) => HttpResponse::InternalServerError().body(e.to_string()),
            _ => HttpResponse::InternalServerError().finish()
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        ApiError::IoError(e)
    }
}

pub async fn add_order(
    query: web::Json<Order>,
    exchanges: web::Data<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>>,
) -> Result<HttpResponse, Error> {
    let order = query.0;
    let mut x = exchanges.lock().await;
    let api = x.get_mut(&order.exchg).ok_or(ExchangeNotFound(order.exchg))?;
    let _resp = api.add_order(order.t, order.pair, order.qty.with_prec(8), Some(order.price.with_prec(2))).await.map_err(|e| ApiError::Coinnect(e))?;
    Ok(HttpResponse::Ok().finish())
}

pub fn dump_profiler_file(name: Option<&String>) -> Result<(), std::io::Error>{
    let string = format!("flame-graph-{}.html", chrono::Utc::now());
    let graph_file_name = name.unwrap_or(&string);
    let graph_file = &mut File::create(graph_file_name)?;
    flame::dump_html(graph_file)
}

#[cfg(feature = "flame_it")]
pub async fn start_profiler() -> Result<HttpResponse, Error> {
    flame::start("main bot");
    Ok(HttpResponse::Ok().finish())
}

#[cfg(feature = "flame_it")]
pub async fn end_profiler() -> Result<HttpResponse, Error> {
    flame::end("main bot");
    dump_profiler_file(None)?;
    Ok(HttpResponse::Ok().finish())
}

#[cfg(feature = "flame_it")]
pub async fn dump_profiler(q: web::Query<HashMap<String, String>>) -> Result<HttpResponse, Error> {
    dump_profiler_file(q.get("f"))?;
    Ok(HttpResponse::Ok().finish())
}


pub fn config_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/orders")
            .service(
                web::resource("")
                    .route(web::post().to(add_order)),
            ),
    );
    #[cfg(feature = "flame_it")]
    cfg.service(
        web::scope("/profiling")
            .service(
                web::resource("start")
                    .route(web::post().to(start_profiler)),
            )
            .service(
                web::resource("end")
                    .route(web::post().to(end_profiler)),
            )
            .service(
                web::resource("dump")
                    .route(web::post().to(dump_profiler)),
            )
    );

}

#[cfg(test)]
mod tests {
    use crate::api::config_app;

    use actix_web::{
        http::{header, StatusCode},
        test, App,
    };
    use coinnect_rt::exchange::Exchange::Bitstamp;
    use coinnect_rt::types::OrderType;
    use coinnect_rt::types::Pair::BTC_USD;
    use bigdecimal::BigDecimal;

    use futures::lock::Mutex;


    #[actix_rt::test]
    async fn test_add_order() {
        let exchanges = crate::api::ExchangeConfig {
            key_file: "keys_real_test.json".to_string(),
            exchanges: vec![Bitstamp]
        };
        let data = Mutex::new(crate::api::build_exchanges(exchanges));
        let mut app = test::init_service(App::new().data(data).configure(config_app)).await;

        let _o = crate::api::Order {exchg:Bitstamp, t: OrderType::SellLimit, pair: BTC_USD, qty: BigDecimal::from(0.000001), price: BigDecimal::from(1)};
        let payload = r#"{"exchg":"Bitstamp","type":"SellLimit","pair":"BTC_USD", "qty": 0.0000001, "price": 0.01}"#.as_bytes();

        let req = test::TestRequest::post()
            .uri("/orders")
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload)
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        let status = resp.status();
        let body = test::read_body(resp).await;
        assert_eq!(status, StatusCode::OK, "status : {}, body: {:?}", status, body);
    }
}
