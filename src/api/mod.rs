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
                exchg_map.insert(exchange, Coinnect::new(exchange, my_creds).unwrap());
            },
            Exchange::Bittrex => {
                let my_creds = BittrexCreds::new_from_file("account_bittrex", path.clone()).unwrap();
                exchg_map.insert(exchange, Coinnect::new(exchange, my_creds).unwrap());
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
    Coinnect(coinnect_rt::error::Error)
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ExchangeNotFound(_e) => HttpResponse::NotFound().finish(),
            ApiError::Coinnect(e) => HttpResponse::InternalServerError().body(e.to_string()),
            _ => HttpResponse::InternalServerError().finish()
        }
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

pub fn config_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::scope("/orders")
            .service(
                web::resource("")
                    .route(web::post().to(add_order)),
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
            key_file: "keys_real.json".to_string(),
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
