use std::collections::HashMap;

use crate::api::ApiError::ExchangeNotFound;
use crate::graphql_schemas::root::{Context, Schema};
use actix_web::{web, Error, HttpResponse, ResponseError};
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::{OrderType, Pair, Price, Volume};
use derive_more::Display;
use juniper_actix::{graphiql_handler as gqli_handler, graphql_handler};
use serde::{Deserialize, Serialize};
#[cfg(feature = "flame_it")]
use std::fs::File;
use std::sync::{Arc, Mutex};
use strategies::{Strategy, StrategyKey};

mod playground_source;

use playground_source::playground_source;

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
    IoError(std::io::Error),
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse {
        match self {
            ExchangeNotFound(_e) => HttpResponse::NotFound().finish(),
            ApiError::Coinnect(e) => HttpResponse::InternalServerError().body(e.to_string()),
            ApiError::IoError(e) => HttpResponse::InternalServerError().body(e.to_string()),
            // _ => HttpResponse::InternalServerError().finish()
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self {
        ApiError::IoError(e)
    }
}

async fn graphiql_handler() -> Result<HttpResponse, Error> {
    gqli_handler("/", None).await
}

pub async fn play_handler(
    graphql_endpoint_url: &str,
    subscriptions_endpoint_url: Option<&'static str>,
) -> Result<HttpResponse, Error> {
    let html = playground_source(graphql_endpoint_url, subscriptions_endpoint_url);
    Ok(HttpResponse::Ok()
        .content_type("text/html; charset=utf-8")
        .body(html))
}

async fn playground_handler() -> Result<HttpResponse, Error> {
    play_handler("/", None).await
}

async fn graphql(
    req: actix_web::HttpRequest,
    payload: actix_web::web::Payload,
    schema: web::Data<Schema>,
    strats: web::Data<Arc<HashMap<StrategyKey, Strategy>>>,
    exchanges: web::Data<Arc<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>>>,
) -> Result<HttpResponse, Error> {
    let ctx = Context {
        strats: strats.get_ref().to_owned(),
        exchanges: exchanges.get_ref().to_owned(),
    };
    graphql_handler(&schema, &ctx, req, payload).await
}

#[cfg(feature = "flame_it")]
fn dump_profiler_file(name: Option<&String>) -> Result<(), std::io::Error> {
    let string = format!("flame-graph-{}.html", chrono::Utc::now());
    let graph_file_name = name.unwrap_or(&string);
    info!("Dumping profiler file at {}", graph_file_name);
    let graph_file = &mut File::create(graph_file_name)?;
    flame::dump_html(graph_file)
}

#[cfg(feature = "flame_it")]
pub async fn dump_profiler(q: web::Query<HashMap<String, String>>) -> Result<HttpResponse, Error> {
    dump_profiler_file(q.get("f"))?;
    Ok(HttpResponse::Ok().finish())
}

pub fn config_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/")
            .route(web::post().to(graphql))
            .route(web::get().to(graphql)),
    );
    cfg.service(web::resource("/playground").route(web::get().to(playground_handler)));
    cfg.service(web::resource("/graphiql").route(web::get().to(graphiql_handler)));
    #[cfg(feature = "flame_it")]
    cfg.service(
        web::scope("/profiling")
            .service(web::resource("dump").route(web::post().to(dump_profiler))),
    );
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::path::PathBuf;

    use actix_web::{
        http::{header, StatusCode},
        test, App,
    };

    use coinnect_rt::bitstamp::BitstampCreds;
    use coinnect_rt::bittrex::BittrexCreds;
    use coinnect_rt::coinnect::Coinnect;
    use coinnect_rt::exchange::Exchange::Bitstamp;
    use coinnect_rt::exchange::{Exchange, ExchangeApi};
    use coinnect_rt::types::OrderType;
    use futures::lock::Mutex;

    use crate::api::config_app;
    use crate::graphql_schemas::root::create_schema;
    use std::sync::Arc;
    use strategies::{Strategy, StrategyKey};

    pub struct ExchangeConfig {
        key_file: String,
        exchanges: Vec<Exchange>,
    }

    fn build_exchanges(cfg: ExchangeConfig) -> HashMap<Exchange, Box<dyn ExchangeApi>> {
        let path = PathBuf::from(cfg.key_file.as_str());
        let mut exchg_map: HashMap<Exchange, Box<dyn ExchangeApi>> = HashMap::new();
        for exchange in cfg.exchanges {
            match exchange {
                Exchange::Bitstamp => {
                    let my_creds =
                        BitstampCreds::new_from_file("account_bitstamp", path.clone()).unwrap();
                    exchg_map.insert(
                        exchange,
                        Coinnect::new_exchange(exchange, Box::new(my_creds)).unwrap(),
                    );
                }
                Exchange::Bittrex => {
                    let my_creds =
                        BittrexCreds::new_from_file("account_bittrex", path.clone()).unwrap();
                    exchg_map.insert(
                        exchange,
                        Coinnect::new_exchange(exchange, Box::new(my_creds)).unwrap(),
                    );
                }
                _ => (),
            }
        }
        exchg_map
    }

    fn strats() -> HashMap<StrategyKey, Strategy> {
        HashMap::new()
    }

    #[actix_rt::test]
    async fn test_add_order() {
        let schema = create_schema();
        let exchanges = ExchangeConfig {
            key_file: "../keys_real_test.json".to_string(),
            exchanges: vec![Bitstamp],
        };
        let data: Arc<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>> =
            Arc::new(Mutex::new(build_exchanges(exchanges)));
        let strats: Arc<HashMap<StrategyKey, Strategy>> = Arc::new(strats());
        let mut app = test::init_service(
            App::new()
                .data(data)
                .data(schema)
                .data(strats)
                .configure(config_app),
        )
        .await;

        let _o = crate::api::Order {
            exchg: Bitstamp,
            t: OrderType::Limit,
            pair: "BTC_USD".into(),
            qty: 0.000001,
            price: 1.0,
        };
        let payload = r##"{"variables": null, "query": "{add_order(exchg:"Bitstamp",type:"SellLimit",pair:"BTC_USD", qty: 0.0000001, price: 0.01}" }"##.as_bytes();

        let req = test::TestRequest::post()
            .uri("/")
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(payload)
            .to_request();
        let resp = test::call_service(&mut app, req).await;
        let status = resp.status();
        let body = test::read_body(resp).await;
        assert_eq!(
            status,
            StatusCode::OK,
            "status : {}, body: {:?}",
            status,
            body
        );
    }
}
