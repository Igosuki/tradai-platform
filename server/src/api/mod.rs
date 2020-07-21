use std::collections::HashMap;

use crate::api::ApiError::ExchangeNotFound;
use crate::graphql_schemas::root::{Context, Schema};
use actix_web::{web, Error, HttpResponse, ResponseError};
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::{OrderType, Pair, Price, Volume};
use derive_more::Display;
use futures::lock::Mutex;
use serde::{Deserialize, Serialize};
#[cfg(feature = "flame_it")]
use std::fs::File;
use std::sync::Arc;
use strategies::{Strategy, StrategyKey};

mod graphql;
mod playground_source;

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
    self::graphql::graphiql_handler("/", None).await
}

async fn playground_handler() -> Result<HttpResponse, Error> {
    self::graphql::playground_handler("/", None).await
}

type ExchangeData = web::Data<Arc<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>>>;
type StratsData = web::Data<Arc<HashMap<StrategyKey, Strategy>>>;

async fn graphql(
    req: actix_web::HttpRequest,
    payload: actix_web::web::Payload,
    schema: web::Data<Schema>,
    strats: StratsData,
    exchanges: ExchangeData,
) -> Result<HttpResponse, Error> {
    let ctx = Context {
        strats: strats.get_ref().to_owned(),
        exchanges: exchanges.get_ref().to_owned(),
    };
    self::graphql::graphql_handler(&schema, &ctx, req, payload).await
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
    use actix_web::{
        http::{header, StatusCode},
        test, App,
    };
    use bytes::Buf;
    use futures::lock::Mutex;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use tokio::time::timeout;

    use coinnect_rt::bitstamp::BitstampCreds;
    use coinnect_rt::bittrex::BittrexCreds;
    use coinnect_rt::coinnect::Coinnect;
    use coinnect_rt::exchange::Exchange::Binance;
    use coinnect_rt::exchange::{Exchange, ExchangeApi};
    use coinnect_rt::types::OrderType;

    use crate::api::config_app;
    use crate::graphql_schemas::root::create_schema;
    use coinnect_rt::binance::BinanceCreds;
    use std::sync::Arc;
    use std::time::Duration;
    use strategies::{Strategy, StrategyKey};

    pub struct ExchangeConfig {
        key_file: String,
        exchanges: Vec<Exchange>,
    }

    async fn build_exchanges(cfg: ExchangeConfig) -> HashMap<Exchange, Box<dyn ExchangeApi>> {
        let path = PathBuf::from(cfg.key_file.as_str());
        let mut exchg_map: HashMap<Exchange, Box<dyn ExchangeApi>> = HashMap::new();
        for exchange in cfg.exchanges {
            match exchange {
                Exchange::Bitstamp => {
                    let my_creds =
                        BitstampCreds::new_from_file("account_bitstamp", path.clone()).unwrap();
                    exchg_map.insert(
                        exchange,
                        Coinnect::new_exchange(exchange, Box::new(my_creds))
                            .await
                            .unwrap(),
                    );
                }
                Exchange::Bittrex => {
                    let my_creds =
                        BittrexCreds::new_from_file("account_bittrex", path.clone()).unwrap();
                    exchg_map.insert(
                        exchange,
                        Coinnect::new_exchange(exchange, Box::new(my_creds))
                            .await
                            .unwrap(),
                    );
                }
                Exchange::Binance => {
                    let my_creds =
                        BinanceCreds::new_from_file("account_binance", path.clone()).unwrap();
                    exchg_map.insert(
                        exchange,
                        Coinnect::new_exchange(exchange, Box::new(my_creds))
                            .await
                            .unwrap(),
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
            exchanges: vec![Binance],
        };
        let exchanges_map = build_exchanges(exchanges).await;
        let data: Arc<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>> =
            Arc::new(Mutex::new(exchanges_map));
        let mut guard = data.lock().await;
        let binance_api: &mut Box<dyn ExchangeApi> = guard.get_mut(&Exchange::Binance).unwrap();
        let ob = binance_api.orderbook("BTC_USDT".into()).await.unwrap();
        drop(guard);
        let price = ob.bids.first().unwrap().0;
        let strats: Arc<HashMap<StrategyKey, Strategy>> = Arc::new(strats());
        let mut app = test::init_service(
            App::new()
                .data(data.clone())
                .data(schema)
                .data(strats)
                .configure(config_app),
        )
        .await;

        let _o = crate::api::Order {
            exchg: Binance,
            t: OrderType::Limit,
            pair: "BTC_USD".into(),
            qty: 0.000001,
            price,
        };
        let string = format!(
            r##"{{"variables": null, "query": "mutation {{ addOrder(input:{{exchg:\"Binance\", orderType: LIMIT,side: SELL, pair:\"BTC_USDT\", quantity: 0.0015, dryRun: true, price: {} }}) {{ identifier }} }}" }}"##,
            price
        );
        let payload = string.as_bytes();

        let req = test::TestRequest::post()
            .uri("/")
            .header(header::CONTENT_TYPE, "application/json")
            .set_payload(string)
            .to_request();
        let resp = timeout(Duration::from_secs(10), test::call_service(&mut app, req)).await;
        assert!(resp.is_ok(), "response: {:?}", resp);
        let resp = resp.unwrap();
        let status = resp.status();
        let body = test::read_body(resp).await;
        let body_string = std::str::from_utf8(body.bytes()).unwrap();
        let res: serde_json::error::Result<serde_json::Value> = serde_json::from_str(body_string);
        assert!(res.is_ok(), "failed to deserialize json: {:?}", body_string);
        let v = res.unwrap();
        assert_eq!(
            status,
            StatusCode::OK,
            "status : {}, body: {:?}",
            status,
            body
        );
        let option = v.get("data");
        assert!(option.is_some(), "data should exist: {:?}", &option);
        assert!(
            option.unwrap() != &serde_json::Value::Null,
            "data should not be null : {:?}",
            v
        );
    }
}
