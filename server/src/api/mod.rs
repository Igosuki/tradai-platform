use std::collections::HashMap;
#[cfg(feature = "flame_it")]
use std::fs::File;
use std::sync::Arc;

use actix_web::{body::Body,
                web::{self, HttpResponse as HttpResponse2},
                Error, HttpResponse, ResponseError};
use derive_more::Display;
use futures::lock::Mutex;
use serde::{Deserialize, Serialize};

use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::{OrderType, Pair, Price, Volume};
use strategies::{Strategy, StrategyKey};

use crate::api::ApiError::ExchangeNotFound;
use crate::graphql_schemas::root::{Context, Schema};
use actix::Addr;
use strategies::order_manager::OrderManager;

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
    #[display(fmt = "Coinnect error {}", _0)]
    Coinnect(coinnect_rt::error::Error),
    #[display(fmt = "Std Io Error {}", _0)]
    IoError(std::io::Error),
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse2<Body> {
        match self {
            ExchangeNotFound(_e) => HttpResponse2::not_found(),
            ApiError::Coinnect(e) => HttpResponse2::internal_server_error().set_body(e.to_string().into()),
            ApiError::IoError(e) => HttpResponse2::internal_server_error().set_body(e.to_string().into()), // _ => HttpResponse::InternalServerError().finish()
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self { ApiError::IoError(e) }
}

async fn graphiql_handler() -> Result<HttpResponse, Error> { self::graphql::graphiql_handler("/", None).await }

async fn playground_handler() -> Result<HttpResponse, Error> { self::graphql::playground_handler("/", None).await }

type ExchangeData = web::Data<Arc<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>>>;
type StratsData = web::Data<Arc<HashMap<StrategyKey, Strategy>>>;
type OrderManagerData = web::Data<Arc<HashMap<Exchange, Addr<OrderManager>>>>;

async fn graphql(
    req: actix_web::HttpRequest,
    payload: actix_web::web::Payload,
    schema: web::Data<Schema>,
    strats: StratsData,
    exchanges: ExchangeData,
    order_managers: OrderManagerData,
) -> Result<HttpResponse, Error> {
    let ctx = Context {
        strats: strats.get_ref().to_owned(),
        exchanges: exchanges.get_ref().to_owned(),
        order_managers: order_managers.get_ref().to_owned(),
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
    cfg.service(web::scope("/profiling").service(web::resource("dump").route(web::post().to(dump_profiler))));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use actix_web::{http::StatusCode, test, App};
    use futures::lock::Mutex;
    use tokio::time::timeout;

    use coinnect_rt::coinnect;
    use coinnect_rt::exchange::Exchange::Binance;
    use coinnect_rt::exchange::{Exchange, ExchangeApi};
    use coinnect_rt::types::OrderType;
    use strategies::{Strategy, StrategyKey};

    use crate::api::config_app;
    use crate::graphql_schemas::root::create_schema;
    use actix_web::http::header::ContentType;

    fn strats() -> HashMap<StrategyKey, Strategy> { HashMap::new() }

    #[actix_rt::test]
    async fn test_add_order() {
        let schema = create_schema();
        let exchanges = coinnect::ExchangeConfig::new("../config/keys_real_test.json".to_string(), vec![Binance]);
        let exchanges_map = coinnect::build_test_exchanges(exchanges).await;
        let data: Arc<Mutex<HashMap<Exchange, Box<dyn ExchangeApi>>>> = Arc::new(Mutex::new(exchanges_map));
        let mut guard = data.lock().await;
        let binance_api: &mut Box<dyn ExchangeApi> = guard.get_mut(&Exchange::Binance).unwrap();
        let _ob = binance_api.orderbook("BTC_USDT".into()).await.unwrap();
        drop(guard);
        let price = 35000.02000000;
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
            qty: 0.00000100,
            price,
        };
        let string = format!(
            r##"{{"variables": null, "query": "mutation {{ addOrder(input:{{exchg:\"Binance\", orderType: LIMIT,side: SELL, pair:\"BTC_USDT\", quantity: 0.0015, dryRun: true, price: {} }}) {{ identifier }} }}" }}"##,
            price
        );
        println!("{}", string);
        let req = test::TestRequest::post()
            .uri("/")
            .insert_header(ContentType::json())
            .set_payload(string)
            .to_request();
        let resp = timeout(Duration::from_secs(10), test::call_service(&mut app, req)).await;
        assert!(resp.is_ok(), "response: {:?}", resp);
        let resp = resp.unwrap();
        let status = resp.status();
        let body = test::read_body(resp).await;
        let body_string = std::str::from_utf8(body.as_ref()).unwrap();
        let res: serde_json::error::Result<serde_json::Value> = serde_json::from_str(body_string);
        assert!(res.is_ok(), "failed to deserialize json: {:?}", body_string);
        let v = res.unwrap();
        assert_eq!(status, StatusCode::OK, "status : {}, body: {:?}", status, body);
        let option = v.get("data");
        assert!(option.is_some(), "data should exist: {:?}", &option);
        assert!(
            option.unwrap() != &serde_json::Value::Null,
            "data should not be null : {}",
            serde_json::to_string_pretty(&v).unwrap()
        );
    }
}
