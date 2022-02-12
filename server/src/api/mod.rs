use std::collections::HashMap;
#[cfg(feature = "flame")]
use std::fs::File;
use std::str::FromStr;
use std::sync::Arc;

use actix::Addr;
use actix_web::body::BoxBody;
use actix_web::{web::{self},
                Error, HttpResponse, ResponseError};
use derive_more::Display;
use serde::{Deserialize, Serialize};

use brokers::pair::pair_confs;
use brokers::prelude::*;
use strategy::{StrategyKey, Trader};
use trading::order_manager::OrderManager;

use crate::api::ApiError::ExchangeNotFound;
use crate::graphql_schemas::root::Schema;
use crate::graphql_schemas::Context;
use crate::settings::Version;

mod graphql;
mod playground_source;

#[derive(Debug, Serialize, Deserialize)]
pub struct Order {
    exchg: Exchange,
    #[serde(rename(serialize = "type", deserialize = "type"))]
    t: OrderType,
    pair: Pair,
    qty: f64,
    price: f64,
}

#[derive(Debug, Display)]
pub enum ApiError {
    #[display(fmt = "Exchange not found")]
    ExchangeNotFound(Exchange),
    #[display(fmt = "Coinnect error {}", _0)]
    Coinnect(brokers::error::Error),
    #[display(fmt = "Std Io Error {}", _0)]
    IoError(std::io::Error),
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse<BoxBody> {
        match self {
            ExchangeNotFound(_e) => HttpResponse::NotFound().finish(),
            ApiError::Coinnect(e) => HttpResponse::InternalServerError().body(e.to_string()),
            ApiError::IoError(e) => HttpResponse::InternalServerError().body(e.to_string()),
            //_ => HttpResponse::InternalServerError().finish(),
        }
    }
}

impl From<std::io::Error> for ApiError {
    fn from(e: std::io::Error) -> Self { ApiError::IoError(e) }
}

async fn graphiql_handler() -> Result<HttpResponse, Error> { self::graphql::graphiql_handler("/", None).await }

async fn playground_handler() -> Result<HttpResponse, Error> { self::graphql::playground_handler("/", None).await }

type BrokerageData = web::Data<Arc<HashMap<Exchange, Arc<dyn Brokerage>>>>;
type StratsData = web::Data<Arc<HashMap<StrategyKey, Trader>>>;
type OrderManagerData = web::Data<Arc<HashMap<Exchange, Addr<OrderManager>>>>;

async fn graphql(
    req: actix_web::HttpRequest,
    payload: actix_web::web::Payload,
    schema: web::Data<Schema>,
    strats: StratsData,
    exchanges: BrokerageData,
    order_managers: OrderManagerData,
) -> Result<HttpResponse, Error> {
    let ctx = Context {
        strats: strats.get_ref().clone(),
        exchanges: exchanges.get_ref().clone(),
        order_managers: order_managers.get_ref().clone(),
    };
    self::graphql::graphql_handler(&schema, &ctx, req, payload).await
}

#[cfg(feature = "flame")]
fn dump_profiler_file(name: Option<&String>) -> Result<(), std::io::Error> {
    let string = format!("flame-graph-{}.html", chrono::Utc::now());
    let graph_file_name = name.unwrap_or(&string);
    info!("Dumping profiler file at {}", graph_file_name);
    let graph_file = &mut File::create(graph_file_name)?;
    flame::dump_html(graph_file)
}

#[cfg(feature = "flame")]
pub async fn dump_profiler(q: web::Query<HashMap<String, String>>) -> Result<HttpResponse, Error> {
    dump_profiler_file(q.get("f"))?;
    Ok(HttpResponse::Ok().finish())
}

async fn exchange_conf(q: web::Query<HashMap<String, String>>) -> Result<HttpResponse, Error> {
    let exchange = Exchange::from_str(q.get("exchange").expect("exchange parameter")).map_err(ApiError::Coinnect)?;
    let confs = pair_confs(&exchange).map_err(ApiError::Coinnect)?;
    Ok(HttpResponse::Ok().json(confs))
}

async fn version(version: web::Data<Option<Version>>) -> Result<HttpResponse, Error> {
    Ok(HttpResponse::Ok().json(version))
}

pub fn config_app(cfg: &mut web::ServiceConfig) {
    cfg.service(
        web::resource("/")
            .route(web::post().to(graphql))
            .route(web::get().to(graphql)),
    );
    cfg.service(web::resource("/exchange_conf").route(web::get().to(exchange_conf)));
    cfg.service(web::resource("/version").route(web::get().to(version)));
    cfg.service(web::resource("/playground").route(web::get().to(playground_handler)));
    cfg.service(web::resource("/graphiql").route(web::get().to(graphiql_handler)));
    #[cfg(feature = "flame")]
    cfg.service(web::scope("/profiling").service(web::resource("dump").route(web::post().to(dump_profiler))));
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::time::Duration;

    use actix::Addr;
    use actix_web::http::header::ContentType;
    use actix_web::web::Data;
    use actix_web::{http::StatusCode, test, web, App};
    use brokers::manager::BrokerageRegistry;
    use tokio::time::timeout;

    use brokers::prelude::*;
    use strategy::{StrategyKey, Trader};
    use trading::order_manager::OrderManager;

    use crate::api::config_app;
    use crate::graphql_schemas::root::create_schema;

    fn strats() -> HashMap<StrategyKey, Trader> { HashMap::new() }

    fn oms() -> Arc<HashMap<Exchange, Addr<OrderManager>>> { Arc::new(HashMap::default()) }

    async fn test_apis() -> BrokerageRegistry {
        let exchanges = [(Exchange::Binance, BrokerSettings {
            orderbook: None,
            orderbook_depth: None,
            trades: None,
            fees: 0.1,
            use_account: true,
            use_margin_account: true,
            use_isolated_margin_account: true,
            isolated_margin_account_pairs: vec![],
            use_test: false,
        })]
        .iter()
        .cloned()
        .collect();
        let manager = Arc::new(Brokerages::new_manager());
        manager
            .build_exchange_apis(Arc::new(exchanges), "../config/keys_real_test.json".into())
            .await;
        Brokerages::load_pair_registries(manager.exchange_apis()).await.unwrap();
        manager.exchange_apis().clone()
    }

    fn build_test_api(apis: BrokerageRegistry, cfg: &mut web::ServiceConfig) {
        let schema = create_schema();
        let strats: Arc<HashMap<StrategyKey, Trader>> = Arc::new(strats());
        cfg.app_data(Data::new(apis))
            .app_data(Data::new(schema))
            .app_data(Data::new(strats))
            .app_data(Data::new(Arc::new(oms())));
        config_app(cfg);
    }

    #[actix_rt::test]
    async fn test_add_order() {
        let apis = test_apis().await;
        let app = App::new().configure(|cfg| build_test_api(apis.clone(), cfg));
        let app = test::init_service(app).await;
        let binance_api = apis.get(&Exchange::Binance).unwrap();
        let _ob = binance_api.orderbook("BTC_USDT".into()).await.unwrap();
        let price = 35_000.020_000_00;
        let _o = crate::api::Order {
            exchg: Exchange::Binance,
            t: OrderType::Limit,
            pair: "BTC_USDT".into(),
            qty: 0.000_001_00,
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
        let resp = timeout(Duration::from_secs(10), test::call_service(&app, req)).await;
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
