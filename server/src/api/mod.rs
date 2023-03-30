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
    #[display(fmt = "exchange not found")]
    ExchangeNotFound(Exchange),
    #[display(fmt = "broker error {}", _0)]
    Broker(brokers::error::Error),
    #[display(fmt = "std Io Error {}", _0)]
    IoError(std::io::Error),
}

impl ResponseError for ApiError {
    fn error_response(&self) -> HttpResponse<BoxBody> {
        match self {
            ExchangeNotFound(_e) => HttpResponse::NotFound().finish(),
            ApiError::Broker(e) => HttpResponse::InternalServerError().body(e.to_string()),
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

type BrokerageData = web::Data<Arc<BrokerageRegistry>>;
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
    let xch_str = q.get("exchange").expect("exchange parameter");
    let exchange = Exchange::from_str(xch_str)
        .map_err(|_| ApiError::Broker(brokers::error::Error::InvalidExchange(xch_str.to_string())))?;
    let confs = pair_confs(&exchange).map_err(ApiError::Broker)?;
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
    use crate::settings::Version;
    #[allow(unused_imports)]
    use brokers::broker_binance::BinanceExchangeConnector;
    use util::test::test_config_path;

    static DEFAULT_SYMBOL: &str = "BTC_USDT";

    async fn test_apis() -> BrokerageRegistry {
        let exchanges = HashMap::from([(Exchange::Binance, BrokerSettings {
            market_channels: vec![],
            fees: 0.1,
            use_account: true,
            use_margin_account: true,
            use_isolated_margin_account: true,
            isolated_margin_account_pairs: vec![],
            use_test: true,
        })]);
        let manager = Arc::new(Brokerages::new_manager());
        manager
            .build_exchange_apis(
                Arc::new(exchanges),
                format!("{}/keys_real_test.json", test_config_path()).into(),
            )
            .await;
        Brokerages::load_pair_registries(manager.exchange_apis()).await.unwrap();
        manager.exchange_apis().clone()
    }

    fn build_test_api(apis: BrokerageRegistry, cfg: &mut web::ServiceConfig) {
        let schema = create_schema();
        let strats: Arc<HashMap<StrategyKey, Trader>> = Arc::new(Default::default());
        let oms: Arc<HashMap<Exchange, Addr<OrderManager>>> = Arc::new(Default::default());
        cfg.app_data(Data::new(schema))
            .app_data(Data::new(Arc::new(apis)))
            .app_data(Data::new(strats))
            .app_data(Data::new(oms))
            .app_data(Data::new(Some(Version {
                version: "test".to_string(),
                sha: "test".to_string(),
            })));
        config_app(cfg);
    }

    // TODO: plugin registry not working
    #[test_log::test(actix::test)]
    async fn test_add_order() {
        let apis = test_apis().await;
        let app = App::new().configure(|cfg| build_test_api(apis.clone(), cfg));
        let app = test::init_service(app).await;
        let binance_api = apis.get(&Exchange::Binance).unwrap();
        let _ob = binance_api.orderbook(DEFAULT_SYMBOL.into()).await.unwrap();
        let price = 35_000.020_000_00;
        let _o = crate::api::Order {
            exchg: Exchange::Binance,
            t: OrderType::Limit,
            pair: DEFAULT_SYMBOL.into(),
            qty: 0.000_001_00,
            price,
        };
        let string = format!(
            r##"{{"variables": null, "query": "mutation {{ addOrder(input:{{exchg:\"binance\", orderType: LIMIT,side: SELL, pair:\"{DEFAULT_SYMBOL}\", quantity: 0.0015, dryRun: true, price: {price} }}) {{ identifier }} }}" }}"##
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
