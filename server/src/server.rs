use crate::graphql_schemas::root::create_schema;
use actix::Addr;
use actix_cors::Cors;
use actix_web::web::Data;
use actix_web::{http, HttpServer};
use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::exchange::{Exchange, ExchangeSettings};
use futures::lock::Mutex;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use strategies::order_manager::OrderManager;
use strategies::{Strategy, StrategyKey};

pub async fn httpserver(
    exchanges: HashMap<Exchange, ExchangeSettings>,
    strategies: Arc<HashMap<StrategyKey, Strategy>>,
    order_managers: Arc<HashMap<Exchange, Addr<OrderManager>>>,
    keys_path: PathBuf,
    port: i32,
) -> std::io::Result<()> {
    // Make and start the api
    let apis = Coinnect::build_exchange_apis(Arc::new(exchanges.clone()), keys_path.clone()).await;
    let api_m = Arc::new(Mutex::new(apis));
    let app = move || {
        let schema = create_schema();

        let cors = Cors::default()
            .allowed_origin(&format!("http://localhost:{}", port))
            .allowed_methods(vec!["GET", "POST"])
            .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
            .allowed_header(http::header::CONTENT_TYPE)
            .supports_credentials()
            .max_age(3600);
        actix_web::App::new()
            .wrap(cors)
            .app_data(Data::new(schema))
            .app_data(Data::new(api_m.clone()))
            .app_data(Data::new(strategies.clone()))
            .app_data(Data::new(order_managers.clone()))
            .configure(crate::api::config_app)
    };
    debug!("Starting api server on {} ...", port);
    HttpServer::new(app).bind(format!("localhost:{}", port))?.run().await
}
