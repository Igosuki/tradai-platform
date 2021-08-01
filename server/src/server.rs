use std::collections::HashMap;
use std::sync::Arc;

use actix::Addr;
use actix_cors::Cors;
use actix_web::web::Data;
use actix_web::{http, HttpServer};

use coinnect_rt::exchange::{Exchange, ExchangeApi};
use strategies::order_manager::OrderManager;
use strategies::{Strategy, StrategyKey};

use crate::graphql_schemas::root::create_schema;
use crate::settings::{ApiSettings, CorsMode};

pub async fn httpserver(
    settings: &ApiSettings,
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
    strategies: Arc<HashMap<StrategyKey, Strategy>>,
    order_managers: Arc<HashMap<Exchange, Addr<OrderManager>>>,
) -> std::io::Result<()> {
    // Make and start the api
    let port = settings.port.0;
    let cors_mode = settings.cors.clone();
    let app = move || {
        let schema = create_schema();

        let cors = match cors_mode {
            CorsMode::Restricted => Cors::default()
                .allowed_origin(&format!("http://localhost:{}", port))
                .allowed_methods(vec!["GET", "POST", "OPTIONS"])
                .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
                .allowed_header(http::header::CONTENT_TYPE)
                .supports_credentials()
                .max_age(3600),
            CorsMode::Permissive => Cors::permissive(),
        };
        actix_web::App::new()
            .wrap(cors)
            .app_data(Data::new(schema))
            .app_data(Data::new(apis.clone()))
            .app_data(Data::new(strategies.clone()))
            .app_data(Data::new(order_managers.clone()))
            .configure(crate::api::config_app)
    };
    debug!("Starting api server on {} ...", port);
    HttpServer::new(app).bind(format!("localhost:{}", port))?.run().await
}
