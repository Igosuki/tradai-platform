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
use crate::settings::{ApiSettings, CorsMode, Version};
use actix_web::middleware::{Compat, Logger};

pub async fn httpserver(
    settings: &ApiSettings,
    version: Option<Version>,
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
    strategies: Arc<HashMap<StrategyKey, Strategy>>,
    order_managers: Arc<HashMap<Exchange, Addr<OrderManager>>>,
) -> std::io::Result<()> {
    // Make and start the api
    let port = settings.port.0;
    let cors_mode = settings.cors.clone();
    let app = move || {
        let schema = create_schema();

        let exposed_headers = vec![
            http::header::CONTENT_RANGE,
            http::header::AUTHORIZATION,
            http::header::ACCEPT,
            http::header::ACCESS_CONTROL_ALLOW_ORIGIN,
            http::header::ACCESS_CONTROL_REQUEST_HEADERS,
            http::header::ACCESS_CONTROL_REQUEST_METHOD,
            http::header::CONTENT_TYPE,
        ];
        let cors = match cors_mode {
            CorsMode::Restricted => Cors::default()
                .allowed_origin(&format!("http://localhost:{}", port))
                .allowed_origin("http://localhost:3001")
                .allowed_methods(vec!["GET", "POST", "OPTIONS"])
                .allowed_headers(exposed_headers.clone())
                .allowed_header(http::header::CONTENT_TYPE)
                .allowed_header(http::header::ACCESS_CONTROL_EXPOSE_HEADERS)
                .expose_headers(exposed_headers)
                .supports_credentials()
                .max_age(3600),
            CorsMode::Permissive => Cors::permissive(),
        };
        actix_web::App::new()
            .wrap(Compat::new(Logger::default()))
            .wrap(cors)
            .app_data(Data::new(schema))
            .app_data(Data::new(apis.clone()))
            .app_data(Data::new(strategies.clone()))
            .app_data(Data::new(order_managers.clone()))
            .app_data(Data::new(version.clone()))
            .configure(crate::api::config_app)
    };
    debug!("Starting api server on {} ...", port);
    HttpServer::new(app).bind(format!("localhost:{}", port))?.run().await
}
