use std::collections::HashMap;
use std::sync::Arc;

use actix_cors::Cors;
use actix_web::middleware::{Compat, Logger};
use actix_web::web::Data;
use actix_web::{http, HttpServer};

use coinnect_rt::prelude::*;
use strategy::{StrategyKey, Trader};

use crate::graphql_schemas::root::create_schema;
use crate::settings::{ApiSettings, CorsMode, Version};

pub async fn httpserver(
    settings: &ApiSettings,
    version: Option<Version>,
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
    strategies: Arc<HashMap<StrategyKey, Trader>>,
) -> std::io::Result<()> {
    // Make and start the api
    let port = settings.port.0;
    let cors_mode = settings.cors.clone();
    let allowed_origins = settings.allowed_origins.as_ref().unwrap_or(&vec![]).clone();
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
            CorsMode::Restricted => {
                let mut cors = Cors::default().allowed_origin(&format!("http://localhost:{}", port));

                for allowed_origin in allowed_origins.clone() {
                    cors = cors.allowed_origin(&allowed_origin.clone());
                }

                cors.allowed_methods(vec!["GET", "POST", "OPTIONS"])
                    .allowed_headers(exposed_headers.clone())
                    .allowed_header(http::header::CONTENT_TYPE)
                    .allowed_header(http::header::ACCESS_CONTROL_EXPOSE_HEADERS)
                    .expose_headers(exposed_headers)
                    .supports_credentials()
                    .max_age(3600)
            }
            CorsMode::Permissive => Cors::permissive(),
        };
        actix_web::App::new()
            .wrap(Compat::new(Logger::default()))
            .wrap(cors)
            .app_data(Data::new(schema))
            .app_data(Data::new(apis.clone()))
            .app_data(Data::new(strategies.clone()))
            .app_data(Data::new(version.clone()))
            .configure(crate::api::config_app)
    };
    debug!("Starting api server on {} ...", port);
    HttpServer::new(app).bind(format!("0.0.0.0:{}", port))?.run().await
}
