use crate::graphql_schemas::root::create_schema;
use actix_cors::Cors;
use actix_web::{http, HttpServer};
use coinnect_rt::binance::BinanceCreds;
use coinnect_rt::bitstamp::BitstampCreds;
use coinnect_rt::bittrex::BittrexCreds;
use coinnect_rt::coinnect::Coinnect;
use coinnect_rt::exchange::{Exchange, ExchangeApi, ExchangeSettings};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use strategies::{Strategy, StrategyKey};

pub async fn httpserver(
    exchanges: HashMap<Exchange, ExchangeSettings>,
    strategies: Arc<HashMap<StrategyKey, Strategy>>,
    keys_path: PathBuf,
    port: i32,
) -> std::io::Result<()> {
    // Make and start the api
    let app = move || {
        let mut apis: HashMap<Exchange, Box<dyn ExchangeApi>> = HashMap::new();
        for (xch, _conf) in exchanges.clone() {
            let xch_api = match xch {
                Exchange::Bittrex => {
                    let creds = Box::new(
                        BittrexCreds::new_from_file("account_bittrex", keys_path.clone()).unwrap(),
                    );
                    Coinnect::new_exchange(xch, creds.clone()).unwrap()
                }
                Exchange::Bitstamp => {
                    let creds = Box::new(
                        BitstampCreds::new_from_file("account_bitstamp", keys_path.clone())
                            .unwrap(),
                    );
                    Coinnect::new_exchange(xch, creds.clone()).unwrap()
                }
                Exchange::Binance => {
                    let creds = Box::new(
                        BinanceCreds::new_from_file("account_binance", keys_path.clone()).unwrap(),
                    );
                    Coinnect::new_exchange(xch, creds.clone()).unwrap()
                }
                _ => {
                    info!("Unknown exchange when building Exchange Apis : {:?}", xch);
                    unimplemented!()
                }
            };
            apis.insert(xch, xch_api);
        }
        let data = Mutex::new(apis);
        let schema = create_schema();

        actix_web::App::new()
            .wrap(
                Cors::new()
                    .allowed_origin("http://localhost:8180")
                    .allowed_methods(vec!["GET", "POST"])
                    .allowed_headers(vec![http::header::AUTHORIZATION, http::header::ACCEPT])
                    .allowed_header(http::header::CONTENT_TYPE)
                    .supports_credentials()
                    .max_age(3600)
                    .finish(),
            )
            .data(schema)
            .data(data)
            .data(strategies.clone())
            .configure(crate::api::config_app)
    };
    debug!("Starting api server on {} ...", port);
    HttpServer::new(app)
        .bind(format!("localhost:{}", port))?
        .run()
        .await
}
