use actix::{Actor, Addr};
use coinnect_rt::exchange::MockApi;
use coinnect_rt::prelude::*;
use db::DbOptions;
use std::path::Path;
use std::sync::Arc;

use crate::order_manager::OrderManager;

pub async fn it_order_manager<S: AsRef<Path>, S2: AsRef<Path>>(
    keys_file: S2,
    dir: S,
    exchange: Exchange,
) -> OrderManager {
    let api = Coinnect::new_manager()
        .build_exchange_api(keys_file.as_ref().to_path_buf(), &exchange, true)
        .await
        .unwrap();
    let om_path = format!("om_{}", exchange);
    OrderManager::new(api, &DbOptions::new(dir), om_path)
}

pub fn new_mock_manager<S: AsRef<Path>>(path: S) -> OrderManager {
    let api: Arc<dyn ExchangeApi> = Arc::new(MockApi);
    OrderManager::new(api, &DbOptions::new(path), "")
}

pub fn mock_manager<S: AsRef<Path>>(path: S) -> Addr<OrderManager> {
    let order_manager = new_mock_manager(path);
    let act = OrderManager::start(order_manager);
    loop {
        if act.connected() {
            break;
        }
    }
    act
}

pub fn local_manager<S: AsRef<Path>>(path: S, api: Arc<dyn ExchangeApi>) -> Addr<OrderManager> {
    let order_manager = OrderManager::new(api, &DbOptions::new(path), "");
    OrderManager::start(order_manager)
}
