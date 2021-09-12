///! Checks internet connectivity
use std::time::Duration;

use actix_web::rt::time;
use prometheus::{register_int_counter, IntCounter};

lazy_static! {
    pub static ref CONNECTIVITY_COUNTER: IntCounter =
        register_int_counter!("connection_check", "Internet connection check").unwrap();
}

pub async fn run_connectivity_checker(interval_secs: u64) {
    let mut interval = time::interval(Duration::from_secs(interval_secs));
    loop {
        interval.tick().await;
        if online::check(None).await.is_ok() {
            (&*CONNECTIVITY_COUNTER).inc();
        }
    }
}
