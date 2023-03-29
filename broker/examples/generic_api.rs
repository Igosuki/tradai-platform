// This example shows how to use the generic API provided by the broker_core crate.
// This method is useful if you have to iterate throught multiple accounts of
// different exchanges and perform the same operation (such as get the current account's balance)
// You can also use the Broker generic API if you want a better error handling since all methods
// return Result<_, Error>.

use brokers::credential::{BasicCredentials, Credentials};
use brokers::exchange::Exchange;
use brokers::manager::BrokerageManager;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // We create a Broker Generic API
    // Since Kraken does not need customer_id field, we set it to None
    let my_creds: Box<dyn Credentials> = Box::new(BasicCredentials::new(
        Exchange::Kraken,
        "my_optional_name",
        "api_key",
        "api_secret",
        HashMap::default(),
    ));
    let manager = BrokerageManager::new();
    let my_api = manager
        .new_exchange_with_options(Exchange::Kraken, my_creds, true)
        .await
        .unwrap();
    let ticker = my_api.ticker("ETC_BTC".into());

    println!(
        "ETC_BTC last trade price is {}.",
        ticker.await.unwrap().last_trade_price
    );
    Ok(())
}
