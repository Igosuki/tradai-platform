// This example shows how to connect to your Poloniex account and perform simple operations

use binance::rest_model::BookTickers;
use broker_binance::BinanceApi;
use brokers::credential::{BasicCredentials, Credentials};
use brokers::exchange::Exchange;
use std::collections::HashMap;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // We create a PoloniexApi by providing API key/secret
    // You can give an empty String if you only use public methods
    let creds: Box<dyn Credentials> = Box::new(BasicCredentials::new(
        Exchange::Binance,
        "my_optionnal_name",
        "api_key",
        "api_secret",
        HashMap::default(),
    ));
    let my_api = BinanceApi::new(creds.as_ref()).await.unwrap();

    // Let's look at the ticker!
    let book_tickers = my_api.market().get_all_book_tickers().await.unwrap();
    match book_tickers {
        BookTickers::AllBookTickers(tickers) => {
            for coin in tickers {
                // please visit Binance API documentation to know how the data is returned
                // or look at the coinnect documentation
                let name = coin.symbol;
                println!(
                    "Coin {} has price : ask={}, bid={}",
                    name, coin.ask_price, coin.bid_price
                );
            }
        }
    }

    Ok(())
}
