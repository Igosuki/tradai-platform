//! Use this module to interact with Binance exchange.
//! Please see examples for more informations.

use binance::account::Account;
use binance::api::Binance;
use binance::config::Config;
use binance::general::General;
use binance::margin::Margin;
use binance::market::Market;

use broker_core::error::*;
use broker_core::prelude::*;

#[derive(Debug, Clone)]
pub struct BinanceApi {
    // unix timestamp in ms, to avoid ban
    pub api_key: Option<String>,
    pub api_secret: Option<String>,
    burst: bool,
    config: Config,
    pub vip_level: u8,
}

impl BinanceApi {
    fn private_api<T: Binance>(api_key: Option<String>, api_secret: Option<String>, config: &Config) -> T {
        Binance::new_with_config(api_key, api_secret, config)
    }

    /// Create a new BinanceApi by providing an API key & API secret
    pub async fn new(creds: &dyn Credentials) -> Result<BinanceApi> {
        Self::new_with_config(creds, Config::default()).await
    }

    /// Create a new BinanceApi poiting to test servers by providing an API key & API secret
    pub async fn new_test(creds: &dyn Credentials) -> Result<BinanceApi> {
        Self::new_with_config(creds, Config::testnet()).await
    }

    pub async fn new_with_host(creds: &dyn Credentials, host: String) -> Result<BinanceApi> {
        let host_http_url = format!("https://{}", host);
        let host_wss_url = format!("wss://{}", host);
        let conf = Config::default()
            .set_rest_api_endpoint(&host_http_url)
            .set_ws_endpoint(&host_wss_url)
            .set_futures_rest_api_endpoint(&host_http_url)
            .set_futures_ws_endpoint(&host_wss_url);
        Self::new_with_config(creds, conf).await
    }

    pub async fn new_with_config(creds: &dyn Credentials, config: Config) -> Result<BinanceApi> {
        if creds.exchange() != Exchange::Binance {
            return Err(Error::InvalidConfigType {
                expected: Exchange::Binance,
                find: creds.exchange(),
            });
        }

        let api_key = creds.get("api_key");
        let api_secret = creds.get("api_secret");
        let vip_level = creds.get("vip_level");

        Ok(BinanceApi {
            api_key,
            api_secret,
            vip_level: vip_level.and_then(|s| s.parse::<u8>().ok()).unwrap_or(0_u8),
            config,
            burst: false, // No burst by default
        })
    }

    pub fn market(&self) -> Market { Self::private_api(self.api_key.clone(), self.api_secret.clone(), &self.config) }

    pub fn account(&self) -> Account { Self::private_api(self.api_key.clone(), self.api_secret.clone(), &self.config) }

    pub fn margin(&self) -> Margin { Self::private_api(self.api_key.clone(), self.api_secret.clone(), &self.config) }

    pub fn general(&self) -> General { Self::private_api(self.api_key.clone(), self.api_secret.clone(), &self.config) }

    /// The number of calls in a given period is limited. In order to avoid a ban we limit
    /// by default the number of api requests.
    /// This function sets or removes the limitation.
    /// Burst false implies no block.
    /// Burst true implies there is a control over the number of calls allowed to the exchange
    pub fn set_burst(&mut self, burst: bool) { self.burst = burst }
}

#[cfg(test)]
mod binance_api_tests {
    //    #[test]
    //    fn should_block_or_not_block_when_enabled_or_disabled() {
    //        let mut api = BinanceApi {
    //            last_request: get_unix_timestamp_ms(),
    //            api_key: "".to_string(),
    //            api_secret: "".to_string(),
    //            customer_id: "".to_string(),
    //            http_client: Client::new(),
    //            burst: false,
    //        };
    //
    //        let mut counter = 0;
    //        loop {
    //            api.set_burst(false);
    //            let start = get_unix_timestamp_ms();
    //            api.block_or_continue();
    //            api.last_request = get_unix_timestamp_ms();
    //
    //            let difference = api.last_request - start;
    //            assert!(difference >= 334);
    //            assert!(difference < 10000);
    //
    //
    //            api.set_burst(true);
    //            let start = get_unix_timestamp_ms();
    //            api.block_or_continue();
    //            api.last_request = get_unix_timestamp_ms();
    //
    //            let difference = api.last_request - start;
    //            assert!(difference < 10);
    //
    //            counter = counter + 1;
    //            if counter >= 3 { break; }
    //        }
    //    }
}
