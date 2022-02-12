pub mod e2e {
    use super::super::error::*;
    use brokers::credential::Credentials;
    use brokers::prelude::*;
    use std::collections::HashMap;
    use std::path::PathBuf;
    use std::sync::Arc;

    #[allow(dead_code)]
    pub async fn build_apis() -> Result<(Box<dyn Credentials>, Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>)> {
        let credentials_file = std::env::var("BITCOINS_E2E_TEST_CREDS_FILE").expect("BITCOINS_E2E_TEST_CREDS_FILE");
        let credentials_path = PathBuf::from(credentials_file);
        let credentials = brokers::Brokerages::credentials_for(Exchange::Binance, credentials_path.clone())
            .expect("valid credentials file");
        let manager = brokers::Brokerages::new_manager();
        let api = manager
            .build_exchange_api(credentials_path, &Exchange::Binance, false)
            .await?;
        let mut apis_map = HashMap::new();
        apis_map.insert(Exchange::Binance, api.clone());
        let apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>> = Arc::new(apis_map);
        Ok((credentials, apis))
    }
}
