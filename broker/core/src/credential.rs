use core::fmt::Debug;
use core::option::Option;
use dyn_clone::DynClone;
use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;

use crate::error::*;
use crate::exchange::Exchange;

pub trait Credentials: Debug + DynClone + Sync + Send {
    /// Get an element from the credentials.
    fn get(&self, cred: &str) -> Option<String>;
    /// Return the targeted `Exchange`.
    fn exchange(&self) -> Exchange;
    /// Return the client name.
    fn name(&self) -> String;
}

dyn_clone::clone_trait_object!(Credentials);

#[derive(Debug, Clone)]
pub struct BasicCredentials {
    exchange: Exchange,
    name: String,
    data: HashMap<String, String>,
}

impl BasicCredentials {
    /// Create a new set of basic credentials
    ///
    /// # Arguments
    ///
    /// * `name`: a unique key used to identify this set of credentials
    /// * `api_key`: the api key
    /// * `api_secret`: the api secret
    /// * `ext`: other fields to pass to clients
    ///
    /// returns: BasicCredentials
    ///
    /// # Examples
    ///
    /// ```
    ///
    /// ```
    pub fn new(exchange: Exchange, name: &str, api_key: &str, api_secret: &str, ext: HashMap<String, String>) -> Self {
        let mut creds = BasicCredentials {
            data: HashMap::new(),
            exchange,
            name: name.to_string(),
        };

        //if api_key.is_empty() {
        //warning!("No API key set for the Bitstamp client");
        //}
        creds.data.insert("api_key".to_string(), api_key.to_string());

        //if api_secret.is_empty() {
        //warning!("No API secret set for the Bitstamp client");
        //}
        creds.data.insert("api_secret".to_string(), api_secret.to_string());

        //if api_secret.is_empty() {
        //warning!("No API customer ID set for the Bitstamp client");
        //}
        creds.data.extend(ext);

        creds
    }

    /// Create a new `BasicCredentials` from a json configuration file. This file must follow this
    /// structure:
    ///
    /// ```json
    /// {
    ///     "account_kraken": {
    ///         "exchange"  : "kraken",
    ///         "api_key"   : "123456789ABCDEF",
    ///         "api_secret": "ABC&EF?abcdef"
    ///     },
    ///     "account_bitstamp": {
    ///         "exchange"   : "bitstamp",
    ///         "api_key"    : "1234567890ABCDEF1234567890ABCDEF",
    ///         "api_secret" : "1234567890ABCDEF1234567890ABCDEF",
    ///         "customer_id": "123456"
    ///     }
    /// }
    /// ```
    /// `BasicCredentials::new_from_file("account_bitstamp", Path::new("/keys.json"))`
    pub fn new_from_file<S: AsRef<Path>>(path: S) -> Result<HashMap<String, Box<dyn Credentials>>> {
        let mut f = File::open(path)?;
        let mut buffer = String::new();
        f.read_to_string(&mut buffer)?;

        let data: HashMap<String, HashMap<String, String>> = serde_json::from_str(&buffer)?;
        let credentials: Result<HashMap<std::string::String, Box<dyn Credentials>>> = data
            .into_iter()
            .map(|(account_name, account)| {
                let exchange = {
                    let exchange_str = account
                        .get("exchange")
                        .ok_or_else(|| Error::MissingField("exchange".to_string()))?;
                    Exchange::from_str(exchange_str).map_err(|e| Error::InvalidFieldValue {
                        value: "exchange".to_string(),
                        source: anyhow!(e),
                    })
                };
                exchange.map(|exchange| {
                    let x: Box<dyn Credentials> = Box::new(BasicCredentials {
                        exchange,
                        name: account_name.clone(),
                        data: account,
                    });
                    (account_name, x)
                })
            })
            .collect();
        credentials
    }

    pub fn empty(exchange: Exchange) -> Self {
        Self {
            exchange,
            name: String::default(),
            data: HashMap::default(),
        }
    }
}

impl Credentials for BasicCredentials {
    /// Return a value from the credentials.
    fn get(&self, key: &str) -> Option<String> { self.data.get(key).cloned() }

    fn name(&self) -> String { self.name.clone() }

    fn exchange(&self) -> Exchange { self.exchange }
}
