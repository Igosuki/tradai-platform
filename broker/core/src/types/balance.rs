use std::collections::HashMap;

use chrono::{DateTime, Utc};

use crate::types::Asset;

pub type Balances = HashMap<Asset, Balance>;

#[derive(Clone, Debug)]
pub struct AccountPosition {
    pub balances: Balances,
    pub update_time: DateTime<Utc>,
}

impl AccountPosition {
    pub fn new() -> Self { AccountPosition::default() }

    pub fn insert(&mut self, asset: Asset, balance: Balance) { self.balances.insert(asset, balance); }
}

impl Default for AccountPosition {
    fn default() -> Self {
        Self {
            balances: HashMap::default(),
            update_time: Utc::now(),
        }
    }
}

#[derive(Clone, Debug, Copy)]
pub struct Balance {
    pub free: f64,
    pub locked: f64,
}

impl Balance {
    pub fn add_free(mut self, delta: f64) -> Self {
        self.free += delta;
        self
    }
}
