#![allow(dead_code)]

use brokers::types::AccountType;
use std::collections::HashMap;

pub struct Wallet {
    pub account_type: AccountType,
    pub holdings: HashMap<String, f64>,
}
