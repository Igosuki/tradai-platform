use crate::exchange::Exchange;
use crate::types::{AccountPosition, BalanceUpdate, OrderUpdate};
use std::collections::HashSet;

#[derive(Debug, Clone, Copy, Eq, PartialEq, Hash, Deserialize, AsRefStr)]
pub enum PrivateStreamChannel {
    #[strum(serialize = "orders")]
    Orders,
    #[strum(serialize = "balances")]
    Balances,
}

impl PrivateStreamChannel {
    pub fn all() -> HashSet<Self> {
        let mut channels = HashSet::new();
        channels.insert(Self::Balances);
        channels.insert(Self::Orders);
        channels
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, EnumString, AsRefStr, Hash)]
#[serde(rename_all = "snake_case")]
pub enum AccountType {
    #[strum(serialize = "spot")]
    Spot,
    #[strum(serialize = "margin")]
    Margin,
    #[strum(serialize = "isolated_margin")]
    IsolatedMargin(String),
    #[strum(serialize = "coin_futures")]
    CoinFutures,
    #[strum(serialize = "usdt_futures")]
    UsdtFutures,
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "()")]
pub enum AccountEvent {
    OrderUpdate(OrderUpdate),
    BalanceUpdate(BalanceUpdate),
    AccountPositionUpdate(AccountPosition),
    Noop,
}

#[derive(Message, Clone, Debug)]
#[rtype(result = "anyhow::Result<()>")]
pub struct AccountEventEnveloppe {
    pub xchg: Exchange,
    pub event: AccountEvent,
    pub account_type: AccountType,
}
