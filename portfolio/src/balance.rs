use actix::{Actor, Context, Handler, ResponseActFuture, WrapFuture};
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::{AccountEvent, AccountEventEnveloppe, Asset, BalanceUpdate, Balances};
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

#[derive(Clone)]
struct BalanceReporter {
    apis: Arc<HashMap<Exchange, Box<dyn ExchangeApi>>>,
    balances: Arc<RwLock<HashMap<Exchange, Balances>>>,
}

impl BalanceReporter {
    fn new(apis: Arc<HashMap<Exchange, Box<dyn ExchangeApi>>>) -> Self {
        Self {
            apis: apis.clone(),
            balances: Default::default(),
        }
    }

    fn update_balance(&self, xchg: Exchange, update: BalanceUpdate) {
        let mut writer = self.balances.write().unwrap();
        if writer.get(&xchg).is_none() {
            writer.insert(xchg, Default::default());
        }
        if let Some(balances) = writer.get_mut(&xchg) {
            let asset: Asset = update.symbol.into();
            match balances.entry(asset) {
                Entry::Vacant(v) => {
                    v.insert(update.delta);
                }
                Entry::Occupied(mut o) => {
                    o.insert(o.get() + update.delta);
                }
            }
        }
    }
}

impl Actor for BalanceReporter {
    type Context = Context<Self>;

    fn started(&mut self, _ctx: &mut Self::Context) {
        // Fetch account balances for each api
        // After that, empty balance update queue
        // Finally, unlock reporting
    }
}

impl Handler<AccountEventEnveloppe> for BalanceReporter {
    type Result = ResponseActFuture<Self, anyhow::Result<()>>;

    fn handle(&mut self, msg: AccountEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        let mut zis = self.clone();
        Box::pin(
            async move {
                match msg.1 {
                    AccountEvent::BalanceUpdate(update) => {
                        zis.update_balance(msg.0, update);
                        Ok(())
                    }
                    // Ignore anything besides order updates
                    _ => Ok(()),
                }
            }
            .into_actor(self),
        )
    }
}
