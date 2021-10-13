use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, ActorFutureExt, AsyncContext, Context, ContextFutureSpawner, Handler, WrapFuture};
use chrono::{DateTime, Utc};
use futures::FutureExt;
use prometheus::GaugeVec;

use coinnect_rt::bot::Ping;
use coinnect_rt::prelude::*;
use coinnect_rt::types::{BalanceInformation, BalanceUpdate, Balances};

#[derive(Clone)]
pub struct BalanceMetrics {
    asset_gauge: GaugeVec,
}

impl BalanceMetrics {
    pub fn new() -> Self {
        let const_labels: HashMap<&str, &str> = labels! {};
        let asset_amount_metrics: GaugeVec = register_gauge_vec!(
            opts!(
                "free_amount",
                "Free coin available in this wallet for this asset and exchange.",
                const_labels
            ),
            &["xchg", "asset"]
        )
        .unwrap();

        Self {
            asset_gauge: asset_amount_metrics,
        }
    }

    pub fn free_amount(&self, xchg: Exchange, asset: Asset, amount: f64) {
        self.asset_gauge
            .with_label_values(&[&xchg.to_string(), asset.as_ref()])
            .set(amount);
    }
}

impl Default for BalanceMetrics {
    fn default() -> Self { Self::new() }
}

#[derive(Default)]
struct BalanceReport {
    balances: Balances,
    server_time: Option<DateTime<Utc>>,
    buffer: Vec<BalanceUpdate>,
}

impl BalanceReport {
    fn init(&mut self, balances: &BalanceInformation) {
        for (asset, amount) in &balances.assets {
            self.balances.insert(asset.clone(), *amount);
        }
        self.server_time = Some(balances.update_time);
        for update in self.buffer.clone() {
            self.push(update.clone());
        }
    }

    fn push(&mut self, update: BalanceUpdate) {
        match self.server_time {
            Some(server_time) => {
                if server_time.lt(&update.event_time) {
                    let asset: Asset = update.symbol.into();
                    match self.balances.entry(asset) {
                        Entry::Vacant(v) => {
                            v.insert(update.delta);
                        }
                        Entry::Occupied(mut o) => {
                            o.insert(o.get() + update.delta);
                        }
                    }
                }
            }
            None => self.buffer.push(update),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct BalanceReporterOptions {
    #[serde(deserialize_with = "util::serde::string_duration")]
    pub refresh_rate: Duration,
}

#[derive(Clone)]
pub struct BalanceReporter {
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
    balances: Arc<RwLock<HashMap<Exchange, BalanceReport>>>,
    refresh_rate: Duration,
    metrics: BalanceMetrics,
}

impl BalanceReporter {
    pub fn new(apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>, options: BalanceReporterOptions) -> Self {
        Self {
            apis: apis.clone(),
            balances: Default::default(),
            refresh_rate: options.refresh_rate,
            metrics: BalanceMetrics::default(),
        }
    }

    fn with_reporter<F>(&self, xchg: Exchange, f: F)
    where
        F: Fn(&mut BalanceReport),
    {
        let mut writer = self.balances.write().unwrap();
        if writer.get(&xchg).is_none() {
            writer.insert(xchg, Default::default());
        }
        if let Some(balance_report) = writer.get_mut(&xchg) {
            f(balance_report);
        }
    }
}

impl Actor for BalanceReporter {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.notify(RefreshBalances);
        ctx.run_interval(self.refresh_rate, move |act, _ctx| {
            for xchg in act.apis.keys() {
                act.with_reporter(*xchg, |balance_report| {
                    for (asset, amount) in balance_report.balances.clone() {
                        act.metrics.free_amount(*xchg, asset, amount);
                    }
                });
            }
        });
    }
}

impl Handler<AccountEventEnveloppe> for BalanceReporter {
    type Result = anyhow::Result<()>;

    fn handle(&mut self, msg: AccountEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        if msg.account_type != AccountType::Spot {
            return Ok(());
        }
        match msg.event {
            AccountEvent::BalanceUpdate(update) => {
                self.with_reporter(msg.xchg, |balance_report| {
                    balance_report.push(update.clone());
                });
                Ok(())
            }
            // Ignore anything besides order updates
            _ => Ok(()),
        }
    }
}

#[derive(actix::Message)]
#[rtype(result = "()")]
struct RefreshBalances;

impl Handler<RefreshBalances> for BalanceReporter {
    type Result = ();

    fn handle(&mut self, _: RefreshBalances, ctx: &mut Self::Context) -> Self::Result {
        let apis = self.apis.clone();
        Box::pin(
            async move {
                futures::future::join_all(
                    apis.clone()
                        .iter()
                        .map(|(&xchg, api)| api.balances().map(move |r| (xchg, r))),
                )
                .await
            }
            .into_actor(self)
            .map(|balances_results, this, _| {
                for (xchg, balance_result) in balances_results.iter() {
                    match balance_result {
                        Ok(balance) => {
                            this.with_reporter(*xchg, |balance_report| {
                                balance_report.init(balance);
                            });
                        }
                        Err(e) => {
                            error!(
                                "BalanceReporter : failed to fetch balance for exchange {xchg} : {err}",
                                xchg = xchg,
                                err = e
                            )
                        }
                    }
                }
            }),
        )
        .spawn(ctx);
    }
}

impl Handler<Ping> for BalanceReporter {
    type Result = ();

    fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self>) {}
}
