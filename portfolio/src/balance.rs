use actix::{Actor, AsyncContext, Context, ContextFutureSpawner, Handler, ResponseActFuture, WrapFuture};
use chrono::{DateTime, Utc};
use coinnect_rt::exchange::{Exchange, ExchangeApi};
use coinnect_rt::types::{AccountEvent, AccountEventEnveloppe, Asset, BalanceInformation, BalanceUpdate, Balances};
use prometheus::GaugeVec;
use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

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
struct BalanceReporter {
    apis: Arc<HashMap<Exchange, Box<dyn ExchangeApi>>>,
    balances: Arc<RwLock<HashMap<Exchange, BalanceReport>>>,
    refresh_rate: Duration,
    metrics: BalanceMetrics,
}

impl BalanceReporter {
    fn new(apis: Arc<HashMap<Exchange, Box<dyn ExchangeApi>>>, options: BalanceReporterOptions) -> Self {
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
        let zis = self.clone();
        Box::pin(
            async move {
                for (xchg, api) in zis.apis.iter() {
                    if let Ok(balances) = api.balances().await {
                        zis.with_reporter(*xchg, |balance_report| {
                            balance_report.init(&balances);
                        });
                    }
                }
            }
            .into_actor(self),
        )
        .spawn(ctx);
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
    type Result = ResponseActFuture<Self, anyhow::Result<()>>;

    fn handle(&mut self, msg: AccountEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        let zis = self.clone();
        Box::pin(
            async move {
                match msg.1 {
                    AccountEvent::BalanceUpdate(update) => {
                        zis.with_reporter(msg.0, |balance_report| {
                            balance_report.push(update.clone());
                        });
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
