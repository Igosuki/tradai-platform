use std::collections::hash_map::Entry;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use actix::{Actor, ActorFutureExt, Addr, AsyncContext, Context, ContextFutureSpawner, Handler, WrapFuture};
use chrono::{DateTime, Utc};
use futures::FutureExt;
use prometheus::GaugeVec;

use coinnect_rt::bot::Ping;
use coinnect_rt::exchange::manager::ExchangeManagerRef;
use coinnect_rt::prelude::*;
use coinnect_rt::types::{AccountPosition, Balance, BalanceUpdate, Balances};

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

    pub fn free_amount(&self, xchg: Exchange, asset: &Asset, amount: f64) {
        self.asset_gauge
            .with_label_values(&[xchg.as_ref(), asset.as_ref()])
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
    pos_buffer: Vec<AccountPosition>,
}

impl BalanceReport {
    fn init(&mut self, balances: &AccountPosition) {
        for (asset, amount) in &balances.balances {
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
                            v.insert(Balance {
                                free: update.delta,
                                locked: 0.0,
                            });
                        }
                        Entry::Occupied(mut o) => {
                            o.insert(o.get().add_free(update.delta));
                        }
                    }
                }
            }
            None => self.buffer.push(update),
        }
    }

    fn reset(&mut self, pos: AccountPosition) {
        match self.server_time {
            Some(server_time) => {
                if server_time.lt(&pos.update_time) {
                    for (asset, balance) in pos.balances {
                        self.balances.insert(asset, balance);
                    }
                }
            }
            None => self.pos_buffer.push(pos),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct BalanceReporterOptions {
    #[serde(deserialize_with = "util::ser::string_duration")]
    pub refresh_rate: Duration,
}

#[derive(Clone)]
pub struct BalanceReporter {
    apis: ExchangeManagerRef,
    balances: Arc<RwLock<HashMap<Exchange, BalanceReport>>>,
    refresh_rate: Duration,
    metrics: BalanceMetrics,
}

impl BalanceReporter {
    pub fn new(apis: ExchangeManagerRef, options: &BalanceReporterOptions) -> Self {
        Self {
            apis,
            balances: Arc::new(RwLock::new(HashMap::default())),
            refresh_rate: options.refresh_rate,
            metrics: BalanceMetrics::default(),
        }
    }

    pub async fn actor(options: &BalanceReporterOptions, apis: ExchangeManagerRef) -> Addr<Self> {
        let balance_reporter = Self::new(apis, options);
        Self::start(balance_reporter)
    }

    fn with_reporter<F>(&self, xchg: Exchange, f: F)
    where
        F: Fn(&mut BalanceReport),
    {
        let mut writer = self.balances.write().unwrap();
        if writer.get(&xchg).is_none() {
            writer.insert(xchg, BalanceReport::default());
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
            for api_ref in act.apis.exchange_apis() {
                let xchg = *api_ref.key();
                act.with_reporter(xchg, |balance_report| {
                    for (asset, amount) in balance_report.balances.clone() {
                        act.metrics.free_amount(xchg, &asset, amount.free);
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
            }
            AccountEvent::AccountPositionUpdate(position) => {
                self.with_reporter(msg.xchg, |balance_report| {
                    balance_report.reset(position.clone());
                });
            }
            // Ignore anything besides order updates
            _ => {}
        }
        Ok(())
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
                futures::future::join_all(apis.exchange_apis().iter().map(|entry| async move {
                    let arc = entry.value().clone();
                    let k = *entry.key();
                    arc.account_balances().map(move |r| (k, r)).await
                }))
                .await
            }
            .into_actor(self)
            .map(|balances_results, this, _| {
                for (xchg, balance_result) in balances_results {
                    match balance_result {
                        Ok(balance) => {
                            this.with_reporter(xchg, |balance_report| {
                                balance_report.init(&balance);
                            });
                        }
                        Err(e) => {
                            error!(
                                "BalanceReporter : failed to fetch balance for exchange {xchg} : {err}",
                                xchg = xchg,
                                err = e
                            );
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
