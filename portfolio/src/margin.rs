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
use coinnect_rt::types::{BalanceUpdate, MarginAccountDetails, MarginAsset};

#[derive(Clone)]
pub struct MarginAccountMetrics {
    free: GaugeVec,
    borrowed: GaugeVec,
    locked: GaugeVec,
    interest: GaugeVec,
    net: GaugeVec,
}

impl MarginAccountMetrics {
    pub fn new() -> Self {
        let const_labels: HashMap<&str, &str> = labels! {};
        let free_amount_metrics: GaugeVec = register_gauge_vec!(
            opts!(
                "margin_free_amount",
                "Free amount of this asset available in this margin wallet.",
                const_labels
            ),
            &["xchg", "asset"]
        )
        .unwrap();
        let borrowed_amount_metrics: GaugeVec = register_gauge_vec!(
            opts!("margin_borrowed_amount", "Borrowed amount per asset.", const_labels),
            &["xchg", "asset"]
        )
        .unwrap();
        let interest_amount_metrics: GaugeVec = register_gauge_vec!(
            opts!("margin_interest_amount", "Interest owed per asset.", const_labels),
            &["xchg", "asset"]
        )
        .unwrap();
        let locked_amount_metrics: GaugeVec = register_gauge_vec!(
            opts!(
                "margin_locked_amount",
                "Asset currently locked in the margin wallet.",
                const_labels
            ),
            &["xchg", "asset"]
        )
        .unwrap();
        let net_amount_metrics: GaugeVec = register_gauge_vec!(
            opts!("margin_net_amount", "Net asset in the margin wallet.", const_labels),
            &["xchg", "asset"]
        )
        .unwrap();
        Self {
            free: free_amount_metrics,
            borrowed: borrowed_amount_metrics,
            locked: locked_amount_metrics,
            interest: interest_amount_metrics,
            net: net_amount_metrics,
        }
    }

    fn free_amount(&self, xchg: Exchange, asset: &str, amount: f64) {
        self.free.with_label_values(&[&xchg.to_string(), asset]).set(amount);
    }

    fn borrowed_amount(&self, xchg: Exchange, asset: &str, amount: f64) {
        self.borrowed.with_label_values(&[&xchg.to_string(), asset]).set(amount);
    }

    fn locked_amount(&self, xchg: Exchange, asset: &str, amount: f64) {
        self.locked.with_label_values(&[&xchg.to_string(), asset]).set(amount);
    }

    fn interest_amount(&self, xchg: Exchange, asset: &str, amount: f64) {
        self.interest.with_label_values(&[&xchg.to_string(), asset]).set(amount);
    }

    fn net_amount(&self, xchg: Exchange, asset: &str, amount: f64) {
        self.net.with_label_values(&[&xchg.to_string(), asset]).set(amount);
    }

    pub fn report_asset(&self, xchg: Exchange, asset: &MarginAsset) {
        self.free_amount(xchg, &asset.asset, asset.free);
        self.borrowed_amount(xchg, &asset.asset, asset.borrowed);
        self.locked_amount(xchg, &asset.asset, asset.locked);
        self.interest_amount(xchg, &asset.asset, asset.interest);
        self.net_amount(xchg, &asset.asset, asset.net_asset);
    }
}

impl Default for MarginAccountMetrics {
    fn default() -> Self { Self::new() }
}

#[derive(Default)]
struct MarginAccountReport {
    balances: HashMap<String, MarginAsset>,
    server_time: Option<DateTime<Utc>>,
    buffer: Vec<BalanceUpdate>,
}

impl MarginAccountReport {
    fn init(&mut self, details: &MarginAccountDetails) {
        for margin_asset in &details.margin_assets {
            self.balances.insert(margin_asset.asset.clone(), margin_asset.clone());
        }
        self.server_time = Some(Utc::now());
        for update in self.buffer.clone() {
            self.push(update.clone());
        }
    }

    fn push(&mut self, update: BalanceUpdate) {
        match self.server_time {
            Some(server_time) => {
                if server_time.lt(&update.event_time) {
                    match self.balances.entry(update.symbol) {
                        Entry::Vacant(v) => {
                            v.insert(MarginAsset {
                                asset: "".to_string(),
                                borrowed: 0.0,
                                free: update.delta,
                                interest: 0.0,
                                locked: 0.0,
                                net_asset: update.delta,
                            });
                        }
                        Entry::Occupied(mut o) => {
                            let mut asset = o.get_mut();
                            asset.free += update.delta;
                        }
                    }
                }
            }
            None => self.buffer.push(update),
        }
    }
}

#[derive(Clone, Debug, Deserialize)]
pub struct MarginAccountReporterOptions {
    #[serde(deserialize_with = "util::ser::string_duration")]
    pub refresh_rate: Duration,
}

#[derive(Clone)]
pub struct MarginAccountReporter {
    apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>,
    balances: Arc<RwLock<HashMap<Exchange, MarginAccountReport>>>,
    refresh_rate: Duration,
    metrics: MarginAccountMetrics,
}

impl MarginAccountReporter {
    pub fn new(apis: Arc<HashMap<Exchange, Arc<dyn ExchangeApi>>>, options: MarginAccountReporterOptions) -> Self {
        Self {
            apis: apis.clone(),
            balances: Default::default(),
            refresh_rate: options.refresh_rate,
            metrics: MarginAccountMetrics::default(),
        }
    }

    fn with_reporter<F>(&self, xchg: Exchange, f: F)
    where
        F: Fn(&mut MarginAccountReport),
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

impl Actor for MarginAccountReporter {
    type Context = Context<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        ctx.notify(RefreshBalances);
        ctx.run_interval(self.refresh_rate, move |act, _ctx| {
            for xchg in act.apis.keys() {
                act.with_reporter(*xchg, |balance_report| {
                    for (_, margin_asset) in balance_report.balances.clone() {
                        act.metrics.report_asset(*xchg, &margin_asset);
                    }
                });
            }
        });
    }
}

impl Handler<AccountEventEnveloppe> for MarginAccountReporter {
    type Result = anyhow::Result<()>;

    fn handle(&mut self, msg: AccountEventEnveloppe, _ctx: &mut Self::Context) -> Self::Result {
        // Ignore anything besides margin account events
        if msg.account_type != AccountType::Margin {
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

impl Handler<RefreshBalances> for MarginAccountReporter {
    type Result = ();

    fn handle(&mut self, _: RefreshBalances, ctx: &mut Self::Context) -> Self::Result {
        let apis = self.apis.clone();
        Box::pin(
            async move {
                futures::future::join_all(
                    apis.clone()
                        .iter()
                        .map(|(&xchg, api)| api.margin_account(None).map(move |r| (xchg, r))),
                )
                .await
            }
            .into_actor(self)
            .map(|margin_account_results, this, _| {
                for (xchg, margin_account_result) in margin_account_results.iter() {
                    match margin_account_result {
                        Ok(margin_account_details) => {
                            this.with_reporter(*xchg, |balance_report| {
                                balance_report.init(margin_account_details);
                            });
                        }
                        Err(e) => {
                            error!(
                                "MarginAccountReporter : failed to fetch balance for exchange {xchg} : {err}",
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

impl Handler<Ping> for MarginAccountReporter {
    type Result = ();

    fn handle(&mut self, _msg: Ping, _ctx: &mut Context<Self>) {}
}
