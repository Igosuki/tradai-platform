use std::collections::HashMap;

use prometheus::{CounterVec, GaugeVec, Registry};

use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::Pair;
use metrics::store::MetricStore;
use portfolio::portfolio::Portfolio;
use stats::indicators::macd_apo::MACDApo;
use trading::book::BookPosition;
use trading::position::{OperationKind, Position, PositionKind};

type PortfolioIndicatorFn = (String, fn(&Portfolio) -> f64);

type PositionIndicatorFn = (String, fn(&Position) -> f64);

type ModelIndicatorFn = (String, fn(&MACDApo) -> f64);

lazy_static! {
    static ref METRIC_STORE: MetricStore<String, MeanRevertingStrategyMetrics> = { MetricStore::new() };
}

pub fn metric_store() -> &'static MetricStore<String, MeanRevertingStrategyMetrics> {
    lazy_static::initialize(&METRIC_STORE);
    &METRIC_STORE
}

#[derive(Clone)]
pub struct MeanRevertingStrategyMetrics {
    common_gauges: HashMap<String, GaugeVec>,
    portfolio_indicator_fns: Vec<PortfolioIndicatorFn>,
    position_indicator_fns: Vec<PositionIndicatorFn>,
    model_indicator_fns: Vec<ModelIndicatorFn>,
    error_counter: CounterVec,
    status_gauge: GaugeVec,
}

impl MeanRevertingStrategyMetrics {
    pub fn for_strat(_registry: &Registry, pair: &str) -> MeanRevertingStrategyMetrics {
        metric_store().get_or_create(pair.to_string(), || Self::new_metrics(_registry, pair))
    }

    fn new_metrics(_registry: &Registry, pair: &str) -> MeanRevertingStrategyMetrics {
        let mut gauges: HashMap<String, GaugeVec> = HashMap::new();
        let const_labels = labels! {"pair" => pair};
        {
            let pos_labels = &["pos", "op"];
            let gauge_name = "mr_position";
            let gauge_vec = register_gauge_vec!(
                opts!(gauge_name, format!("gauge for {}", gauge_name), const_labels),
                pos_labels
            )
            .unwrap();
            gauges.insert(gauge_name.to_string(), gauge_vec);
        }

        {
            let gauge_name = "mr_mid";
            let gauge_vec =
                register_gauge_vec!(opts!(gauge_name, format!("gauge for {}", gauge_name), const_labels), &[
                ])
                .unwrap();
            gauges.insert(gauge_name.to_string(), gauge_vec);
        }

        let model_gauges: Vec<ModelIndicatorFn> = vec![("apo".to_string(), |x| x.apo)];

        let threshold_gauges: Vec<String> = vec!["mr_threshold_short".to_string(), "mr_threshold_long".to_string()];
        let portfolio_gauges: Vec<PortfolioIndicatorFn> = vec![
            ("value_strat".to_string(), |x| x.value()),
            ("pnl".to_string(), |x| x.pnl()),
        ];
        let position_gauges: Vec<PositionIndicatorFn> = vec![
            ("return".to_string(), |x| x.unreal_profit_loss),
            ("traded_price".to_string(), |x| {
                x.open_order.as_ref().and_then(|o| o.price).unwrap_or(0.0)
            }),
            ("nominal_position".to_string(), |x| {
                x.open_order.as_ref().and_then(|o| o.base_qty).unwrap_or(0.0)
            }),
        ];
        {
            for gauge_name in portfolio_gauges
                .iter()
                .map(|g| &g.0)
                .chain(threshold_gauges.iter())
                .chain(position_gauges.iter().map(|g| &g.0))
                .chain(model_gauges.iter().map(|g| &g.0))
            {
                let gauge_vec = register_gauge_vec!(
                    opts!(gauge_name, format!("gauge for {}", gauge_name.clone()), const_labels),
                    &[]
                )
                .unwrap();
                gauges.insert(gauge_name.clone(), gauge_vec.clone());
            }
        }

        let error_counter = register_counter_vec!(opts!("error", "Total number of process errors.", const_labels), &[
            "kind"
        ])
        .unwrap();

        let status_gauge = register_gauge_vec!(
            opts!("mr_is_trading", "Whether the strategy is trading or not.", const_labels),
            &[]
        )
        .unwrap();

        MeanRevertingStrategyMetrics {
            common_gauges: gauges,
            portfolio_indicator_fns: portfolio_gauges.clone(),
            model_indicator_fns: model_gauges.clone(),
            position_indicator_fns: position_gauges.clone(),
            error_counter,
            status_gauge,
        }
    }

    pub(super) fn log_position(&self, pos_kind: PositionKind, op_kind: OperationKind, price: f64) {
        if let Some(g) = self.common_gauges.get("mr_position") {
            g.with_label_values(&[pos_kind.as_ref(), op_kind.as_ref()]).set(price);
        }
    }

    fn log_all_with_portfolio(&self, gauges: &[PortfolioIndicatorFn], state: &Portfolio) {
        for (state_gauge_name, state_gauge_fn) in gauges {
            if let Some(g) = self.common_gauges.get(state_gauge_name) {
                g.with_label_values(&[]).set(state_gauge_fn(state))
            }
        }
    }

    fn log_all_with_position(&self, gauges: &[PositionIndicatorFn], state: &Position) {
        for (state_gauge_name, state_gauge_fn) in gauges {
            if let Some(g) = self.common_gauges.get(state_gauge_name) {
                g.with_label_values(&[]).set(state_gauge_fn(state))
            }
        }
    }

    pub(super) fn log_portfolio(&self, xch: Exchange, pair: Pair, portfolio: &Portfolio) {
        self.log_all_with_portfolio(&self.portfolio_indicator_fns, portfolio);
        if let Some(pos) = portfolio.open_position(xch, pair) {
            self.log_all_with_position(&self.position_indicator_fns, pos);
        }
    }

    pub(super) fn log_thresholds(&self, threshold_short: f64, threshold_long: f64) {
        if let Some(g) = self.common_gauges.get("mr_threshold_short") {
            g.with_label_values(&[]).set(threshold_short)
        }
        if let Some(g) = self.common_gauges.get("mr_threshold_long") {
            g.with_label_values(&[]).set(threshold_long)
        }
    }
    pub(super) fn log_model(&self, model: MACDApo) {
        for (gauge_name, model_gauge_fn) in &self.model_indicator_fns {
            if let Some(g) = self.common_gauges.get(gauge_name) {
                g.with_label_values(&[]).set(model_gauge_fn(&model))
            }
        }
    }

    pub(super) fn log_pos(&self, pos: &BookPosition) {
        if let Some(g) = self.common_gauges.get("mr_mid") {
            g.with_label_values(&[]).set(pos.mid);
        };
    }

    pub(super) fn log_error(&self, label: &str) { self.error_counter.with_label_values(&[label]).inc(); }

    pub(super) fn log_is_trading(&self, trading: bool) {
        self.status_gauge
            .with_label_values(&[])
            .set(if trading { 1.0 } else { 0.0 });
    }
}
