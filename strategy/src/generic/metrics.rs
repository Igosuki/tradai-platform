use std::collections::HashMap;

use prometheus::{default_registry, CounterVec, GaugeVec, Registry};

use coinnect_rt::exchange::Exchange;
use coinnect_rt::types::Pair;
use metrics::{make_gauges, MetricGaugeProvider, MetricProviderFn};
use portfolio::portfolio::Portfolio;
use trading::position::Position;
use trading::signal::TradeSignal;

lazy_static! {
    static ref METRICS: GenericDriverMetrics = { GenericDriverMetrics::new(default_registry()) };
}

type PortfolioIndicatorFn = MetricProviderFn<Portfolio>;
type PositionIndicatorFn = MetricProviderFn<Position>;
type SignalIndicatorFn = MetricProviderFn<TradeSignal>;

#[derive(Clone)]
pub struct GenericDriverMetrics {
    lock_counters: GaugeVec,
    failed_position_counters: GaugeVec,
    signal_errors: CounterVec,
    errors: CounterVec,
    signal_fns: Vec<SignalIndicatorFn>,
    signal_gauges: HashMap<String, GaugeVec>,
    portfolio_fns: Vec<PortfolioIndicatorFn>,
    portfolio_gauges: HashMap<String, GaugeVec>,
    position_fns: Vec<PositionIndicatorFn>,
    position_gauges: HashMap<String, GaugeVec>,
    status_gauge: GaugeVec,
}

impl GenericDriverMetrics {
    fn new(_registry: &Registry) -> Self {
        let const_labels: HashMap<&str, &str> = HashMap::new();
        let lock_counters = {
            let pos_labels = &["xch", "pair"];
            let vec_name = "dr_pos_lock";
            register_gauge_vec!(
                opts!(vec_name, format!("gauge for {}", vec_name), const_labels),
                pos_labels
            )
            .unwrap()
        };

        let failed_position_counters = {
            let pos_labels = &["xch", "pair"];
            let vec_name = "dr_pos_failed";
            register_gauge_vec!(
                opts!(vec_name, format!("gauge for {}", vec_name), const_labels),
                pos_labels
            )
            .unwrap()
        };

        let signal_errors = {
            let pos_labels = &["xch", "pair"];
            let vec_name = "dr_sig_err";
            register_counter_vec!(
                opts!(vec_name, format!("counter for {}", vec_name), const_labels),
                pos_labels
            )
            .unwrap()
        };

        let errors = {
            let pos_labels = &["err"];
            let vec_name = "dr_all_err";
            register_counter_vec!(
                opts!(vec_name, format!("counter for {}", vec_name), const_labels),
                pos_labels
            )
            .unwrap()
        };

        let signal_fns: Vec<SignalIndicatorFn> = vec![
            ("signal_price".to_string(), |x| x.price),
            ("signal_qty".to_string(), |x| x.qty.unwrap_or(0.0)),
        ];
        let signal_gauges = make_gauges(const_labels.clone(), &["skey", "xch", "mkt", "pos", "op"], &signal_fns);

        let portfolio_fns: Vec<PortfolioIndicatorFn> = vec![
            ("ptf_value".to_string(), Portfolio::value),
            ("ptf_pnl".to_string(), Portfolio::pnl),
            ("ptf_current_return".to_string(), Portfolio::current_return),
        ];
        let portfolio_gauges = make_gauges(const_labels.clone(), &["skey"], &portfolio_fns);

        let position_fns: Vec<PositionIndicatorFn> = vec![
            ("pos_return".to_string(), |x| x.unreal_profit_loss),
            ("pos_traded_price".to_string(), |x| {
                x.open_order.as_ref().and_then(|o| o.price).unwrap_or(0.0)
            }),
            ("pos_nominal_position".to_string(), |x| {
                x.open_order.as_ref().and_then(|o| o.base_qty).unwrap_or(0.0)
            }),
        ];

        let position_gauges = make_gauges(const_labels.clone(), &["skey", "xch", "mkt"], &position_fns);

        let status_gauge = register_gauge_vec!(
            opts!("is_trading", "Whether the strategy is trading or not.", const_labels),
            &["skey"]
        )
        .unwrap();

        Self {
            lock_counters,
            failed_position_counters,
            signal_errors,
            errors,
            signal_fns,
            signal_gauges,
            portfolio_fns,
            portfolio_gauges,
            position_fns,
            position_gauges,
            status_gauge,
        }
    }

    pub(super) fn log_lock(&self, xch: Exchange, pair: &Pair) {
        self.lock_counters
            .with_label_values(&[xch.as_ref(), pair.as_ref()])
            .set(1.0);
    }

    pub(super) fn log_failed_position(&self, xch: Exchange, pair: &Pair) {
        self.failed_position_counters
            .with_label_values(&[xch.as_ref(), pair.as_ref()])
            .set(1.0);
    }

    pub(super) fn signal_error(&self, xch: Exchange, pair: &Pair) {
        self.signal_errors
            .with_label_values(&[xch.as_ref(), pair.as_ref()])
            .inc();
    }

    pub(super) fn log_error(&self, e: &str) { self.errors.with_label_values(&[e]).inc(); }

    pub(super) fn log_signals(&self, strat_key: &str, signals: &[TradeSignal]) {
        for signal in signals {
            self.log_all_with_providers(&self.signal_fns, signal, &[
                strat_key,
                signal.exchange.as_ref(),
                &signal.pair,
                signal.pos_kind.as_ref(),
                signal.op_kind.as_ref(),
            ]);
        }
    }

    pub(super) fn log_portfolio(&self, strat_key: &str, portfolio: &Portfolio) {
        self.log_all_with_providers(&self.portfolio_fns, portfolio, &[strat_key]);
        for position in portfolio.open_positions().values() {
            self.log_all_with_providers(&self.position_fns, position, &[
                strat_key,
                position.exchange.as_ref(),
                position.symbol.as_ref(),
            ]);
        }
    }

    pub(super) fn log_is_trading(&self, strat_key: &str, trading: bool) {
        self.status_gauge
            .with_label_values(&[strat_key])
            .set(if trading { 1.0 } else { 0.0 });
    }
}

impl MetricGaugeProvider<Portfolio> for GenericDriverMetrics {
    fn gauges(&self) -> &HashMap<String, GaugeVec> { &self.portfolio_gauges }
}

impl MetricGaugeProvider<TradeSignal> for GenericDriverMetrics {
    fn gauges(&self) -> &HashMap<String, GaugeVec> { &self.signal_gauges }
}

impl MetricGaugeProvider<Position> for GenericDriverMetrics {
    fn gauges(&self) -> &HashMap<String, GaugeVec> { &self.position_gauges }
}

pub fn get() -> &'static GenericDriverMetrics {
    lazy_static::initialize(&METRICS);
    &METRICS
}
