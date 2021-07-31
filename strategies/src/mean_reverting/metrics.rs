use crate::mean_reverting::ema_model::SinglePosRow;
use crate::mean_reverting::state::{MeanRevertingState, Position};
use crate::types::OperationKind;
use prometheus::{CounterVec, GaugeVec, Registry};
use std::collections::HashMap;

type StateIndicatorFn = (String, fn(&MeanRevertingState) -> f64);

pub struct MeanRevertingStrategyMetrics {
    common_gauges: HashMap<String, GaugeVec>,
    threshold_indicator_fns: Vec<StateIndicatorFn>,
    state_indicator_fns: Vec<StateIndicatorFn>,
    error_counter: CounterVec,
    status_gauge: GaugeVec,
}

impl MeanRevertingStrategyMetrics {
    pub fn for_strat(_registry: &Registry, pair: &str) -> MeanRevertingStrategyMetrics {
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

        let threshold_gauges: Vec<StateIndicatorFn> = vec![
            ("mr_threshold_short".to_string(), |x: &MeanRevertingState| {
                x.threshold_short()
            }),
            ("mr_threshold_long".to_string(), |x: &MeanRevertingState| {
                x.threshold_long()
            }),
        ];
        {
            for (gauge_name, _) in threshold_gauges.clone() {
                let gauge_vec = register_gauge_vec!(
                    opts!(
                        &gauge_name,
                        format!("threshold gauge for {}", gauge_name.clone()),
                        const_labels
                    ),
                    &[]
                )
                .unwrap();
                gauges.insert(gauge_name.clone(), gauge_vec.clone());
            }
        }

        let state_gauges: Vec<StateIndicatorFn> = vec![
            ("apo".to_string(), |x| x.apo()),
            ("threshold_long".to_string(), |x| x.threshold_long()),
            ("threshold_short".to_string(), |x| x.threshold_short()),
            ("value_strat".to_string(), |x| x.value_strat()),
            ("previous_value_strat".to_string(), |x| x.previous_value_strat()),
            ("return".to_string(), |x| {
                x.long_position_return() + x.short_position_return()
            }),
            ("traded_price".to_string(), |x| x.traded_price()),
            ("pnl".to_string(), |x| x.pnl()),
            ("nominal_position".to_string(), |x| x.nominal_position()),
        ];
        {
            for (gauge_name, _) in state_gauges.clone() {
                let gauge_vec = register_gauge_vec!(
                    opts!(
                        &gauge_name,
                        format!("state gauge for {}", gauge_name.clone()),
                        const_labels
                    ),
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
            opts!("is_trading", "Whether the strategy is trading or not.", const_labels),
            &[]
        )
        .unwrap();

        MeanRevertingStrategyMetrics {
            common_gauges: gauges,
            threshold_indicator_fns: threshold_gauges.clone(),
            state_indicator_fns: state_gauges.clone(),
            error_counter,
            status_gauge,
        }
    }

    pub(super) fn log_position(&self, pos: &Position, op: &OperationKind) {
        if let Some(g) = self.common_gauges.get("mr_position") {
            g.with_label_values(&[pos.kind.as_ref(), op.as_ref()]).set(pos.price);
        }
    }

    fn log_all_with_state(&self, gauges: &[StateIndicatorFn], state: &MeanRevertingState) {
        for (state_gauge_name, state_gauge_fn) in gauges {
            if let Some(g) = self.common_gauges.get(state_gauge_name) {
                g.with_label_values(&[]).set(state_gauge_fn(state))
            }
        }
    }

    pub(super) fn log_state(&self, state: &MeanRevertingState) {
        self.log_all_with_state(&self.state_indicator_fns, state);
    }

    pub(super) fn log_thresholds(&self, state: &MeanRevertingState) {
        self.log_all_with_state(&self.threshold_indicator_fns, state);
    }

    pub(super) fn log_row(&self, lr: &SinglePosRow) {
        if let Some(g) = self.common_gauges.get("mr_mid") {
            g.with_label_values(&[]).set(lr.pos.mid);
        };
    }

    pub(super) fn log_error(&self, label: &str) { self.error_counter.with_label_values(&[label]).inc(); }

    pub(super) fn log_is_trading(&self, trading: bool) {
        self.status_gauge
            .with_label_values(&[])
            .set(if trading { 1.0 } else { 0.0 });
    }
}
