use super::state::MovingState;
use super::state::OperationKind;
use crate::naive_pair_trading::data_table::DataRow;
use crate::naive_pair_trading::state::Position;
use prometheus::{GaugeVec, Opts, Registry};
use std::collections::HashMap;

pub struct StrategyMetrics {
    gauges: HashMap<String, GaugeVec>,
    state_gauges: Vec<(String, fn(&MovingState) -> f64)>,
}

impl StrategyMetrics {
    pub fn for_strat(registry: &Registry, left_pair: &str, right_pair: &str) -> StrategyMetrics {
        let mut gauges: HashMap<String, GaugeVec> = HashMap::new();

        {
            let pos_labels = &["pair", "as", "pos", "op"];
            let pos_gauge_names = vec!["position"];
            for gauge_name in pos_gauge_names {
                let gauge_vec_opts = Opts::new(gauge_name, &format!("gauge for {}", gauge_name))
                    .const_label("left_pair", left_pair)
                    .const_label("right_pair", right_pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        {
            let pos_labels = &[];
            let right_mid_names = vec!["left_mid", "right_mid"];
            for gauge_name in right_mid_names {
                let gauge_vec_opts = Opts::new(gauge_name, &format!("gauge for {}", gauge_name))
                    .const_label("left_pair", left_pair)
                    .const_label("right_pair", right_pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        let state_gauges: Vec<(String, fn(&MovingState) -> f64)> = vec![
            ("return".to_string(), |x| {
                x.short_position_return() + x.long_position_return()
            }),
            ("predicted_right".to_string(), |x| x.predicted_right()),
            ("relative_spread".to_string(), |x| x.res()),
            ("pnl".to_string(), |x| x.pnl()),
            ("nominal_position".to_string(), |x| x.nominal_position()),
            ("beta".to_string(), |x| x.beta_lr()),
        ];
        {
            let pos_labels = &[];
            for (gauge_name, _) in state_gauges.clone() {
                let string = format!("state gauge for {}", gauge_name.clone());
                let gauge_vec_opts = Opts::new(&gauge_name, &string)
                    .const_label("left_pair", left_pair)
                    .const_label("right_pair", right_pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.clone(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        StrategyMetrics {
            gauges,
            state_gauges: state_gauges.clone(),
        }
    }

    pub(super) fn log_position(&self, pos: &Position, op: &OperationKind) {
        self.gauges.get("position").map(|g| {
            g.with_label_values(&[&pos.left_pair, "left", pos.kind.as_ref(), op.as_ref()])
                .set(pos.left_price);
            g.with_label_values(&[&pos.right_pair, "right", pos.kind.as_ref(), op.as_ref()])
                .set(pos.right_price);
        });
    }

    pub(super) fn log_state(&self, state: &MovingState) {
        for (state_gauge_name, state_gauge_fn) in &self.state_gauges {
            self.gauges
                .get(state_gauge_name)
                .map(|g| g.with_label_values(&[]).set(state_gauge_fn(state)));
        }
    }

    pub(super) fn log_row(&self, lr: &DataRow) {
        self.gauges
            .get("left_mid")
            .map(|g| g.with_label_values(&[]).set(lr.left.mid));
        self.gauges
            .get("right_mid")
            .map(|g| g.with_label_values(&[]).set(lr.right.mid));
    }
}
