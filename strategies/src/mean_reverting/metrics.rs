use crate::mean_reverting::ema_model::SinglePosRow;
use crate::mean_reverting::state::{MeanRevertingState, Position};
use crate::types::OperationKind;
use prometheus::{GaugeVec, Opts, Registry};
use std::collections::HashMap;

type StateGauge = (String, fn(&MeanRevertingState) -> f64);

pub struct MeanRevertingStrategyMetrics {
    common_gauges: HashMap<String, GaugeVec>,
    threshold_gauges: Vec<StateGauge>,
    state_gauges: Vec<StateGauge>,
}

impl MeanRevertingStrategyMetrics {
    pub fn for_strat(registry: &Registry, pair: &str) -> MeanRevertingStrategyMetrics {
        let mut gauges: HashMap<String, GaugeVec> = HashMap::new();

        {
            let pos_labels = &["pos", "op"];
            let pos_gauge_names = vec!["mr_position"];
            for gauge_name in pos_gauge_names {
                let gauge_vec_opts =
                    Opts::new(gauge_name, &format!("gauge for {}", gauge_name)).const_label("pair", pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }

        {
            let pos_labels = &[];
            let mid_names = vec!["mr_mid"];
            for gauge_name in mid_names {
                let gauge_vec_opts =
                    Opts::new(gauge_name, &format!("gauge for {}", gauge_name)).const_label("pair", pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }

        let threshold_gauges: Vec<StateGauge> = vec![
            ("mr_threshold_short".to_string(), |x: &MeanRevertingState| {
                x.threshold_short()
            }),
            ("mr_threshold_long".to_string(), |x: &MeanRevertingState| {
                x.threshold_long()
            }),
        ];
        {
            let pos_labels = &[];
            for (gauge_name, _) in threshold_gauges.clone() {
                let string = format!("threshold gauge for {}", gauge_name.clone());
                let gauge_vec_opts = Opts::new(&gauge_name, &string).const_label("pair", pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.clone(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }

        let state_gauges: Vec<StateGauge> = vec![];
        {
            let pos_labels = &[];
            for (gauge_name, _) in state_gauges.clone() {
                let string = format!("state gauge for {}", gauge_name.clone());
                let gauge_vec_opts = Opts::new(&gauge_name, &string).const_label("pair", pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.clone(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }

        MeanRevertingStrategyMetrics {
            common_gauges: gauges,
            threshold_gauges: threshold_gauges.clone(),
            state_gauges: state_gauges.clone(),
        }
    }

    pub(super) fn log_position(&self, pos: &Position, op: &OperationKind) {
        if let Some(g) = self.common_gauges.get("mr_position") {
            g.with_label_values(&[pos.kind.as_ref(), op.as_ref()]).set(pos.price);
        }
    }

    fn log_all_with_state(&self, gauges: &Vec<StateGauge>, state: &MeanRevertingState) {
        for (state_gauge_name, state_gauge_fn) in gauges {
            if let Some(g) = self.common_gauges.get(state_gauge_name) {
                g.with_label_values(&[]).set(state_gauge_fn(state))
            }
        }
    }

    pub(super) fn log_state(&self, state: &MeanRevertingState) { self.log_all_with_state(&self.state_gauges, state); }

    pub(super) fn log_thresholds(&self, state: &MeanRevertingState) {
        self.log_all_with_state(&self.threshold_gauges, state);
    }

    pub(super) fn log_row(&self, lr: &SinglePosRow) {
        if let Some(g) = self.common_gauges.get("mr_mid") {
            g.with_label_values(&[]).set(lr.pos.mid);
        };
    }
}
