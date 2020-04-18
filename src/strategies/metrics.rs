use super::state::MovingState;
use super::state::Operation;
use super::state::PositionKind;
use prometheus::{GaugeVec, Opts, Registry};
use std::collections::HashMap;

pub struct StrategyMetrics {
    gauges: HashMap<String, GaugeVec>,
}

impl StrategyMetrics {
    pub fn for_strat(registry: &Registry, left_pair: &str, right_pair: &str) -> StrategyMetrics {
        let labels = &["pos", "op"];
        let gauge_names = vec!["position"];
        let mut gauges: HashMap<String, GaugeVec> = HashMap::new();
        for gauge_name in gauge_names {
            let gauge_vec_opts = Opts::new(gauge_name, &format!("gauge for {}", gauge_name))
                .const_label("left_pair", left_pair)
                .const_label("right_pair", right_pair);
            let gauge_vec = GaugeVec::new(gauge_vec_opts, labels).unwrap();
            gauges.insert(gauge_name.to_string(), gauge_vec.clone());
            registry.register(Box::new(gauge_vec.clone())).unwrap();
        }
        StrategyMetrics { gauges }
    }

    #[allow(dead_code)]
    pub(super) fn log_position(&self, pos: PositionKind, op: Operation, state: MovingState) {
        self.gauges.get("position").map(|g| {
            g.with_label_values(&[&format!("{}", pos), &format!("{}", op)])
                .set(state.predicted_right())
        });
    }
}
