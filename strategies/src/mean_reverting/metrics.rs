use crate::mean_reverting::MeanRevertingState;
use prometheus::{GaugeVec, Opts, Registry};
use std::collections::HashMap;

type StateGauge = (String, fn(&MeanRevertingState) -> f64);

pub struct MeanRevertingStrategyMetrics {
    gauges: HashMap<String, GaugeVec>,
    state_gauges: Vec<StateGauge>,
}

impl MeanRevertingStrategyMetrics {
    pub fn for_strat(registry: &Registry, pair: &str) -> MeanRevertingStrategyMetrics {
        let mut gauges: HashMap<String, GaugeVec> = HashMap::new();

        {
            let pos_labels = &["pair", "as", "pos", "op"];
            let pos_gauge_names = vec!["mr_position", "mr_mid"];
            for gauge_name in pos_gauge_names {
                let gauge_vec_opts = Opts::new(gauge_name, &format!("gauge for {}", gauge_name))
                    .const_label("pair", pair);
                let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
                gauges.insert(gauge_name.to_string(), gauge_vec.clone());
                registry.register(Box::new(gauge_vec.clone())).unwrap();
            }
        }
        // let state_gauges: Vec<StateGauge> = vec![
        //     ("return".to_string(), |x| {
        //         x.short_position_return() + x.long_position_return()
        //     }),
        //     ("predicted_right".to_string(), |x| x.predicted_right()),
        //     ("relative_spread".to_string(), |x| x.res()),
        //     ("pnl".to_string(), |x| x.pnl()),
        //     ("nominal_position".to_string(), |x| x.nominal_position()),
        //     ("beta".to_string(), |x| x.beta_lr()),
        // ];
        // {
        //     let pos_labels = &[];
        //     for (gauge_name, _) in state_gauges.clone() {
        //         let string = format!("state gauge for {}", gauge_name.clone());
        //         let gauge_vec_opts = Opts::new(&gauge_name, &string)
        //             .const_label("left_pair", left_pair)
        //             .const_label("right_pair", right_pair);
        //         let gauge_vec = GaugeVec::new(gauge_vec_opts, pos_labels).unwrap();
        //         gauges.insert(gauge_name.clone(), gauge_vec.clone());
        //         registry.register(Box::new(gauge_vec.clone())).unwrap();
        //     }
        // }
        let state_gauges: Vec<StateGauge> = vec![];
        MeanRevertingStrategyMetrics {
            gauges,
            state_gauges: state_gauges.clone(),
        }
    }

    pub(super) fn log_state(&self, state: &MeanRevertingState) {
        for (state_gauge_name, state_gauge_fn) in &self.state_gauges {
            if let Some(g) = self.gauges.get(state_gauge_name) {
                g.with_label_values(&[]).set(state_gauge_fn(state))
            }
        }
    }
}
