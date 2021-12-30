use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::Path;

use chrono::{DateTime, Utc};
use itertools::Itertools;
use serde_json::Value;
use strategy::models::io::SerializedModel;

use strategy::query::PortfolioSnapshot;
use strategy::types::{OperationEvent, TradeEvent};

use crate::draw::TimedEntry;

#[derive(Debug, Serialize, Clone, typed_builder::TypedBuilder)]
pub struct StrategyLog {
    pub event_time: DateTime<Utc>,
    pub prices: HashMap<(String, String), f64>,
    pub model: HashMap<String, Option<Value>>,
    pub snapshot: PortfolioSnapshot,
    pub nominal_positions: HashMap<(String, String), f64>,
}

impl StrategyLog {
    pub fn new(
        event_time: DateTime<Utc>,
        prices: HashMap<(String, String), f64>,
        model: SerializedModel,
        snapshot: PortfolioSnapshot,
        nominal_positions: HashMap<(String, String), f64>,
    ) -> StrategyLog {
        StrategyLog {
            event_time,
            prices,
            model: model.into_iter().collect(),
            snapshot,
            nominal_positions,
        }
    }
}

impl TimedEntry for StrategyLog {
    fn time(&self) -> DateTime<Utc> { self.event_time }
}

pub fn write_models(dest_dir: &str, log: &[StrategyLog]) {
    if let Some(last_model) = log.iter().last() {
        let model_keys = last_model
            .model
            .iter()
            .map(|v| v.0.clone())
            .sorted()
            .collect::<Vec<String>>();
        let mut csv_keys = vec!["ts", "portfolio_value"];
        csv_keys.extend(model_keys.iter().map(String::as_str));
        write_csv(
            format!("{}/model_values.csv", dest_dir),
            &csv_keys,
            log.iter().map(|r| {
                let mut values = vec![
                    r.event_time.format(util::time::TIMESTAMP_FORMAT).to_string(),
                    r.snapshot.value.to_string(),
                ];
                for k in &model_keys {
                    values.push(
                        r.model
                            .get(k)
                            .and_then(Option::as_ref)
                            .unwrap_or(&Value::Null)
                            .to_string(),
                    );
                }
                values
            }),
        );
    } else {
        error!("no model values to write");
    }
}

#[allow(dead_code)]
pub fn write_trade_events(test_results_dir: &str, trade_events: &[(OperationEvent, TradeEvent)]) {
    write_csv(
        format!("{}/trade_events.csv", test_results_dir),
        &[
            "ts",
            "pair",
            "op",
            "pos",
            "trade_kind",
            "price",
            "qty",
            "value_strat",
            "borrowed",
            "interest",
        ],
        trade_events.iter().map(|(o, t)| {
            vec![
                t.at.format(util::time::TIMESTAMP_FORMAT).to_string(),
                t.pair.clone(),
                o.op.as_ref().to_string(),
                o.pos.as_ref().to_string(),
                t.side.as_ref().to_string(),
                t.price.to_string(),
                t.qty.to_string(),
                t.strat_value.to_string(),
                t.borrowed.unwrap_or(0.0).to_string(),
                t.interest.unwrap_or(0.0).to_string(),
            ]
        }),
    );
}

#[allow(dead_code)]
/// # Panics
///
/// if the file cannot be created
pub fn write_csv<I, J, S>(path: S, header: &[&str], records: I)
where
    I: Iterator<Item = J>,
    J: IntoIterator<Item = String>,
    S: AsRef<Path>,
{
    let bfw = BufWriter::new(File::create(path).unwrap());
    let mut writer = csv::Writer::from_writer(bfw);
    writer.write_record(header).unwrap();
    for record in records {
        writer.write_record(record).unwrap();
    }
    writer.flush().unwrap();
}
