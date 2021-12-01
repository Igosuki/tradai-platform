use crate::types::{OperationEvent, TradeEvent};

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
pub fn write_csv<I, J>(path: String, header: &[&str], records: I)
where
    I: Iterator<Item = J>,
    J: IntoIterator<Item = String>,
{
    let mut writer = csv::Writer::from_path(path).unwrap();
    writer.write_record(header).unwrap();
    for record in records {
        writer.write_record(record).unwrap();
    }
    writer.flush().unwrap();
}
