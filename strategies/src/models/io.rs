use std::io::{Read, Write};

use coinnect_rt::types::MarketEventEnvelope;
use serde::Serialize;

use crate::error::Result;

pub trait LoadableModel {
    /// Overwrite the current model using the reader
    /// If true, will read the model as snappy compressed
    /// Warning : in case of failure, this may lead to loss of data
    fn import<R: Read>(&mut self, read: R, compressed: bool) -> Result<()>;

    /// Write the current model value,
    /// If true, will write the model as snappy compressed
    fn export<W: Write>(&self, out: W, compressed: bool) -> Result<()>;
}

pub trait IterativeModel {
    type ExportValue: Serialize;

    /// Generate the next model value from the input event
    fn next_model(&mut self, e: &MarketEventEnvelope) -> Result<()>;

    /// Serialize a short, readable version of the model
    fn export_values(&self) -> Result<Self::ExportValue>;
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};

    use coinnect_rt::exchange::Exchange;
    use db::MemoryKVStore;
    use util::serde::write_as_seq;
    use util::test::test_results_dir;

    use crate::error::Result;
    use crate::mean_reverting::model::MeanRevertingModel;
    use crate::mean_reverting::options::Options;
    use crate::models::io::{IterativeModel, LoadableModel};
    use crate::test_util::init;
    use crate::test_util::input;

    const PAIR: &str = "BTC_USDT";

    #[tokio::test]
    async fn test_lodable_model_round_trip() -> Result<()> {
        init();
        let events = input::load_csv_events(
            Utc.ymd(2021, 8, 1),
            Utc.ymd(2021, 8, 9),
            vec![PAIR],
            "Binance",
            "order_books",
        )
        .await;
        // align data
        let options = Options::new_test_default(PAIR, Exchange::Binance);
        let memory_store = Arc::new(MemoryKVStore::new());
        let mut model = MeanRevertingModel::new(&options, memory_store);
        let mut model_values = vec![];

        for event in events {
            model.next(&event.e).unwrap();
            model_values.push(model.export_values().unwrap());
        }
        let results_dir = PathBuf::from(test_results_dir(module_path!()));
        let mut models_file_path = results_dir.clone();
        models_file_path.push("exported_model.json");
        let model_file = std::fs::OpenOptions::new()
            .create(true)
            .write(true)
            .open(models_file_path.clone())?;
        model.export(model_file, false).unwrap();
        let model_file = std::fs::OpenOptions::new().read(true).open(models_file_path).unwrap();
        model.import(model_file, false).unwrap();
        model.try_load().unwrap();

        let mut model_values_file_path = results_dir;
        model_values_file_path.push("model_values.json");
        write_as_seq(model_values_file_path, model_values.as_slice());
        Ok(())
    }
}
