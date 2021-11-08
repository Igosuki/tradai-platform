#[cfg(test)]
mod test {
    use std::collections::{BTreeMap, HashMap};
    use std::io::{BufReader, BufWriter, Read, Write};
    use std::path::{Path, PathBuf};
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};
    use serde::ser::{SerializeSeq, SerializeStruct};
    use serde::{Serialize, Serializer};

    use coinnect_rt::exchange::Exchange;
    use db::{MemoryKVStore, Storage};
    use ext::ResultExt;
    use math::indicators::macd_apo::MACDApo;
    use util::test::test_results_dir;

    use crate::error::Result;
    use crate::generic::InputEvent;
    use crate::mean_reverting::model;
    use crate::mean_reverting::model::{ema_indicator_model, ApoThresholds};
    use crate::mean_reverting::options::Options;
    use crate::models::persist::TimedValue;
    use crate::models::{IndicatorModel, Sampler, WindowedModel};
    use crate::test_util::init;
    use crate::{input, Model};

    struct MeanRevertingModel {
        sampler: Sampler,
        apo: IndicatorModel<MACDApo, f64>,
        thresholds: Option<WindowedModel<f64, ApoThresholds>>,
    }

    impl MeanRevertingModel {
        pub fn new(n: Options, db: Arc<dyn Storage>) -> Self {
            let ema_model = ema_indicator_model(n.pair.as_ref(), db.clone(), n.short_window_size, n.long_window_size);
            let threshold_table = if n.dynamic_threshold() {
                n.threshold_window_size.map(|thresold_window_size| {
                    WindowedModel::new(
                        &format!("thresholds_{}", n.pair.as_ref()),
                        db.clone(),
                        thresold_window_size,
                        Some(thresold_window_size * 2),
                        model::threshold,
                        Some(ApoThresholds::new(n.threshold_short, n.threshold_long)),
                    )
                })
            } else {
                None
            };
            Self {
                sampler: Sampler::new(n.sample_freq(), Utc.timestamp_millis(0)),
                apo: ema_model,
                thresholds: threshold_table,
            }
        }
        pub fn next(&mut self, e: InputEvent) -> Result<()> {
            let book_pos = match e {
                InputEvent::BookPosition(bp) => bp,
                _ => return Ok(()),
            };
            if !self.sampler.sample(book_pos.event_time) {
                return Ok(());
            }
            self.apo
                .update(book_pos.mid)
                .err_into()
                .and_then(|_| {
                    self.apo
                        .value()
                        .ok_or_else(|| crate::error::Error::ModelLoadError("no mean reverting model value".to_string()))
                })
                .map_err(|e| {
                    tracing::debug!(err = %e, "failed to update apo");
                    e
                })?;
            if let Some(apo) = self.apo.value().map(|m| m.apo) {
                if let Some(t) = self.thresholds.as_mut() {
                    t.push(&apo);
                    if t.is_filled() {
                        t.update_model().map_err(|e| {
                            tracing::debug!(err = %e, "failed to update thresholds");
                            e
                        })?;
                    }
                }
            }
            Ok(())
        }

        pub fn load(&mut self) -> crate::error::Result<()> {
            {
                self.apo.try_load()?;
                if let Some(_model_time) = self.apo.last_model_time() {
                    //self.sampler.set_last_time(model_time);
                }
            }
            {
                if let Some(threshold_table) = &mut self.thresholds {
                    threshold_table.try_load()?;
                }
            }
            if !self.is_loaded() {
                Err(crate::error::Error::ModelLoadError(
                    "models still not loaded loading".to_string(),
                ))
            } else {
                Ok(())
            }
        }

        fn is_loaded(&self) -> bool {
            self.apo.is_loaded() && self.thresholds.as_ref().map(|t| t.is_loaded()).unwrap_or_else(|| false)
        }

        fn reset(&mut self, name: Option<String>) -> Result<()> {
            if name == Some("apo".to_string()) || name.is_none() {
                self.apo.wipe()?;
            }
            if name == Some("thresholds".to_string()) || name.is_none() {
                self.thresholds.as_mut().map(|t| t.wipe()).transpose()?;
            }
            Ok(())
        }

        fn values(&self) -> Vec<(String, Option<serde_json::Value>)> {
            vec![
                (
                    "apo".to_string(),
                    self.apo.value().and_then(|v| serde_json::to_value(v.apo).ok()),
                ),
                (
                    "thresholds".to_string(),
                    self.thresholds
                        .as_ref()
                        .and_then(|t| t.model().and_then(|m| serde_json::to_value(m.value).ok())),
                ),
            ]
        }
    }

    impl LoadableModel for MeanRevertingModel {
        fn import<R: Read>(&mut self, read: R, compressed: bool) -> Result<()> {
            let reader: Box<dyn std::io::Read> = if compressed {
                Box::new(snap::read::FrameDecoder::new(read))
            } else {
                Box::new(BufReader::new(read))
            };
            let models: HashMap<String, serde_json::Value> = serde_json::from_reader(reader)?;
            if let Some(model) = models.get("apo") {
                self.apo.import(model.to_owned())?;
            }
            if let (Some(thresholds_model), Some(thresholds_table)) =
                (models.get("thresholds"), models.get("thresholds_table"))
            {
                if let Some(thresholds) = self.thresholds.as_mut() {
                    thresholds.import(thresholds_model.to_owned(), thresholds_table.to_owned())?;
                }
            }
            Ok(())
        }

        fn export<W: Write>(&self, out: W, compressed: bool) -> Result<()> {
            let writer: Box<dyn std::io::Write> = if compressed {
                Box::new(snap::write::FrameEncoder::new(out))
            } else {
                Box::new(BufWriter::new(out))
            };
            let mut ser = serde_json::Serializer::new(writer);
            let mut ser_struct = ser.serialize_struct("MeanRevertingModel", 3).unwrap();
            if let Some(model) = self.apo.value() {
                ser_struct.serialize_field("apo", &model)?;
            }
            if let Some(windowed_model) = &self.thresholds {
                if let Some(model) = windowed_model.model() {
                    ser_struct.serialize_field("thresholds", &model.value)?;
                }
                ser_struct.serialize_field(
                    "thresholds_table",
                    &windowed_model.timed_window().collect::<Vec<&TimedValue<f64>>>(),
                )?;
            }
            SerializeStruct::end(ser_struct)?;
            Ok(())
        }
    }

    impl IterativeModel for MeanRevertingModel {
        type ExportValue = BTreeMap<String, Option<serde_json::Value>>;

        fn next(&mut self, e: InputEvent) -> Result<()> { self.next(e) }

        fn export_values(&self) -> Result<Self::ExportValue> { Ok(self.values().into_iter().collect()) }
    }

    trait LoadableModel {
        /// Overwrite the current model using the reader
        /// If true, will read the model as snappy compressed
        /// Warning : in case of failure, this may lead to loss of data
        fn import<R: Read>(&mut self, read: R, compressed: bool) -> Result<()>;

        /// Write the current model value,
        /// If true, will write the model as snappy compressed
        fn export<W: Write>(&self, out: W, compressed: bool) -> Result<()>;
    }

    trait IterativeModel {
        type ExportValue: Serialize;

        /// Generate the next model value from the input event
        fn next(&mut self, e: InputEvent) -> Result<()>;

        /// Serialize a short, readable version of the model
        fn export_values(&self) -> Result<Self::ExportValue>;
    }

    fn write_as_seq<P: AsRef<Path>, T: Serialize>(out_file: P, all_models: Vec<T>) {
        let logs_f = std::fs::File::create(out_file).unwrap();
        let mut ser = serde_json::Serializer::new(BufWriter::new(logs_f));
        let mut seq = ser.serialize_seq(None).unwrap();
        for models in all_models {
            seq.serialize_element(&models).unwrap();
        }
        SerializeSeq::end(seq).unwrap();
    }

    const PAIR: &str = "BTC_USDT";

    #[tokio::test]
    async fn test_lodable_model_round_trip() -> Result<()> {
        init();
        let csv_records = input::load_csv_records(
            Utc.ymd(2021, 8, 1),
            Utc.ymd(2021, 8, 9),
            vec![PAIR],
            "Binance",
            "order_books",
        )
        .await;
        let _num_records = csv_records.len();
        // align data
        let pair_csv_records = csv_records[0].iter();
        let options = Options::new_test_default(PAIR, Exchange::Binance);
        let memory_store = Arc::new(MemoryKVStore::new());
        let mut model = MeanRevertingModel::new(options, memory_store);
        let mut model_values = vec![];

        for csvr in pair_csv_records {
            model.next(InputEvent::BookPosition(csvr.into())).unwrap();
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
        model.load().unwrap();
        eprintln!("model = {:?}", model.values());

        let mut model_values_file_path = results_dir;
        model_values_file_path.push("model_values.json");
        write_as_seq(model_values_file_path, model_values.clone());
        Ok(())
    }
}
