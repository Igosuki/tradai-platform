use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde::ser::SerializeStruct;
use serde::ser::Serializer;

use ext::ResultExt;
use stats::indicators::macd_apo::MACDApo;
use stats::indicators::thresholds::Thresholds;
use strategy::coinnect::types::{MarketEvent, MarketEventEnvelope};
use strategy::db::Storage;
use strategy::error::{Error, Result};
use strategy::models::indicator_windowed_model::IndicatorWindowedModel;
use strategy::models::io::{IterativeModel, LoadableModel};
use strategy::models::{IndicatorModel, Sampler, TimedValue, WindowedModel};
use strategy::prelude::*;
use strategy::trading::book::BookPosition;

use super::options::Options;

#[derive(Debug)]
pub struct MeanRevertingModel {
    sampler: Sampler,
    apo: IndicatorModel<MACDApo, f64>,
    thresholds: Option<IndicatorWindowedModel<f64, Thresholds>>,
    thresholds_0: (f64, f64),
}

impl MeanRevertingModel {
    pub fn new(n: &Options, db: Arc<dyn Storage>) -> Self {
        let macd_apo = MACDApo::new(n.long_window_size, n.short_window_size);
        let ema_model = IndicatorModel::new(&format!("model_{}", n.pair), db.clone(), macd_apo);
        let threshold_table = if n.dynamic_threshold() {
            n.threshold_window_size.map(|thresold_window_size| {
                IndicatorWindowedModel::new(
                    &format!("thresholds_{}", n.pair.as_ref()),
                    db,
                    thresold_window_size,
                    Some(thresold_window_size * 2),
                    Thresholds::new(n.threshold_short, n.threshold_long),
                )
            })
        } else {
            None
        };
        Self {
            sampler: Sampler::new(n.sample_freq(), Utc.timestamp_millis(0)),
            apo: ema_model,
            thresholds: threshold_table,
            thresholds_0: (n.threshold_short, n.threshold_long),
        }
    }

    pub fn next(&mut self, e: &MarketEvent) -> Result<()> {
        let book_pos: BookPosition = if let MarketEvent::Orderbook(ob) = e {
            ob.try_into()?
        } else {
            return Ok(());
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
                    .ok_or_else(|| Error::ModelLoadError("no mean reverting model value".to_string()))
            })
            .map_err(|e| {
                tracing::debug!(err = %e, "failed to update apo");
                e
            })?;
        if let Some(apo) = self.apo.value().map(|m| m.apo) {
            if let Some(t) = self.thresholds.as_mut() {
                t.push(apo);
                if t.is_filled() {
                    t.update().map_err(|e| {
                        tracing::debug!(err = %e, "failed to update thresholds");
                        e
                    })?;
                }
            }
        }
        Ok(())
    }

    pub fn try_load(&mut self) -> strategy::error::Result<()> {
        {
            self.apo.try_load()?;
            if let Some(_model_time) = self.apo.last_model_time() {
                // TODO: set last sample time from loaded data
                //self.sampler.set_last_time(model_time);
            }
        }
        {
            if let Some(threshold_table) = &mut self.thresholds {
                threshold_table.try_load()?;
                if let Some(thresholds) = threshold_table.value().as_mut() {
                    thresholds.high_0 = self.thresholds_0.0;
                    thresholds.low_0 = self.thresholds_0.1;
                }
            }
        }
        if self.is_loaded() {
            Ok(())
        } else {
            Err(Error::ModelLoadError("models still not loaded loading".to_string()))
        }
    }

    pub(crate) fn is_loaded(&self) -> bool {
        self.apo.is_loaded() && self.thresholds.as_ref().map_or(true, Model::is_loaded)
    }

    // TODO: use this in the new trait that will be returned to the driver
    #[allow(dead_code)]
    pub(crate) fn reset(&mut self, name: Option<&str>) -> Result<()> {
        if name == Some("apo") || name.is_none() {
            self.apo.wipe()?;
        }
        if name == Some("thresholds") || name.is_none() {
            self.thresholds.as_mut().map(Model::wipe).transpose()?;
        }
        Ok(())
    }

    pub(crate) fn values(&self) -> Vec<(String, Option<serde_json::Value>)> {
        vec![
            (
                "apo".to_string(),
                self.apo.value().and_then(|v| serde_json::to_value(v.apo).ok()),
            ),
            (
                "short_ema".to_string(),
                self.apo
                    .value()
                    .and_then(|v| serde_json::to_value(v.short_ema.current).ok()),
            ),
            (
                "long_ema".to_string(),
                self.apo
                    .value()
                    .and_then(|v| serde_json::to_value(v.long_ema.current).ok()),
            ),
            (
                "threshold_short".to_string(),
                self.thresholds
                    .as_ref()
                    .and_then(|t| t.value().and_then(|m| serde_json::to_value(m.high).ok())),
            ),
            (
                "threshold_long".to_string(),
                self.thresholds
                    .as_ref()
                    .and_then(|t| t.value().and_then(|m| serde_json::to_value(m.low).ok())),
            ),
        ]
    }

    pub(crate) fn apo(&self) -> Option<f64> { self.apo.value().map(|m| m.apo) }

    pub(crate) fn apo_value(&self) -> Option<MACDApo> { self.apo.value() }

    pub(crate) fn thresholds(&self) -> (f64, f64) {
        match self.thresholds.as_ref() {
            Some(t) if t.is_filled() => t.value().map(|m| (m.high, m.low)),
            _ => None,
        }
        .unwrap_or(self.thresholds_0)
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
            self.apo.import(model.clone())?;
        }
        if let (Some(thresholds_model), Some(thresholds_table)) =
            (models.get("thresholds"), models.get("thresholds_table"))
        {
            if let Some(thresholds) = self.thresholds.as_mut() {
                thresholds.import(thresholds_model.clone(), thresholds_table.clone())?;
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
            if let Some(model) = windowed_model.value() {
                ser_struct.serialize_field("thresholds", &model)?;
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

    fn next_model(&mut self, e: &MarketEventEnvelope) -> Result<()> { self.next(&e.e) }

    fn export_values(&self) -> Result<Self::ExportValue> { Ok(self.values().into_iter().collect()) }
}

#[cfg(test)]
mod test {
    use std::path::PathBuf;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};

    use strategy::coinnect::exchange::Exchange;
    use strategy::db::MemoryKVStore;
    use strategy::error::Result;
    use strategy::models::io::{IterativeModel, LoadableModel};
    use strategy_test_util::init;
    use strategy_test_util::input;
    use util::ser::write_as_seq;
    use util::test::test_results_dir;

    use crate::mean_reverting::model::MeanRevertingModel;
    use crate::mean_reverting::options::Options;

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
        write_as_seq(model_values_file_path, model_values.as_slice()).unwrap();
        Ok(())
    }
}
