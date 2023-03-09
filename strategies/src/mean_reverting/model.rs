use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use serde::ser::SerializeStruct;
use serde::ser::Serializer;

use brokers::types::{MarketEvent, MarketEventEnvelope};
use db::Storage;
use ext::ResultExt;
use stats::indicators::ppo::PercentPriceOscillator;
use stats::indicators::thresholds::Thresholds;
use strategy::error::{Error, Result};
use strategy::models::indicator_windowed_model::IndicatorWindowedModel;
use strategy::models::io::{IterativeModel, LoadableModel};
use strategy::models::{IndicatorModel, Sampler, TimedValue, WindowedModel};
use strategy::prelude::*;
use util::time::utc_zero;

use super::options::Options;

#[derive(Debug)]
pub struct MeanRevertingModel {
    sampler: Sampler,
    ppo: IndicatorModel<PercentPriceOscillator, f64>,
    thresholds: Option<IndicatorWindowedModel<f64, Thresholds>>,
    thresholds_0: (f64, f64),
}

impl MeanRevertingModel {
    pub fn new(n: &Options, db: Arc<dyn Storage>) -> Self {
        let ppo = PercentPriceOscillator::new(n.long_window_size, n.short_window_size);
        let ppo_model = IndicatorModel::new(&format!("model_{}", n.pair), db.clone(), ppo);
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
            sampler: Sampler::new(n.sample_freq, utc_zero()),
            ppo: ppo_model,
            thresholds: threshold_table,
            thresholds_0: (n.threshold_short, n.threshold_long),
        }
    }

    pub fn next(&mut self, e: &MarketEventEnvelope) -> Result<()> {
        let event = &e.e;
        if !self.sampler.sample(event.time()) {
            return Ok(());
        }
        let ob = match event {
            MarketEvent::Orderbook(ob) if ob.has_bids_and_asks() => ob,
            _ => return Ok(()),
        };
        let vwap = ob.vwap().unwrap();
        self.ppo
            .update(vwap)
            .err_into()
            .and_then(|_| {
                self.ppo
                    .value()
                    .ok_or_else(|| Error::ModelLoadError("no mean reverting model value".to_string()))
            })
            .map_err(|e| {
                tracing::debug!(err = %e, "failed to update ppo");
                e
            })?;
        if let Some(ppo) = self.ppo.value().map(|m| m.ppo) {
            if let Some(t) = self.thresholds.as_mut() {
                t.push(ppo);
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
            self.ppo.try_load()?;
            if let Some(_model_time) = self.ppo.last_model_time() {
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
        self.ppo.is_loaded() && self.thresholds.as_ref().map_or(true, Model::is_loaded)
    }

    // TODO: use this in the new trait that will be returned to the driver
    #[allow(dead_code)]
    pub(crate) fn reset(&mut self, name: Option<&str>) -> Result<()> {
        if name == Some("ppo") || name.is_none() {
            self.ppo.wipe()?;
        }
        if name == Some("thresholds") || name.is_none() {
            self.thresholds.as_mut().map(Model::wipe).transpose()?;
        }
        Ok(())
    }

    pub(crate) fn values(&self) -> Vec<(String, Option<serde_json::Value>)> {
        vec![
            (
                "ppo".to_string(),
                self.ppo.value().and_then(|v| serde_json::to_value(v.ppo).ok()),
            ),
            (
                "high".to_string(),
                self.thresholds
                    .as_ref()
                    .and_then(|t| t.value().and_then(|m| serde_json::to_value(m.high).ok())),
            ),
            (
                "low".to_string(),
                self.thresholds
                    .as_ref()
                    .and_then(|t| t.value().and_then(|m| serde_json::to_value(m.low).ok())),
            ),
        ]
    }

    pub(crate) fn ppo(&self) -> Option<f64> { self.ppo.value().map(|m| m.ppo) }

    pub(crate) fn ppo_value(&self) -> Option<PercentPriceOscillator> { self.ppo.value() }

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
        if let Some(model) = models.get("ppo") {
            self.ppo.import(model.clone())?;
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
        if let Some(model) = self.ppo.value() {
            ser_struct.serialize_field("ppo", &model)?;
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

    fn next_model(&mut self, e: &MarketEventEnvelope) -> Result<()> { self.next(e) }

    fn export_values(&self) -> Result<Self::ExportValue> { Ok(self.values().into_iter().collect()) }
}

#[cfg(all(test, feature = "backtests"))]
mod test {
    use std::fs::File;
    use std::io::{BufReader, BufWriter};
    use std::path::PathBuf;
    use std::sync::Arc;

    use chrono::{TimeZone, Utc};

    use brokers::exchange::Exchange;
    use db::MemoryKVStore;
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
            "binance",
            "order_books",
        )
        .await;
        // align data
        let options = Options::new_test_default(PAIR, Exchange::Binance);
        let memory_store = Arc::new(MemoryKVStore::new());
        let mut model = MeanRevertingModel::new(&options, memory_store);
        let mut model_values = vec![];

        for event in events {
            model.next(&event).unwrap();
            model_values.push(model.export_values().unwrap());
        }
        let results_dir = PathBuf::from(test_results_dir(module_path!()));
        let mut models_file_path = results_dir.clone();
        models_file_path.push("exported_model.json");
        let model_file = File::options()
            .create(true)
            .write(true)
            .open(models_file_path.clone())?;
        model.export(BufWriter::new(model_file), false).unwrap();
        let model_file = File::options().read(true).open(models_file_path).unwrap();
        model.import(BufReader::new(model_file), false).unwrap();
        model.try_load().unwrap();

        let mut model_values_file_path = results_dir;
        model_values_file_path.push("model_values.json");
        write_as_seq(model_values_file_path, model_values.as_slice()).unwrap();
        Ok(())
    }
}
