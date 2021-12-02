use std::cmp::{max, min};
use std::collections::{BTreeMap, HashMap};
use std::io::{BufReader, BufWriter, Read, Write};
use std::sync::Arc;

use chrono::{TimeZone, Utc};
use itertools::Itertools;
use ordered_float::OrderedFloat;
use serde::ser::SerializeStruct;
use serde::ser::Serializer;

use coinnect_rt::types::{MarketEvent, MarketEventEnvelope};
use db::Storage;
use ext::ResultExt;
use stats::indicators::macd_apo::MACDApo;
use stats::iter::QuantileExt;
use trading::book::BookPosition;

use crate::error::Result;
use crate::mean_reverting::options::Options;
use crate::models::io::{IterativeModel, LoadableModel};
use crate::models::{IndicatorModel, PersistentWindowedModel, Sampler, TimedValue, Window, WindowedModel};
use crate::Model;

pub fn ema_indicator_model(
    pair: &str,
    db: Arc<dyn Storage>,
    short_window_size: u32,
    long_window_size: u32,
) -> IndicatorModel<MACDApo, f64> {
    let init = MACDApo::new(long_window_size, short_window_size);
    IndicatorModel::new(&format!("model_{}", pair), db, init)
}

#[derive(Clone, Copy, Serialize, Deserialize, Debug, Default)]
pub struct ApoThresholds {
    pub short_0: f64,
    pub long_0: f64,
    pub long: f64,
    pub short: f64,
}

impl ApoThresholds {
    pub fn new(short_0: f64, long_0: f64) -> Self {
        Self {
            short_0,
            long_0,
            long: 0.0,
            short: 0.0,
        }
    }
}

pub fn threshold(m: &ApoThresholds, wdw: Window<'_, f64>) -> ApoThresholds {
    let (threshold_short_iter, threshold_long_iter) = wdw.tee();
    let threshold_short = max(m.short_0.into(), OrderedFloat(threshold_short_iter.quantile(0.99))).into();
    let threshold_long = min(m.long_0.into(), OrderedFloat(threshold_long_iter.quantile(0.01))).into();
    ApoThresholds {
        short: threshold_short,
        long: threshold_long,
        ..*m
    }
}

#[derive(Debug)]
pub struct MeanRevertingModel {
    sampler: Sampler,
    apo: IndicatorModel<MACDApo, f64>,
    thresholds: Option<PersistentWindowedModel<f64, ApoThresholds>>,
    thresholds_0: (f64, f64),
}

impl MeanRevertingModel {
    pub fn new(n: &Options, db: Arc<dyn Storage>) -> Self {
        let ema_model = ema_indicator_model(n.pair.as_ref(), db.clone(), n.short_window_size, n.long_window_size);
        let threshold_table = if n.dynamic_threshold() {
            n.threshold_window_size.map(|thresold_window_size| {
                PersistentWindowedModel::new(
                    &format!("thresholds_{}", n.pair.as_ref()),
                    db.clone(),
                    thresold_window_size,
                    Some(thresold_window_size * 2),
                    threshold,
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
                    t.update().map_err(|e| {
                        tracing::debug!(err = %e, "failed to update thresholds");
                        e
                    })?;
                }
            }
        }
        Ok(())
    }

    pub fn try_load(&mut self) -> crate::error::Result<()> {
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

    pub(crate) fn is_loaded(&self) -> bool {
        self.apo.is_loaded() && self.thresholds.as_ref().map(|t| t.is_loaded()).unwrap_or(true)
    }

    pub(crate) fn reset(&mut self, name: Option<String>) -> Result<()> {
        if name == Some("apo".to_string()) || name.is_none() {
            self.apo.wipe()?;
        }
        if name == Some("thresholds".to_string()) || name.is_none() {
            self.thresholds.as_mut().map(|t| t.wipe()).transpose()?;
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
                    .and_then(|t| t.value().and_then(|m| serde_json::to_value(m.short).ok())),
            ),
            (
                "threshold_long".to_string(),
                self.thresholds
                    .as_ref()
                    .and_then(|t| t.value().and_then(|m| serde_json::to_value(m.long).ok())),
            ),
        ]
    }

    pub(crate) fn apo(&self) -> Option<f64> { self.apo.value().map(|m| m.apo) }

    pub(crate) fn apo_value(&self) -> Option<MACDApo> { self.apo.value() }

    pub(crate) fn thresholds(&self) -> (f64, f64) {
        match self.thresholds.as_ref() {
            Some(t) if t.is_filled() => t.value().map(|m| (m.short, m.long)),
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
