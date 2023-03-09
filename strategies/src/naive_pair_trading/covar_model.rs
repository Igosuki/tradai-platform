use std::ops::{Add, Mul, Sub};
use std::sync::Arc;

use chrono::{DateTime, Duration, TimeZone, Utc};
use itertools::Itertools;

use db::Storage;
use stats::iter::{CovarianceExt, MeanExt, VarianceExt};
use stats::Next;
use strategy::error::*;
use strategy::models::{Model, PersistentWindowedModel, Sampler, Window, WindowedModel};
use trading::book::BookPosition;
use util::time::{now, utc_zero};

#[derive(Debug, Copy, Clone, Serialize, Deserialize)]
pub struct DualBookPosition {
    pub time: DateTime<Utc>,
    pub left: BookPosition,
    pub right: BookPosition,
}

pub fn beta<'a, I>(i: I) -> f64
where
    I: Iterator<Item = &'a DualBookPosition>,
{
    let (var, covar) = i.tee();
    let variance: f64 = var.map(|r| r.left.mid).variance();
    trace!("variance {}", variance);
    let covariance: f64 = covar.map(|r| (r.left.mid, r.right.mid)).covariance::<(f64, f64), f64>();
    trace!("covariance {}", covariance);
    let beta_val = covariance / variance;
    trace!("beta_val {}", beta_val);
    beta_val
}

pub fn alpha<'a, I>(i: I, beta_val: f64) -> f64
where
    I: Iterator<Item = &'a DualBookPosition>,
{
    let (left, right) = i.tee();
    let mean_left: f64 = left.map(|l| l.left.mid).mean();
    trace!("mean left {}", mean_left);
    let mean_right: f64 = right.map(|l| l.right.mid).mean();
    trace!("mean right {}", mean_right);
    mean_right - beta_val * mean_left
}

#[derive(Debug, Copy, Clone, Deserialize, Serialize, Default)]
pub struct LinearModelValue {
    pub beta: f64,
    pub alpha: f64,
}

pub fn linear_model<'a>(m: &'a mut LinearModelValue, i: Window<'_, DualBookPosition>) -> &'a LinearModelValue {
    let (a, b) = i.tee();
    let beta = beta(a);
    let alpha = alpha(b, beta);
    *m = LinearModelValue { beta, alpha };
    m
}

pub fn predict(alpha: f64, beta: f64, value: f64) -> f64 { alpha + beta * value }

const LM_AGE_CUTOFF_RATIO: f64 = 0.0013;

#[derive(Debug)]
pub struct LinearSpreadModel {
    sampler: Sampler,
    linear_model: PersistentWindowedModel<DualBookPosition, LinearModelValue>,
    last_sample_time_at_eval: DateTime<Utc>,
    beta_eval_freq: i32,
}

impl LinearSpreadModel {
    pub(super) fn new(
        db: Arc<dyn Storage>,
        id: &str,
        window_size: usize,
        sample_freq: Duration,
        eval_freq: i32,
    ) -> Self {
        Self {
            sampler: Sampler::new(sample_freq, utc_zero()),
            linear_model: PersistentWindowedModel::new(id, db, window_size, None, linear_model, None),
            last_sample_time_at_eval: utc_zero(),
            beta_eval_freq: eval_freq,
        }
    }

    pub(super) fn should_eval(&self, event_time: DateTime<Utc>) -> bool {
        let model_time = self.last_sample_time_at_eval;
        let sample_freq = self.sampler.freq();
        self.linear_model.value().map_or(true, |lm| {
            let mt_obsolescence = if lm.beta < 0.0 {
                // When beta is negative the evaluation frequency is ten times lower
                model_time.add(sample_freq.mul(self.beta_eval_freq / 10))
            } else {
                // Model obsolescence is defined here as event time being greater than the sample window
                model_time.add(sample_freq.mul(self.beta_eval_freq))
            };
            let should_eval_model = event_time.ge(&mt_obsolescence);
            if should_eval_model {
                trace!(
                    "model obsolete, eval time reached : {} > {} with model_time = {}, beta_val = {}",
                    event_time,
                    mt_obsolescence,
                    model_time,
                    lm.beta
                );
            }
            should_eval_model
        })
    }

    pub(super) fn predict(&self, current_price: f64) -> Option<f64> {
        self.linear_model.value().map(|lm| {
            let predicted = predict(lm.alpha, lm.beta, current_price);
            trace!(
                alpha = lm.alpha,
                beta = lm.beta,
                current_price = current_price,
                predicted = predicted
            );
            predicted
        })
    }

    pub(super) fn beta(&self) -> Option<f64> { self.linear_model.value().map(|lm| lm.beta) }

    #[allow(dead_code)]
    pub(super) fn alpha(&self) -> Option<f64> { self.linear_model.value().map(|lm| lm.alpha) }

    pub(super) fn update(&mut self) -> Result<()> {
        self.last_sample_time_at_eval = self.sampler.last_sample_time();
        self.linear_model.update()
    }

    pub fn try_load(&mut self) -> strategy::error::Result<()> {
        self.linear_model.try_load()?;
        self.last_sample_time_at_eval = self.linear_model.last_model_time().unwrap_or_else(utc_zero).unwrap();
        trace!(loaded_model_time = %self.last_sample_time_at_eval, "model loaded");
        if self.linear_model.is_loaded() {
            Ok(())
        } else {
            Err(strategy::error::Error::ModelLoadError(
                "models not loaded for unknown reasons".to_string(),
            ))
        }
    }

    /// Check that model is more recent than sample freq * (eval frequency + `CUTOFF_RATIO`%)
    #[allow(clippy::cast_possible_truncation, clippy::cast_lossless)]
    pub(super) fn is_obsolete(&self) -> bool {
        self.linear_model.last_model_time().map_or(false, |at| {
            at.gt(&now().sub(
                self.sampler
                    .freq()
                    .mul((self.beta_eval_freq as f64 * (1.0 + LM_AGE_CUTOFF_RATIO)) as i32),
            ))
        })
    }

    pub(super) fn has_model(&self) -> bool { self.linear_model.has_model() && self.linear_model.is_loaded() }

    pub(super) fn value(&self) -> Option<LinearModelValue> { self.linear_model.value() }

    #[allow(dead_code)]
    pub(super) fn reset(&mut self) -> Result<()> { self.linear_model.wipe() }

    pub(super) fn push(&mut self, input: DualBookPosition) { self.linear_model.push(input); }

    pub(crate) fn serialized(&self) -> Vec<(String, Option<serde_json::Value>)> {
        vec![
            (
                "beta".to_string(),
                self.linear_model
                    .value()
                    .and_then(|v| serde_json::to_value(v.beta).ok()),
            ),
            (
                "alpha".to_string(),
                self.linear_model
                    .value()
                    .and_then(|v| serde_json::to_value(v.alpha).ok()),
            ),
        ]
    }
}

impl Next<DualBookPosition> for LinearSpreadModel {
    type Output = Result<Option<LinearModelValue>>;

    fn next(&mut self, input: DualBookPosition) -> Self::Output {
        if !self.sampler.sample(input.time) {
            return Ok(None);
        }
        self.push(input);
        if self.linear_model.value().is_none() && self.linear_model.is_filled() {
            self.update()?;
        }
        Ok(self.linear_model.value())
    }
}
