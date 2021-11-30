use crate::error::*;
use crate::models::{PersistentWindowedModel, Sampler, Window, WindowedModel};
use crate::naive_pair_trading::covar_model;
use crate::Model;
use chrono::{DateTime, Duration, TimeZone, Utc};
use db::Storage;
use itertools::Itertools;
use stats::iter::{CovarianceExt, MeanExt, VarianceExt};
use stats::Next;
use std::ops::{Add, Mul, Sub};
use std::sync::Arc;
use trading::book::BookPosition;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualBookPosition {
    pub time: DateTime<Utc>,
    pub left: BookPosition,
    pub right: BookPosition,
}

pub fn beta(i: Window<'_, DualBookPosition>) -> f64 {
    let (var, covar) = i.tee();
    let variance: f64 = var.map(|r| r.left.mid).variance();
    trace!("variance {}", variance);
    let covariance: f64 = covar.map(|r| (r.left.mid, r.right.mid)).covariance::<(f64, f64), f64>();
    trace!("covariance {}", covariance);
    let beta_val = covariance / variance;
    trace!("beta_val {}", beta_val);
    beta_val
}

pub fn alpha(i: Window<'_, DualBookPosition>, beta_val: f64) -> f64 {
    let (left, right) = i.tee();
    let mean_left: f64 = left.map(|l| l.left.mid).mean();
    trace!("mean left {}", mean_left);
    let mean_right: f64 = right.map(|l| l.right.mid).mean();
    trace!("mean right {}", mean_right);
    mean_right - beta_val * mean_left
}

#[derive(Debug, Deserialize, Serialize, Clone, Default)]
pub struct LinearModelValue {
    pub beta: f64,
    pub alpha: f64,
}

pub fn linear_model(_m: &LinearModelValue, i: Window<'_, DualBookPosition>) -> LinearModelValue {
    let beta = beta(i.clone());
    let alpha = alpha(i.clone(), beta);
    LinearModelValue { beta, alpha }
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
            sampler: Sampler::new(sample_freq, Utc.timestamp_millis(0)),
            linear_model: PersistentWindowedModel::new(id, db, window_size, None, covar_model::linear_model, None),
            last_sample_time_at_eval: Utc.timestamp_millis(0),
            beta_eval_freq: eval_freq,
        }
    }

    pub(super) fn should_eval(&self, event_time: DateTime<Utc>) -> bool {
        let model_time = self.last_sample_time_at_eval;
        let sample_freq = self.sampler.freq();
        self.linear_model
            .value()
            .map(|lm| {
                let mt_obsolescence = if lm.beta < 0.0 {
                    // When beta is negative the evaluation frequency is ten times lower
                    model_time.add(sample_freq.mul(self.beta_eval_freq / 10))
                } else {
                    // Model obsolescence is defined here as event time being greater than the sample window
                    model_time.add(sample_freq.mul(self.beta_eval_freq))
                };
                let should_eval_model = event_time.ge(&mt_obsolescence);
                if should_eval_model {
                    debug!(
                        "model obsolete, eval time reached : {} > {} with model_time = {}, beta_val = {}",
                        event_time, mt_obsolescence, model_time, lm.beta
                    );
                }
                should_eval_model
            })
            .unwrap_or(true)
    }

    pub(super) fn predict(&self, current_price: f64) -> Option<f64> {
        self.linear_model
            .value()
            .map(|lm| covar_model::predict(lm.alpha, lm.beta, current_price))
    }

    pub(super) fn beta(&self) -> Option<f64> { self.linear_model.value().map(|lm| lm.beta) }

    #[allow(dead_code)]
    pub(super) fn alpha(&self) -> Option<f64> { self.linear_model.value().map(|lm| lm.alpha) }

    pub(super) fn update(&mut self) -> Result<()> {
        self.last_sample_time_at_eval = self.sampler.last_sample_time();
        self.linear_model.update()
    }

    pub fn try_load(&mut self) -> crate::error::Result<()> {
        self.linear_model.try_load()?;
        self.last_sample_time_at_eval = self
            .linear_model
            .last_model_time()
            .unwrap_or_else(|| Utc.timestamp_millis(0));
        trace!(loaded_model_time = %self.last_sample_time_at_eval, "model loaded");
        if !self.linear_model.is_loaded() {
            Err(crate::error::Error::ModelLoadError(
                "models not loaded for unknown reasons".to_string(),
            ))
        } else {
            Ok(())
        }
    }

    /// Check that model is more recent than sample freq * (eval frequency + CUTOFF_RATIO%)
    pub(super) fn is_obsolete(&self) -> bool {
        self.linear_model
            .last_model_time()
            .map(|at| {
                at.gt(&Utc::now().sub(
                    self.sampler
                        .freq()
                        .mul((self.beta_eval_freq as f64 * (1.0 + LM_AGE_CUTOFF_RATIO)) as i32),
                ))
            })
            .unwrap_or(false)
    }

    pub(super) fn has_model(&self) -> bool { self.linear_model.has_model() && self.linear_model.is_loaded() }

    pub(super) fn value(&self) -> Option<LinearModelValue> { self.linear_model.value() }

    pub(super) fn reset(&mut self) -> Result<()> { self.linear_model.wipe() }

    pub(super) fn push(&mut self, input: &DualBookPosition) { self.linear_model.push(input); }
}

impl Next<&DualBookPosition> for LinearSpreadModel {
    type Output = Result<Option<LinearModelValue>>;

    fn next(&mut self, input: &DualBookPosition) -> Self::Output {
        if !self.sampler.sample(input.time) {
            return Ok(None);
        }
        trace!(
            event_time = %input.time,
            rows = self.linear_model.len(),
            "linear model sample"
        );
        self.push(input);
        if self.linear_model.value().is_none() && self.linear_model.is_filled() {
            self.update()?;
        }
        Ok(self.linear_model.value())
    }
}
