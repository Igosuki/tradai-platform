use crate::models::Window;
use crate::types::BookPosition;
use chrono::{DateTime, Utc};
use itertools::Itertools;
use math::iter::{CovarianceExt, MeanExt, VarianceExt};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataRow {
    pub time: DateTime<Utc>,
    pub left: BookPosition,  // crypto_1
    pub right: BookPosition, // crypto_2
}

pub fn beta(i: Window<DataRow>) -> f64 {
    let (var, covar) = i.tee();
    let variance: f64 = var.map(|r| r.left.mid).variance();
    trace!("variance {}", variance);
    let covariance: f64 = covar.map(|r| (r.left.mid, r.right.mid)).covariance::<(f64, f64), f64>();
    trace!("covariance {}", covariance);
    let beta_val = covariance / variance;
    trace!("beta_val {}", beta_val);
    beta_val
}

pub fn alpha(i: Window<DataRow>, beta_val: f64) -> f64 {
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

pub fn linear_model(_m: &LinearModelValue, i: Window<DataRow>) -> LinearModelValue {
    let beta = beta(i.clone());
    let alpha = alpha(i.clone(), beta);
    LinearModelValue { beta, alpha }
}

pub fn predict(alpha: f64, beta: f64, value: f64) -> f64 { alpha + beta * value }
