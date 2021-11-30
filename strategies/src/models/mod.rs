use std::ops::{Add, Mul};

use chrono::{DateTime, Duration, Utc};

pub use indicator_model::IndicatorModel;
pub use windowed_model::PersistentWindowedModel;

use crate::error::Result;

pub(crate) mod indicator_model;
pub mod io;
pub(crate) mod persist;
pub(crate) mod windowed_model;

pub trait Model<T> {
    fn ser(&self) -> Option<serde_json::Value>;
    fn try_load(&mut self) -> Result<()>;
    fn is_loaded(&self) -> bool;
    fn wipe(&mut self) -> Result<()>;
    fn last_model_time(&self) -> Option<DateTime<Utc>>;
    fn has_model(&self) -> bool;
    fn value(&self) -> Option<T>;
}

pub type Window<'a, T> = impl Iterator<Item = &'a T> + Clone;
pub type TimedWindow<'a, T> = impl Iterator<Item = &'a TimedValue<T>> + Clone;

#[derive(Debug, Serialize, Deserialize)]
pub struct TimedValue<T>(i64, T);

pub trait WindowedModel<R, M>: Model<M> {
    fn is_filled(&self) -> bool;
    fn window(&self) -> Window<'_, R>;
    fn timed_window(&self) -> TimedWindow<'_, R>;
    fn push(&mut self, row: &R);
    fn len(&self) -> usize;
}

// TODO: Maybe used in a middleware like structure
// with Filter { fn filters -> Option<T> }, filters: Vec<Filter>
// filters.fold().is_some()
// Could be an enum with various types of samplers, distribution based, time based
#[derive(Derivative)]
#[derivative(Debug)]
pub(crate) struct Sampler {
    sample_freq: Duration,
    eval_freq: i32,
    last_time: DateTime<Utc>,
}

impl Sampler {
    pub fn new(sample_freq: Duration, last_time: DateTime<Utc>) -> Self {
        Self {
            sample_freq,
            eval_freq: 1,
            last_time,
        }
    }

    pub fn sample(&mut self, current_time: DateTime<Utc>) -> bool {
        let obsolete_time = self.last_time.add(self.sample_freq.mul(self.eval_freq));
        let should_sample = current_time.ge(&obsolete_time);
        if should_sample {
            self.last_time = current_time;
        }
        should_sample
    }

    pub fn last_sample_time(&self) -> DateTime<Utc> { self.last_time }

    #[allow(dead_code)]
    pub fn set_last_time(&mut self, last_time: DateTime<Utc>) { self.last_time = last_time; }

    pub fn freq(&self) -> Duration { self.sample_freq }
}

/// Time obsolescence is defined by last_time + (sample_freq * eval_freq) > current_time
#[allow(dead_code)]
pub fn is_eval_time_reached(
    current_time: DateTime<Utc>,
    last_time: DateTime<Utc>,
    sample_freq: Duration,
    eval_freq: i32,
) -> bool {
    let obsolete_time = last_time.add(sample_freq.mul(eval_freq));
    current_time.ge(&obsolete_time)
}
