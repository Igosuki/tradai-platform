use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;

use db::Storage;
use ext::ResultExt;
use stats::Next;

use crate::error::Result;
use crate::models::persist::UpdateFn;
use crate::models::Model;

use super::persist::{ModelValue, PersistentValue};

#[derive(Derivative)]
#[derivative(Debug)]
pub struct IndicatorModel<T, R> {
    model: PersistentValue<T>,
    #[derivative(Debug = "ignore")]
    update_fn: UpdateFn<T, R>,
}

impl<T: Serialize + DeserializeOwned + Copy + Next<R>, R> IndicatorModel<T, R> {
    pub fn new(id: &str, db: Arc<dyn Storage>, initial_value: T) -> Self {
        Self {
            model: PersistentValue::new(db, id, Some(ModelValue::new(initial_value))),
            update_fn: |m, args| {
                m.next(args);
                m
            },
        }
    }

    pub fn update(&mut self, next_value: R) -> Result<()> { self.model.update(self.update_fn, next_value).err_into() }

    pub fn import(&mut self, v: serde_json::Value) -> Result<()> {
        let model: T = serde_json::from_value(v)?;
        self.model.wipe()?;
        self.model.set_last_model(model);
        Ok(())
    }
}

impl<T: Serialize + DeserializeOwned + Copy + Next<R>, R> Model<T> for IndicatorModel<T, R> {
    fn json(&self) -> Option<serde_json::Value> { self.value().and_then(|m| serde_json::to_value(m).ok()) }

    fn try_load(&mut self) -> crate::error::Result<()> { self.model.try_loading() }

    fn is_loaded(&self) -> bool { self.model.is_loaded() }

    fn wipe(&mut self) -> Result<()> { self.model.wipe().err_into() }

    fn last_value_time(&self) -> Option<DateTime<Utc>> { self.model.last_value_time() }

    fn has_value(&self) -> bool { self.model.has_model() }

    fn value(&self) -> Option<T> { self.model.value() }
}

#[cfg(test)]
mod test {}
