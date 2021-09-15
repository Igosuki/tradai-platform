use std::sync::Arc;

use chrono::{DateTime, Utc};
use serde::de::DeserializeOwned;
use serde::Serialize;

use db::Storage;
use ext::ResultExt;

use crate::error::Result;
use crate::models::persist::{ModelValue, TimedWindow};
use crate::models::Model;

use super::persist::{PersistentModel, PersistentVec, Window};

type WindowFn<T, M> = fn(&M, Window<'_, T>) -> M;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct WindowedModel<T: Serialize + DeserializeOwned + Clone, M: Serialize + DeserializeOwned + Clone> {
    rows: PersistentVec<T>,
    model: PersistentModel<M>,
    #[derivative(Debug = "ignore")]
    window_fn: WindowFn<T, M>,
}

impl<T: Serialize + DeserializeOwned + Clone, M: Serialize + DeserializeOwned + Clone + Default> WindowedModel<T, M> {
    pub fn new(
        id: &str,
        db: Arc<dyn Storage>,
        window_size: usize,
        max_size_o: Option<usize>,
        window_fn: WindowFn<T, M>,
    ) -> Self {
        let max_size = max_size_o.unwrap_or_else(|| (1.2 * window_size as f64) as usize);
        Self {
            rows: PersistentVec::new(db.clone(), &format!("{}_rows", id), max_size, window_size),
            model: PersistentModel::new(db, id, None),
            window_fn,
        }
    }

    pub fn update_model(&mut self) -> Result<()> {
        if self.is_filled() && !self.has_model() {
            self.model.set_last_model(M::default());
        }
        self.model.update(self.window_fn, self.rows.window()).err_into()
    }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> { self.model.last_model_time() }

    pub fn has_model(&self) -> bool { self.model.has_model() }

    pub fn push(&mut self, row: &T) { self.rows.push(row); }

    pub fn is_filled(&self) -> bool { self.rows.is_filled() }

    pub fn window(&self) -> Window<'_, T> { self.rows.window() }

    pub fn timed_window(&self) -> TimedWindow<'_, T> { self.rows.timed_window() }

    pub fn len(&self) -> usize { self.rows.len() }

    pub fn model(&self) -> Option<ModelValue<M>> { self.model.model() }

    pub fn is_loaded(&self) -> bool { self.model.is_loaded() && self.rows.is_loaded() }

    pub fn wipe(&mut self) -> Result<()> {
        self.model.wipe()?;
        self.rows.wipe()?;
        Ok(())
    }
}

impl<R: Serialize + DeserializeOwned + Clone, M: Serialize + DeserializeOwned + Clone + Default> Model
    for WindowedModel<R, M>
{
    fn ser(&self) -> Option<serde_json::Value> { self.model().and_then(|m| serde_json::to_value(m).ok()) }

    fn try_load(&mut self) -> crate::error::Result<()> {
        self.model.try_loading()?;
        self.rows.try_loading()
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use test::Bencher;

    use chrono::{DateTime, Utc};
    use fake::Fake;
    use quickcheck::{Arbitrary, Gen};

    use crate::models::WindowedModel;
    use crate::models::{Model, Window};
    use crate::test_util::test_db;
    use crate::types::BookPosition;

    #[derive(Debug, Clone, Serialize, Deserialize)]
    pub struct TestRow {
        pub time: DateTime<Utc>,
        pub pos: BookPosition, // crypto_1
    }

    impl Arbitrary for TestRow {
        fn arbitrary(g: &mut Gen) -> TestRow {
            let time: chrono::DateTime<Utc> = fake::faker::chrono::en::DateTime().fake();
            TestRow {
                time,
                pos: BookPosition::arbitrary(g),
            }
        }
    }

    fn sum_window(_lm: &f64, window: Window<'_, TestRow>) -> f64 { window.map(|t| t.pos.mid).sum::<f64>() }

    #[bench]
    fn test_save_load_model(b: &mut Bencher) {
        let id = "default";
        let max_size = 2000;
        let db = test_db();
        let mut table = WindowedModel::new(id, db, 1000, Some(max_size), sum_window);
        let mut gen = Gen::new(500);
        for _ in 0..max_size {
            table.push(&TestRow::arbitrary(&mut gen))
        }
        b.iter(|| {
            table.update_model().unwrap();
            table.try_load().unwrap();
        });
    }
}
