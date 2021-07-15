use super::persist::{PersistentModel, PersistentVec, Window};
use crate::models::persist::ModelValue;
use chrono::{DateTime, Utc};
use db::Storage;
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::sync::Arc;

type WindowFn<T, M> = fn(&M, Window<T>) -> M;

#[derive(Derivative)]
#[derivative(Debug)]
pub struct WindowedModel<T: Serialize + DeserializeOwned + Clone, M: Serialize + DeserializeOwned + Clone> {
    rows: PersistentVec<T>,
    model: PersistentModel<M>,
    #[derivative(Debug = "ignore")]
    window_fn: WindowFn<T, M>,
}

impl<T: Serialize + DeserializeOwned + Clone, M: Serialize + DeserializeOwned + Clone> WindowedModel<T, M> {
    pub fn new(
        id: &str,
        db: Arc<Box<dyn Storage>>,
        window_size: usize,
        max_size_o: Option<usize>,
        window_fn: WindowFn<T, M>,
    ) -> Self {
        let max_size = max_size_o.unwrap_or_else(|| 2 * window_size);
        Self {
            rows: PersistentVec::new(db.clone(), &format!("{}_rows", id), max_size, window_size),
            model: PersistentModel::new(db, id, None),
            window_fn,
        }
    }

    pub fn update_model(&mut self) -> Result<(), db::Error> {
        self.model.update_model(self.window_fn, self.rows.window())
    }

    pub fn last_model_time(&self) -> Option<DateTime<Utc>> { self.model.last_model_time() }

    pub fn has_model(&self) -> bool { self.model.has_model() }

    pub fn push(&mut self, row: &T) { self.rows.push(row); }

    pub fn is_filled(&self) -> bool { self.rows.is_filled() }

    pub fn window(&self) -> Window<T> { self.rows.window() }

    pub fn len(&self) -> usize { self.rows.len() }

    pub fn try_loading_model(&mut self) -> crate::error::Result<()> {
        self.model.try_loading()?;
        self.rows.try_loading()
    }

    pub fn model(&self) -> Option<ModelValue<M>> { self.model.model() }

    pub fn is_loaded(&self) -> bool { self.model.is_loaded() && self.rows.is_loaded() }
}

#[cfg(test)]
mod test {
    extern crate test;

    use tempfile::TempDir;

    use crate::models::Window;
    use crate::models::WindowedModel;
    use crate::types::BookPosition;
    use chrono::{DateTime, Utc};
    use db::get_or_create;
    use fake::Fake;
    use quickcheck::{Arbitrary, Gen};
    use std::sync::Arc;
    use test::Bencher;

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

    fn test_dir() -> String {
        let tempdir = TempDir::new().unwrap();
        tempdir.into_path().to_str().unwrap().to_string()
    }

    fn sum_window(_lm: &f64, window: Window<TestRow>) -> f64 { window.map(|t| t.pos.mid).sum::<f64>() }

    #[bench]
    fn test_save_load_model(b: &mut Bencher) {
        let id = "default";
        let max_size = 2000;
        let test_dir = test_dir();
        let db = Arc::new(get_or_create(test_dir, vec![]));
        let mut table = WindowedModel::new(id, db, 1000, Some(max_size), sum_window);
        let mut gen = Gen::new(500);
        for _ in 0..max_size {
            table.push(&TestRow::arbitrary(&mut gen))
        }
        b.iter(|| {
            table.update_model().unwrap();
            table.try_loading_model().unwrap();
        });
    }
}
