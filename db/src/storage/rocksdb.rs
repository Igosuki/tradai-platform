use crate::error::*;
use crate::storage::Storage;
use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};
use std::path::Path;
use std::sync::Arc;

type Bytes = Box<[u8]>;

#[derive(Debug)]
pub struct RocksDbStorage {
    inner: DB,
}

impl RocksDbStorage {
    pub fn new<S: AsRef<Path>>(db_path: S, tables: Vec<String>) -> Self {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let mut tables = tables;
        let query = DB::list_cf(&options, db_path.as_ref());
        if let Ok(cfs) = query {
            tables.extend_from_slice(cfs.as_slice());
        }
        let column_families: Vec<ColumnFamilyDescriptor> = tables
            .iter()
            .map(|table| {
                let cf_opts = RocksDbStorage::default_cf_options();
                ColumnFamilyDescriptor::new(table, cf_opts)
            })
            .collect();
        let db = DB::open_cf_descriptors(&options, db_path, column_families).unwrap();
        Self { inner: db }
    }

    fn default_cf_options() -> Options {
        let mut cf_opts = Options::default();
        cf_opts.set_max_write_buffer_number(16);
        cf_opts
    }

    fn cf(&self, name: &str) -> Result<Arc<BoundColumnFamily>> {
        self.inner
            .cf_handle(name.as_ref())
            .ok_or_else(|| Error::NotFound(name.to_string()))
    }
}

impl Storage for RocksDbStorage {
    fn _put(&self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = self.cf(table)?;
        self.inner.put_cf(&cf, key, value).map_err(|e| e.into())
    }

    fn _get(&self, table: &str, key: &[u8]) -> Result<Vec<u8>> {
        let cf = self.cf(table)?;
        self.inner
            .get_cf(&cf, key)
            .map_err(|e| e.into())
            .and_then(|r| r.ok_or_else(|| Error::NotFound(String::from_utf8(key.into()).unwrap())))
    }

    fn _get_ranged(&self, table: &str, from: &[u8]) -> Result<Vec<Bytes>> {
        let mode = IteratorMode::From(from, Direction::Forward);
        let cf = self.cf(table)?;
        Ok(self.inner.iterator_cf(&cf, mode).map(|(_k, v)| v).collect())
    }

    fn _get_all(&self, table: &str) -> Result<Vec<(String, Bytes)>> {
        let mode = IteratorMode::Start;
        let cf = self.cf(table)?;
        Ok(self
            .inner
            .iterator_cf(&cf, mode)
            .map(|(k, v)| (String::from_utf8(k.into()).unwrap(), v))
            .collect())
    }

    fn _delete(&self, table: &str, key: &[u8]) -> Result<()> {
        let cf = self.cf(table)?;
        self.inner.delete_cf(&cf, key).map_err(|e| e.into())
    }

    fn _delete_range(&self, table: &str, from: &[u8], to: &[u8]) -> Result<()> {
        let cf = self.cf(table)?;
        self.inner.delete_range_cf(&cf, from, to).map_err(|e| e.into())
    }

    fn ensure_table(&self, name: &str) -> Result<()> {
        if self.inner.cf_handle(name).is_none() {
            self.inner
                .create_cf(name, &RocksDbStorage::default_cf_options())
                .map_err(|e| e.into())
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod test {
    extern crate test;
    extern crate util;

    use crate::error::Error;
    use crate::storage::rocksdb::RocksDbStorage;
    use crate::storage::Storage;
    use crate::StorageExt;
    use chrono::Utc;
    use test::Bencher;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct Foobar {
        foo: String,
        number: i32,
    }

    fn db(tables: Vec<String>) -> RocksDbStorage { RocksDbStorage::new(&util::test::test_dir(), tables) }

    #[bench]
    fn db_serde_put_bench(b: &mut Bencher) {
        let table = "foos";
        let db = db(vec![table.to_string()]);
        //let mut vec: Vec<Foobar> = Vec::new();
        let size = 10_i32;
        let mut inserts = vec![];
        for i in 0..size {
            let v = Foobar {
                foo: "bar".to_string(),
                number: i,
            };
            let result = bincode::serialize(&v).unwrap();
            inserts.push((i, result));
        }
        b.iter(|| {
            for (i, insert) in inserts.clone() {
                let r = db._put(table, format!("foo{}", i).as_bytes(), insert.as_slice());
                assert!(r.is_ok(), "failed to write all foos {:?}", r);
            }
        });
    }

    #[test]
    fn db_put_get_delete() {
        let table = "foos";
        let key = "foo".as_bytes();
        let db = db(vec![table.to_string()]);
        let rw_cmp = |k, v| {
            let result = bincode::serialize(&v).unwrap();
            let r = db._put(table, k, result.as_slice());
            assert!(
                r.is_ok(),
                "failed to write {} {:?} {:?}",
                std::str::from_utf8(k).unwrap(),
                v,
                r
            );
            let r: Vec<u8> = db._get(table, k).unwrap();
            let deserialized: Foobar = bincode::deserialize(r.as_slice()).unwrap();
            assert_eq!(deserialized, v);
        };
        rw_cmp(key, Foobar {
            foo: "bar".to_string(),
            number: 10,
        });
        rw_cmp(key, Foobar {
            foo: "baz".to_string(),
            number: 11,
        });
        db._delete(table, key).unwrap();
        let get_result = db._get(table, key);
        matches!(get_result, Err(Error::NotFound(x)) if x == "foo");
        //assert_eq!(Err(Error::NotFound(String::from_utf8_lossy(key).to_string())), foo);
    }

    #[test]
    fn db_serde_put_get_delete() {
        let table = "foos";
        let key = "foo".as_bytes();
        let db = db(vec![table.to_string()]);
        let rw_cmp = |k, v: Foobar| {
            let r = db.put(table, k, v.clone());
            assert!(
                r.is_ok(),
                "failed to write {} {:?} {:?}",
                std::str::from_utf8(k).unwrap(),
                &v,
                r
            );
            let r: Foobar = db.get(table, k).unwrap();
            assert_eq!(r, v);
        };
        rw_cmp(key, Foobar {
            foo: "bar".to_string(),
            number: 10,
        });
        rw_cmp(key, Foobar {
            foo: "baz".to_string(),
            number: 11,
        });
        db.delete(table, key).unwrap();
        let get_result: crate::error::Result<Foobar> = db.get(table, key);
        matches!(get_result, Err(Error::NotFound(x)) if x == "foo");
        //assert_eq!(Err(Error::NotFound(String::from_utf8_lossy(key).to_string())), foo);
    }

    #[test]
    fn serde_get_ranged_cf() {
        let table = "rows";
        let db = db(vec![table.to_string()]);
        let size = 10_i32.pow(3) as i32;
        let before = Utc::now();
        let mut items = vec![];
        for i in 0..size {
            let v = Foobar {
                foo: "bar".to_string(),
                number: i,
            };
            items.push(v.clone());
            let key = &format!("{}", Utc::now());
            let r = db.put(table, key.as_bytes(), v.clone());
            assert!(r.is_ok(), "failed to write {} {:?} {:?}", key, v, r);
        }

        let vec1: Vec<Foobar> = db.get_ranged(table, before.to_string().as_bytes()).unwrap();
        assert_eq!(vec1, items);
    }

    #[test]
    fn get_ranged_cf() {
        let table = "rows";
        let db = db(vec![table.to_string()]);
        let size = 10_i32.pow(3) as i32;
        let before = Utc::now();
        let mut items = vec![];
        for i in 0..size {
            let v = Foobar {
                foo: "bar".to_string(),
                number: i,
            };
            items.push(v.clone());
            let result = bincode::serialize(&v).unwrap();
            let key = &format!("{}", Utc::now());
            let r = db._put(table, key.as_bytes(), result.as_slice());
            assert!(r.is_ok(), "failed to write {} {:?} {:?}", key, v, r);
        }

        let vec1: Vec<Foobar> = db
            ._get_ranged(table, before.to_string().as_bytes())
            .unwrap()
            .iter()
            .map(|i| bincode::deserialize(i).unwrap())
            .collect();
        assert_eq!(vec1, items);
    }

    #[test]
    fn delete_ranged_cf() {
        init();
        let table = "rows";
        let db = db(vec![table.to_string()]);
        let size = 10_i32.pow(4) as i32;
        let before = Utc::now();
        let mut then = Utc::now();
        let mut items = vec![];

        for i in 0..size {
            let v = Foobar {
                foo: "bar".to_string(),
                number: i,
            };
            items.push(v.clone());
            let result = bincode::serialize(&v).unwrap();
            let key = &format!("{}", Utc::now());
            let r = db._put(table, key.as_bytes(), result.as_slice());
            assert!(r.is_ok(), "failed to write {} {:?} {:?}", key, v, r);
            if i == size / 2 {
                then = Utc::now();
            }
        }
        {
            info_time!("Deleted items in range {}, {}", &before, &then);
            db._delete_range(table, before.to_string().as_bytes(), then.to_string().as_bytes())
                .unwrap();
            let remaining: Vec<Foobar> = db
                ._get_all(table)
                .unwrap()
                .iter()
                .map(|(_k, v)| bincode::deserialize(v).unwrap())
                .collect();
            let _: Vec<Foobar> = items.drain(0..((size / 2) + 1) as usize).collect();
            assert_eq!(remaining, items);
        }
    }
}
