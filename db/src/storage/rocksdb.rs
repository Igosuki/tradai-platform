use crate::error::*;
use crate::storage::Storage;
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};
use serde::de::DeserializeOwned;
use serde::Serialize;

type Bytes = Box<[u8]>;

#[derive(Debug)]
pub struct RocksDbStorage {
    inner: DB,
}

impl RocksDbStorage {
    pub fn new(db_path: &str, tables: Vec<String>) -> Self {
        let mut options = Options::default();
        options.create_if_missing(true);
        options.create_missing_column_families(true);
        let column_families: Vec<ColumnFamilyDescriptor> = tables
            .iter()
            .map(|table| {
                let mut cf_opts = Options::default();
                cf_opts.set_max_write_buffer_number(16);
                ColumnFamilyDescriptor::new(table, cf_opts)
            })
            .collect();
        let db = DB::open_cf_descriptors(&options, db_path, column_families).unwrap();
        Self { inner: db }
    }

    fn cf(&self, name: &str) -> Result<&ColumnFamily> {
        self.inner
            .cf_handle(name.as_ref())
            .ok_or_else(|| Error::NotFound(name.to_string()))
    }
}

impl Storage for RocksDbStorage {
    fn _put(&mut self, table: &str, key: &[u8], value: &[u8]) -> Result<()> {
        let cf = self.cf(table)?;
        self.inner.put_cf(cf, key, value).map_err(|e| e.into())
    }

    fn _get(&self, table: &str, key: &[u8]) -> Result<Vec<u8>> {
        info_time!("RocksDb Get");
        let k = key.as_ref();
        let cf = self.cf(table)?;
        self.inner
            .get_cf(cf, k)
            .map_err(|e| e.into())
            .and_then(|r| r.ok_or_else(|| Error::NotFound(String::from_utf8(k.into()).unwrap())))
    }

    fn _get_ranged(&self, table: &str, from: &[u8]) -> Result<Vec<Bytes>> {
        let mode = IteratorMode::From(from.as_ref(), Direction::Forward);
        let cf = self.cf(table)?;
        Ok(self.inner.iterator_cf(cf, mode).map(|(_k, v)| v).collect())
    }

    fn _get_all(&self, table: &str) -> Result<Vec<(String, Bytes)>> {
        let mode = IteratorMode::Start;
        let cf = self.cf(table)?;
        Ok(self
            .inner
            .iterator_cf(cf, mode)
            .map(|(k, v)| (String::from_utf8(k.into()).unwrap(), v))
            .collect())
    }

    fn _delete(&mut self, table: &str, key: &[u8]) -> Result<()> {
        let cf = self.cf(table)?;
        self.inner.delete_cf(cf, key).map_err(|e| e.into())
    }

    fn _delete_range(&mut self, table: &str, from: &[u8], to: &[u8]) -> Result<()> {
        let cf = self.cf(table)?;
        self.inner.delete_range_cf(cf, from, to).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use crate::storage::rocksdb::RocksDbStorage;
    use crate::storage::Storage;
    use chrono::Utc;
    use test::Bencher;

    fn init() { let _ = env_logger::builder().is_test(true).try_init(); }

    #[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
    struct Foobar {
        foo: String,
        number: i32,
    }

    fn test_dir() -> String {
        let basedir = std::env::var("BITCOINS_TEST_RAMFS_DIR").unwrap_or_else(|_| "/media/ramdisk".to_string());
        let root = tempdir::TempDir::new(&format!("{}/test_data", basedir)).unwrap();
        let buf = root.into_path();
        let path = buf.to_str().unwrap();
        path.to_string()
    }

    fn db(tables: Vec<String>) -> RocksDbStorage { RocksDbStorage::new(&test_dir(), tables) }

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
                let r = db.put(table, &format!("foo{}", i), &insert.as_slice());
                assert!(r.is_ok(), "failed to write all foos {:?}", r);
            }
        });
    }

    #[test]
    fn db_serde_put_get_delete() {
        let table = "foos";
        let db = db(vec![table.to_string()]);
        let rw_cmp = |k, v| {
            let result = bincode::serialize(&v).unwrap();
            let r = db.put(table, k, &result.as_slice());
            assert!(r.is_ok(), "failed to write {} {:?} {:?}", k, v, r);
            let r: String = db.get(table, k).unwrap();
            let deserialized: Foobar = bincode::deserialize(r.as_bytes()).unwrap();
            assert_eq!(deserialized, v);
        };
        rw_cmp("foo", Foobar {
            foo: "bar".to_string(),
            number: 10,
        });
        rw_cmp("foo", Foobar {
            foo: "baz".to_string(),
            number: 11,
        });
        db.delete(table, "foo").unwrap();
        let foo = db.get(table, "foo");
        assert_eq!(Err(crate::error::Error::NotFoundError("foo".to_string())), foo);
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
            let r = db.put(table, key, &result.as_slice());
            assert!(r.is_ok(), "failed to write {} {:?} {:?}", key, v, r);
        }

        let from = format!("{}", before);
        let vec1: Vec<Foobar> = db
            .get_ranged(table, from)
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
            let r = db.put(table, key, &result.as_slice());
            assert!(r.is_ok(), "failed to write {} {:?} {:?}", key, v, r);
            if i == size / 2 {
                then = Utc::now();
            }
        }
        {
            let from = format!("{}", before);
            let to = format!("{}", then);
            info_time!("Deleted items in range {}, {}", &from, &to);
            db.delete_range(table, from, to).unwrap();
            let remaining: Vec<Foobar> = db
                .get_all(table)
                .unwrap()
                .iter()
                .map(|(k, v)| bincode::deserialize(v).unwrap())
                .collect();
            let remaining_items: Vec<Foobar> = items.drain(0..((size / 2) + 1) as usize).collect();
            assert_eq!(remaining, items);
        }
    }
}
