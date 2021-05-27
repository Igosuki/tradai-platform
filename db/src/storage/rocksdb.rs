use crate::error::*;
use crate::storage::Storage;
use rocksdb::{ColumnFamilyDescriptor, Direction, IteratorMode, Options, DB};

struct RocksDbStorage {
    inner: DB,
}

impl RocksDbStorage {
    fn new(db_path: &str, tables: Vec<String>) -> Self {
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
}

impl Storage for RocksDbStorage {
    fn put<K, V>(&self, key: K, value: V) -> Result<()>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.inner.put(key, value).map_err(|e| e.into())
    }

    fn get<K>(&self, key: K) -> Result<String>
    where
        K: AsRef<[u8]>,
    {
        let k = key.as_ref();
        self.inner.get(k).map_err(|e| e.into()).and_then(|r| {
            r.map(|v| String::from_utf8(v).unwrap())
                .ok_or_else(|| Error::NotFoundError(String::from_utf8(k.into()).unwrap()))
        })
    }

    fn get_ranged<F>(&self, table: &str, from: F) -> Result<Vec<String>>
    where
        F: AsRef<[u8]>,
    {
        let mode = IteratorMode::From(from.as_ref(), Direction::Forward);
        let cf = self
            .inner
            .cf_handle(table.as_ref())
            .ok_or_else(|| Error::NotFoundError(table.to_string()))?;
        Ok(self
            .inner
            .iterator_cf(cf, mode)
            .map(|(k, v)| String::from_utf8(v.to_vec()).unwrap())
            .collect())
    }

    fn get_all(&self, table: &str) -> Result<Vec<(String, String)>> {
        let mode = IteratorMode::Start;
        let cf = self
            .inner
            .cf_handle(table.as_ref())
            .ok_or_else(|| Error::NotFoundError(table.to_string()))?;
        Ok(self
            .inner
            .iterator_cf(cf, mode)
            .map(|(k, v)| {
                (
                    String::from_utf8(k.to_vec()).unwrap(),
                    String::from_utf8(v.to_vec()).unwrap(),
                )
            })
            .collect())
    }

    fn delete<K>(&self, table: &str, key: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let cf = self
            .inner
            .cf_handle(table.as_ref())
            .ok_or_else(|| Error::NotFoundError(table.to_string()))?;
        self.inner.delete_cf(cf, key).map_err(|e| e.into())
    }

    fn delete_range<K>(&self, table: &str, from: K, to: K) -> Result<()>
    where
        K: AsRef<[u8]>,
    {
        let cf = self
            .inner
            .cf_handle(table.as_ref())
            .ok_or_else(|| Error::NotFoundError(table.to_string()))?;
        self.inner.delete_range_cf(cf, from, to).map_err(|e| e.into())
    }
}

#[cfg(test)]
mod test {
    use crate::storage::rocksdb::RocksDbStorage;
    use crate::storage::Storage;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
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

    fn db(tables: Vec<String>) -> RocksDbStorage {
        let _dir = tempdir::TempDir::new("s").unwrap();
        RocksDbStorage::new(&test_dir(), tables)
    }

    #[test]
    fn db_serde_put_1m() {
        let db = db(vec![]);
        //let mut vec: Vec<Foobar> = Vec::new();
        let size = 10_i32.pow(6) as i32;
        {
            info_time!("Insert {} records", size);
            for i in 0..size {
                let v = Foobar {
                    foo: "bar".to_string(),
                    number: i,
                };
                let result = bincode::serialize(&v).unwrap();
                let r = db.put("foos", &result.as_slice());
                assert!(r.is_ok(), "failed to write all foos {:?}", r);
            }
        }
    }

    #[test]
    fn db_serde_put_get() {
        let db = db(vec![]);
        let rw_cmp = |k, v| {
            let result = bincode::serialize(&v).unwrap();
            let r = db.put(k, &result.as_slice());
            assert!(r.is_ok(), "failed to write {} {:?} {:?}", k, v, r);
            let r: String = db.get(k).unwrap();
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
    }

    #[test]
    fn get_ranged_cf() {
        let db = db(vec![]);
        let rw_cmp = |k, v| {
            let result = bincode::serialize(&v).unwrap();
            let r = db.put(k, &result.as_slice());
            assert!(r.is_ok(), "failed to write {} {:?} {:?}", k, v, r);
            let r: String = db.get(k).unwrap();
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
    }
}
