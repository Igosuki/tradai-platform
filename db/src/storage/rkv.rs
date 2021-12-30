use std::fs;
use std::path::Path;
use std::sync::{Arc, RwLock, RwLockReadGuard};

use crate::rkv::DataStoreError::LockError;
use rkv::backend::{BackendDatabase, BackendEnvironmentBuilder, BackendRwTransaction, Lmdb, LmdbDatabase,
                   LmdbEnvironment, LmdbRwTransaction};
use rkv::{Manager, OwnedValue, Rkv, SingleStore, StoreError, StoreOptions, Value, Writer};
use serde::de::DeserializeOwned;
use serde::Serialize;
use thiserror::Error;

type RkvLmdb = Rkv<LmdbEnvironment>;

#[derive(Error, Debug)]
pub enum DataStoreError {
    #[error("rkv error")]
    StoreError(#[from] rkv::StoreError),
    #[error("expected a json value")]
    ExpectedJson,
    #[error("json error")]
    JsonError(#[from] serde_json::error::Error),
    #[error("blob error")]
    BlobError(#[from] bincode::Error),
    #[error("io error")]
    IOError(#[from] std::io::Error),
    #[error("lock poison error")]
    LockError,
}

#[derive(Debug, Clone)]
pub struct Db {
    pub name: String,
    pub root: Box<Path>,
    rwl: Arc<RwLock<RkvLmdb>>,
}

pub type DbResult<T> = Result<T, DataStoreError>;

pub type WriteResult = DbResult<()>;

impl Db {
    pub fn try_new(root: &str, name: String) -> DbResult<Self> {
        fs::create_dir_all(root)?;
        let path = Path::new(root);
        let root: Box<Path> = Box::from(path);
        // First determine the path to the environment, which is represented
        // on disk as a directory containing two files:
        //
        //   * a data file containing the key/value stores
        //   * a lock file containing metadata about current transactions
        //
        // The Manager enforces that each process opens the same environment
        // at most once by caching a handle to each environment that it opens.
        // Use it to retrieve the handle to an opened environment—or create one
        // if it hasn't already been opened:
        let created_arc = Manager::<LmdbEnvironment>::singleton()
            .write()
            .map_err(|_| LockError)?
            .get_or_create(root.as_ref(), Self::new_rkv)?;
        Ok(Self {
            root,
            name,
            rwl: created_arc,
        })
    }

    fn new_rkv(path: &Path) -> Result<RkvLmdb, StoreError> {
        if !path.is_dir() {
            return Err(StoreError::UnsuitableEnvironmentPath(path.into()));
        }

        let mut builder = Rkv::environment_builder::<Lmdb>();
        builder.set_max_dbs(5);
        builder.set_map_size(10_i32.pow(8) as usize);
        Rkv::from_builder(path, builder)
    }

    pub fn with_db<F, B>(&self, process: F) -> B
    where
        F: Fn(RwLockReadGuard<'_, RkvLmdb>, SingleStore<LmdbDatabase>) -> B,
    {
        let env = self.rwl.read().unwrap();
        // Then you can use the environment handle to get a handle to a datastore:
        let x: &str = &self.name;
        let store = env.open_single(x, StoreOptions::create()).unwrap();
        process(env, store)
    }

    fn write<F, E>(&self, process: F) -> WriteResult
    where
        F: Fn(&mut Writer<LmdbRwTransaction<'_>>, SingleStore<LmdbDatabase>) -> Result<(), E>,
        E: Into<DataStoreError>,
    {
        self.with_db(|env, store| {
            let mut w = env.write().unwrap();
            process(&mut w, store).map_err(|e| e.into())?;
            w.commit().map_err(|e| e.into())
        })
    }

    pub fn all(&self) -> Vec<String> {
        self.with_db(|env, store| {
            let reader = env.read().unwrap();
            let mut strings: Vec<String> = Vec::new();
            for (kb, ov) in store.iter_start(&reader).unwrap().flatten() {
                let result: Option<&str> = std::str::from_utf8(kb).ok();
                strings.push(format!("{:?}:{:?}", result, ov))
            }
            strings
        })
    }

    pub fn read_json_vec<T: DeserializeOwned>(&self, key: &str) -> Vec<T> {
        self.with_db(|env, store| {
            let reader = env.read().expect("reader");
            let iter = store.iter_from(&reader, key).unwrap();
            let x: Vec<T> = iter
                .flat_map(|r| {
                    r.map_err(DataStoreError::StoreError).and_then(|v| match v.1 {
                        rkv::value::Value::Json(json_str) => {
                            serde_json::from_str::<T>(json_str).map_err(DataStoreError::JsonError)
                        }
                        rkv::value::Value::Blob(str) => {
                            bincode::deserialize::<T>(str).map_err(DataStoreError::BlobError)
                        }
                        _ => Err(DataStoreError::ExpectedJson),
                    })
                })
                .collect();
            x
        })
    }

    pub fn read_all_json<T: DeserializeOwned>(&self) -> Vec<(String, T)> {
        self.with_db(|env, store| {
            let reader = env.read().expect("reader");
            let iter = store.iter_start(&reader).unwrap();
            iter.map(|r| {
                r.map_err(DataStoreError::StoreError).and_then(|v| match v.1 {
                    rkv::value::Value::Json(json_str) => {
                        let t = serde_json::from_str::<T>(json_str).map_err(DataStoreError::JsonError);
                        t.map(|record| (std::str::from_utf8(v.0).unwrap().to_string(), record))
                    }
                    rkv::value::Value::Blob(str) => {
                        let t = bincode::deserialize::<T>(str).map_err(DataStoreError::BlobError);
                        t.map(|record| (std::str::from_utf8(v.0).unwrap().to_string(), record))
                    }
                    _ => Err(DataStoreError::ExpectedJson),
                })
            })
            .filter_map(|r| r.ok())
            .collect()
        })
    }

    pub fn read_json<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.with_db(|env, store| {
            let reader = env.read().expect("reader");
            store.get(&reader, key).unwrap().and_then(|v| match v {
                rkv::value::Value::Json(json_str) => serde_json::from_str::<T>(json_str).ok(),
                _ => None,
            })
        })
    }

    pub fn read_string(&self, key: &str) -> Option<String> {
        self.with_db(|env, store| {
            let reader = env.read().expect("reader");
            store.get(&reader, key).unwrap().and_then(|v| match v {
                rkv::value::Value::Str(s) => Some(s.to_string()),
                _ => None,
            })
        })
    }

    pub fn read_b<T: DeserializeOwned>(&self, key: &str) -> Option<T> {
        self.with_db(|env, store| {
            let reader = env.read().expect("reader");
            store.get(&reader, key).unwrap().and_then(|v| match v {
                rkv::value::Value::Blob(s) => {
                    let v: T = bincode::deserialize(s).unwrap();
                    Some(v)
                }
                _ => None,
            })
        })
    }

    pub fn delete_all(&self, key: &str) -> WriteResult {
        self.write(|writer, store| Self::safe_delete(store, writer, key))
    }

    pub fn put_json<T: Serialize>(&self, key: &str, v: T) -> WriteResult {
        self.write(|writer, store| {
            let result = serde_json::to_string(&v).unwrap();
            store.put(writer, &key, &Value::Json(&result))
        })
    }

    pub fn put_all_json<T: Serialize>(&self, key: &str, v: &[T]) -> WriteResult {
        self.write::<_, rkv::StoreError>(|writer, store| {
            v.iter()
                .enumerate()
                .map(|(i, v)| {
                    let result = serde_json::to_string(&v).unwrap();
                    store.put(writer, &format!("{}{}", key, i), &Value::Json(&result))
                })
                .collect::<Result<Vec<()>, rkv::StoreError>>()?;
            Ok(())
        })
    }

    fn safe_delete<T: BackendDatabase, I: BackendRwTransaction<Database = T>>(
        store: SingleStore<T>,
        writer: &mut Writer<I>,
        key: &str,
    ) -> Result<(), StoreError> {
        match store.delete(writer, key) {
            Ok(()) => Ok(()),
            Err(e) => match e {
                StoreError::LmdbError(lmdb::Error::NotFound) => Ok(()),
                e => {
                    error!("Failed to delete key '{}' for {}", key, e);
                    Err(e)
                }
            },
        }
    }

    pub fn replace_all<T: Serialize>(&self, key: &str, v: &[T]) -> WriteResult {
        self.with_db(|env, store| {
            let reader = env.read().unwrap();
            let mut iter = store.iter_from(&reader, key).unwrap();
            let mut writer = env.write().unwrap();
            while let Some(Ok((k, _v))) = iter.next() {
                store.delete(&mut writer, std::str::from_utf8(k).unwrap()).unwrap();
                //Self::safe_delete(store, writer, str::from_utf8(k).unwrap())?;
            }
            v.iter()
                .enumerate()
                .map(|(i, v)| {
                    let encoded: Vec<u8> = bincode::serialize(&v).unwrap();
                    store.put(&mut writer, &format!("{}{}", key, i), &Value::Blob(&encoded))
                })
                .collect::<Result<Vec<()>, rkv::StoreError>>()
                .unwrap();
            Ok(writer.commit()?)
        })
    }

    pub fn put_b<T: Serialize>(&self, key: &str, v: &T) -> WriteResult {
        let encoded: Vec<u8> = bincode::serialize(&v).unwrap();
        self.put(key, &OwnedValue::Blob(encoded))
    }

    pub fn put<'a, T: 'a>(&self, key: &str, v: &'a T) -> WriteResult
    where
        Value<'a>: From<&'a T>,
    {
        self.write(|writer, store| store.put(writer, &key, &v.into()))
    }
}

#[cfg(test)]
mod test {
    extern crate test;

    use std::sync::RwLockReadGuard;
    use std::time::Instant;
    use test::Bencher;

    use rkv::backend::LmdbDatabase;
    use rkv::{SingleStore, Value};

    use util::test::test_dir;

    use super::{Db, RkvLmdb};

    fn make_db_test(env: RwLockReadGuard<'_, RkvLmdb>, store: SingleStore<LmdbDatabase>) {
        {
            // Use a write transaction to mutate the store via a `Writer`.
            // There can be only one writer for a given environment, so opening
            // a second one will block until the first completes.
            let mut writer = env.write().unwrap();

            // Keys are `AsRef<[u8]>`, while values are `Value` enum instances.
            // Use the `Blob` variant to store arbitrary collections of bytes.
            // Putting data returns a `Result<(), StoreError>`, where StoreError
            // is an enum identifying the reason for a failure.
            store.put(&mut writer, "int", &Value::I64(1234)).unwrap();
            store.put(&mut writer, "uint", &Value::U64(1234_u64)).unwrap();
            store.put(&mut writer, "float", &Value::F64(1234.0.into())).unwrap();
            store
                .put(&mut writer, "instant", &Value::Instant(1528318073700))
                .unwrap();
            store.put(&mut writer, "boolean", &Value::Bool(true)).unwrap();
            store.put(&mut writer, "string", &Value::Str("Héllo, wörld!")).unwrap();
            store
                .put(&mut writer, "json", &Value::Json(r#"{"foo":"bar", "number": 1}"#))
                .unwrap();
            store.put(&mut writer, "blob", &Value::Blob(b"blob")).unwrap();

            // You must commit a write transaction before the writer goes out
            // of scope, or the transaction will abort and the data won't persist.
            writer.commit().unwrap();
        }

        {
            // Use a read transaction to query the store via a `Reader`.
            // There can be multiple concurrent readers for a store, and readers
            // never block on a writer nor other readers.
            let reader = env.read().expect("reader");

            // Keys are `AsRef<u8>`, and the return value is `Result<Option<Value>, StoreError>`.
            println!("Get int {:?}", store.get(&reader, "int").unwrap());
            println!("Get uint {:?}", store.get(&reader, "uint").unwrap());
            println!("Get float {:?}", store.get(&reader, "float").unwrap());
            println!("Get instant {:?}", store.get(&reader, "instant").unwrap());
            println!("Get boolean {:?}", store.get(&reader, "boolean").unwrap());
            println!("Get string {:?}", store.get(&reader, "string").unwrap());
            println!("Get json {:?}", store.get(&reader, "json").unwrap());
            println!("Get blob {:?}", store.get(&reader, "blob").unwrap());

            // Retrieving a non-existent value returns `Ok(None)`.
            println!(
                "Get non-existent value {:?}",
                store.get(&reader, "non-existent").unwrap()
            );

            // A read transaction will automatically close once the reader
            // goes out of scope, so isn't necessary to close it explicitly,
            // although you can do so by calling `Reader.abort()`.
        }

        {
            // Aborting a write transaction rolls back the change(s).
            let mut writer = env.write().unwrap();
            store.put(&mut writer, "foo", &Value::Str("bar")).unwrap();
            writer.abort();
            let reader = env.read().expect("reader");
            println!("It should be None! ({:?})", store.get(&reader, "foo").unwrap());
        }

        {
            // Explicitly aborting a transaction is not required unless an early
            // abort is desired, since both read and write transactions will
            // implicitly be aborted once they go out of scope.
            {
                let mut writer = env.write().unwrap();
                store.put(&mut writer, "foo", &Value::Str("bar")).unwrap();
            }
            let reader = env.read().expect("reader");
            println!("It should be None! ({:?})", store.get(&reader, "foo").unwrap());
        }

        {
            // Deleting a key/value pair also requires a write transaction.
            let mut writer = env.write().unwrap();
            store.put(&mut writer, "foo", &Value::Str("bar")).unwrap();
            store.put(&mut writer, "bar", &Value::Str("baz")).unwrap();
            store.delete(&mut writer, "foo").unwrap();

            // A write transaction also supports reading, and the version of the
            // store that it reads includes the changes it has made regardless of
            // the commit state of that transaction.
            // In the code above, "foo" and "bar" were put into the store,
            // then "foo" was deleted so only "bar" will return a result when the
            // database is queried via the writer.
            println!("It should be None! ({:?})", store.get(&writer, "foo").unwrap());
            println!("Get bar ({:?})", store.get(&writer, "bar").unwrap());

            // But a reader won't see that change until the write transaction
            // is committed.
            {
                let reader = env.read().expect("reader");
                println!("Get foo {:?}", store.get(&reader, "foo").unwrap());
                println!("Get bar {:?}", store.get(&reader, "bar").unwrap());
            }
            writer.commit().unwrap();
            {
                let reader = env.read().expect("reader");
                println!("It should be None! ({:?})", store.get(&reader, "foo").unwrap());
                println!("Get bar {:?}", store.get(&reader, "bar").unwrap());
            }

            // Committing a transaction consumes the writer, preventing you
            // from reusing it by failing at compile time with an error.
            // This line would report error[E0382]: borrow of moved value: `writer`.
            // store.put(&mut writer, "baz", &Value::Str("buz")).unwrap();
        }

        {
            // Clearing all the entries in the store with a write transaction.
            {
                let mut writer = env.write().unwrap();
                store.put(&mut writer, "foo", &Value::Str("bar")).unwrap();
                store.put(&mut writer, "bar", &Value::Str("baz")).unwrap();
                writer.commit().unwrap();
            }

            {
                let mut writer = env.write().unwrap();
                store.clear(&mut writer).unwrap();
                writer.commit().unwrap();
            }

            {
                let reader = env.read().expect("reader");
                println!("It should be None! ({:?})", store.get(&reader, "foo").unwrap());
                println!("It should be None! ({:?})", store.get(&reader, "bar").unwrap());
            }
        }
    }

    fn db() -> Db {
        let dir = test_dir();
        Db::try_new(dir.as_ref().to_str().unwrap(), "mydb".to_string()).unwrap()
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct Foobar {
        foo: String,
        number: i32,
    }

    fn insert_json(env: RwLockReadGuard<'_, RkvLmdb>, store: SingleStore<LmdbDatabase>) {
        let mut writer = env.write().unwrap();
        store
            .put(&mut writer, "jsona", &Value::Json(r#"{"foo":"bar", "number": 1}"#))
            .unwrap();
        store
            .put(&mut writer, "jsonb", &Value::Json(r#"{"foo":"bar", "number": 2}"#))
            .unwrap();
        writer.commit().unwrap();
    }

    #[test]
    fn db_json_fetch() {
        let db = db();
        db.with_db(insert_json);
        let vec: Vec<Foobar> = db.read_json_vec("json");
        assert_eq!(vec, [
            Foobar {
                foo: "bar".to_string(),
                number: 1,
            },
            Foobar {
                foo: "bar".to_string(),
                number: 2,
            }
        ])
    }

    #[test]
    fn db_json_put_all_max() {
        let db = db();
        db.with_db(insert_json);
        let mut vec: Vec<Foobar> = Vec::new();
        for i in 0..10000 {
            vec.push(Foobar {
                foo: "bar".to_string(),
                number: i,
            });
        }
        let r = db.put_all_json("foos", &vec);
        assert!(r.is_ok(), "failed to write all foos {:?}", r);
    }

    #[test]
    fn db_test_basics() {
        let db = db();
        db.with_db(make_db_test);
    }

    #[test]
    fn db_insert_large_array() {
        let db = db();
        let size = 100000;
        let mut vals: Vec<(f64, f64)> = Vec::with_capacity(size);
        let now = Instant::now();
        for _i in 0..10000 {
            let v = (rand::random::<f64>(), rand::random::<f64>());
            vals.push(v);
        }
        db.put_b("row", &vals).unwrap();
        println!("inserted array in {}", now.elapsed().as_millis());
        let mut vals: Vec<(f64, f64)> = Vec::with_capacity(size);
        for _i in 0..size {
            let v = (rand::random::<f64>(), rand::random::<f64>());
            vals.push(v);
        }
        let now = Instant::now();
        db.delete_all("row").unwrap();
        println!("deleted array in {}", now.elapsed().as_millis());
        let now = Instant::now();
        db.put_b("row", &vals).unwrap();
        println!("inserted larger array in {}", now.elapsed().as_millis());
        //db.replace_all("row", vals.as_slice());

        let now = Instant::now();
        let read_values: Vec<(f64, f64)> = db.read_b("row").unwrap();
        println!("read larger array in {}", now.elapsed().as_millis());
        assert_eq!(read_values.len(), size)
    }

    // #[test]
    // fn db_replace_all() {
    //     let db = db();
    //     let size = 100000;
    //     let mut vals: Vec<(f64, f64)> = Vec::with_capacity(size);
    //     let now = Instant::now();
    //     for i in 0..10000 {
    //         let v = (rand::random::<f64>(), rand::random::<f64>());
    //         vals.push(v);
    //         db.put_b(&format!("row{}", i), &v);
    //     }
    //     println!("inserted rows in {}", now.elapsed().as_millis());
    //     let mut vals: Vec<(f64, f64)> = Vec::with_capacity(size);
    //     for i in 0..size {
    //         let v = (rand::random::<f64>(), rand::random::<f64>());
    //         vals.push(v);
    //     }
    //
    //     db.replace_all("row", vals.as_slice());
    //
    //     let read_values : Vec<(f64, f64)> = db.read_json_vec("row");
    //     assert_eq!(read_values.len(), size)
    // }

    #[bench]
    fn db_replace_all_10k(b: &mut Bencher) {
        let db = db();
        let size = 10000;
        let mut vals: Vec<(f64, f64)> = Vec::with_capacity(size);
        for _i in 0..100 {
            let v = (rand::random::<f64>(), rand::random::<f64>());
            vals.push(v);
            db.put_b("row", &v).unwrap();
        }
        for _i in 0..size {
            let v = (rand::random::<f64>(), rand::random::<f64>());
            vals.push(v);
        }

        b.iter(|| {
            db.replace_all("row", vals.as_slice()).unwrap();
        });
    }
}
