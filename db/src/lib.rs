#[macro_use]
extern crate log;
#[cfg(test)]
#[macro_use]
extern crate serde_derive;

use rkv::error::StoreError::LmdbError;
use rkv::{Manager, Rkv, SingleStore, StoreOptions, Value, Writer};
use serde::de::DeserializeOwned;
use serde::Serialize;
use std::fs;
use std::path::Path;
use std::sync::RwLockReadGuard;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DataStoreError {
    #[error("rkv error")]
    StoreError(rkv::error::StoreError),
    #[error("expected a json value")]
    ExpectedJson,
    #[error("json error")]
    JsonError(#[from] serde_json::error::Error),
}

#[derive(Debug, Clone)]
pub struct Db {
    name: String,
    root: Box<Path>,
}

impl Db {
    pub fn new(root: &str, name: String) -> Self {
        fs::create_dir_all(root).unwrap();
        let path = Path::new(root);
        Self {
            root: Box::from(path),
            name,
        }
    }

    pub fn with_db<F, B>(&self, process: F) -> B
    where
        F: Fn(RwLockReadGuard<Rkv>, SingleStore) -> B,
    {
        // First determine the path to the environment, which is represented
        // on disk as a directory containing two files:
        //
        //   * a data file containing the key/value stores
        //   * a lock file containing metadata about current transactions
        //
        // In this example, we use the `tempfile` crate to create the directory.
        //
        // let root = Builder::new().prefix("simple-db").tempdir().unwrap();

        // The Manager enforces that each process opens the same environment
        // at most once by caching a handle to each environment that it opens.
        // Use it to retrieve the handle to an opened environment—or create one
        // if it hasn't already been opened:
        let created_arc = Manager::singleton()
            .write()
            .unwrap()
            .get_or_create(self.root.as_ref(), Rkv::new)
            .unwrap();
        let env = created_arc.read().unwrap();

        // Then you can use the environment handle to get a handle to a datastore:
        let x: &str = &self.name;
        let store = env.open_single(x, StoreOptions::create()).unwrap();
        process(env, store)
    }

    pub fn read_json_vec<T: DeserializeOwned>(&self, key: &str) -> Vec<T> {
        self.with_db(|env, store| {
            let reader = env.read().expect("reader");
            let iter = store.iter_from(&reader, key).unwrap();
            let x: Vec<T> = iter
                .flat_map(|r| {
                    r.map_err(|e| DataStoreError::StoreError(e))
                        .and_then(|v| match v.1 {
                            Some(rkv::value::Value::Json(json_str)) => {
                                serde_json::from_str::<T>(json_str)
                                    .map_err(|e| DataStoreError::JsonError(e))
                            }
                            _ => Err(DataStoreError::ExpectedJson),
                        })
                })
                .collect();
            x
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

    pub fn delete_all(&self, key: &str) {
        self.with_db(|env, store| {
            let mut writer = env.write().unwrap();
            match store.delete(&mut writer, key) {
                Ok(()) => {}
                Err(e) => match e {
                    LmdbError(lmdb::Error::NotFound) => {}
                    e => error!("Failed to delete key {} for {}", key, e),
                },
            };
            Self::commit(writer, key);
        })
    }

    pub fn put_json<T: Serialize>(&self, key: &str, v: T) {
        self.with_db(|env, store| {
            let result = serde_json::to_string(&v).unwrap();
            let mut writer = env.write().unwrap();

            store.put(&mut writer, &key, &Value::Json(&result)).unwrap();
            Self::commit(writer, key);
        });
    }

    pub fn put_all_json<T: Serialize>(&self, key: &str, v: &Vec<T>) {
        self.with_db(|env, store| {
            let mut writer = env.write().unwrap();
            for (i, v) in v.iter().enumerate() {
                let result = serde_json::to_string(&v).unwrap();
                store
                    .put(&mut writer, &format!("{}{}", key, i), &Value::Json(&result))
                    .unwrap();
            }
            Self::commit(writer, key);
        });
    }

    pub fn put<'a, T: 'a>(&self, key: &str, v: &'a T)
    where
        Value<'a>: From<&'a T>,
    {
        self.with_db(|env, store| {
            let mut writer = env.write().unwrap();
            let _value: Value = v.into();
            store.put(&mut writer, &key, &v.into()).unwrap();
            Self::commit(writer, key);
        });
    }

    fn commit(w: Writer, key: &str) {
        match w.commit() {
            Ok(_) => {}
            Err(e) => error!("Failed to commit key {} for {}", key, e),
        }
    }
}

#[cfg(test)]
mod test {
    use crate::Db;
    use rkv::{Rkv, SingleStore, Value};
    use std::sync::RwLockReadGuard;

    fn make_db_test(env: RwLockReadGuard<Rkv>, store: SingleStore) {
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
            store
                .put(&mut writer, "uint", &Value::U64(1234_u64))
                .unwrap();
            store
                .put(&mut writer, "float", &Value::F64(1234.0.into()))
                .unwrap();
            store
                .put(&mut writer, "instant", &Value::Instant(1528318073700))
                .unwrap();
            store
                .put(&mut writer, "boolean", &Value::Bool(true))
                .unwrap();
            store
                .put(&mut writer, "string", &Value::Str("Héllo, wörld!"))
                .unwrap();
            store
                .put(
                    &mut writer,
                    "json",
                    &Value::Json(r#"{"foo":"bar", "number": 1}"#),
                )
                .unwrap();
            store
                .put(&mut writer, "blob", &Value::Blob(b"blob"))
                .unwrap();

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
            println!(
                "It should be None! ({:?})",
                store.get(&reader, "foo").unwrap()
            );
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
            println!(
                "It should be None! ({:?})",
                store.get(&reader, "foo").unwrap()
            );
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
            println!(
                "It should be None! ({:?})",
                store.get(&writer, "foo").unwrap()
            );
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
                println!(
                    "It should be None! ({:?})",
                    store.get(&reader, "foo").unwrap()
                );
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
                println!(
                    "It should be None! ({:?})",
                    store.get(&reader, "foo").unwrap()
                );
                println!(
                    "It should be None! ({:?})",
                    store.get(&reader, "bar").unwrap()
                );
            }
        }
    }

    fn db() -> Db {
        let dir = tempdir::TempDir::new("s").unwrap();
        Db::new(dir.into_path().to_str().unwrap(), "mydb".to_string())
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Foobar {
        foo: String,
        number: i32,
    }

    fn insert_json(env: RwLockReadGuard<Rkv>, store: SingleStore) {
        let mut writer = env.write().unwrap();
        store
            .put(
                &mut writer,
                "jsona",
                &Value::Json(r#"{"foo":"bar", "number": 1}"#),
            )
            .unwrap();
        store
            .put(
                &mut writer,
                "jsonb",
                &Value::Json(r#"{"foo":"bar", "number": 2}"#),
            )
            .unwrap();
        writer.commit().unwrap();
    }

    #[test]
    fn db_json_fetch() {
        let db = db();
        db.with_db(insert_json);
        let vec: Vec<Foobar> = db.read_json_vec("json");
        assert_eq!(
            vec,
            [
                Foobar {
                    foo: "bar".to_string(),
                    number: 1,
                },
                Foobar {
                    foo: "bar".to_string(),
                    number: 2,
                }
            ]
        )
    }

    #[test]
    fn db_test_basics() {
        let db = db();
        db.with_db(make_db_test);
    }
}
