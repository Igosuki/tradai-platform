use db::{get_or_create, DbOptions, Storage};
use std::sync::Arc;

pub fn test_db() -> Arc<dyn Storage> {
    let path = util::test::test_dir();
    get_or_create(&DbOptions::new(path), "", vec![])
}
