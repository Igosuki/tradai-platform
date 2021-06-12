use chrono::Utc;
use tempdir::TempDir;

pub fn test_dir() -> TempDir {
    let basedir = std::env::var("BITCOINS_TEST_RAMFS_DIR").unwrap_or_else(|_| "/media/ramdisk".to_string());
    tempdir::TempDir::new(&format!("{}/test_data", basedir)).unwrap()
}

pub const TIMESTAMP_FORMAT: &str = "%Y%m%d %H:%M:%S";

#[allow(dead_code)]
pub fn now_str() -> String {
    let now = Utc::now();
    now.format(TIMESTAMP_FORMAT).to_string()
}
