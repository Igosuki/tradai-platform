use chrono::Utc;

pub fn test_dir() -> String {
    let basedir = std::env::var("BITCOINS_TEST_RAMFS_DIR").unwrap_or_else(|_| "/media/ramdisk".to_string());
    let root = tempdir::TempDir::new(&format!("{}/test_data", basedir)).unwrap();
    let buf = root.into_path();
    let path = buf.to_str().unwrap();
    path.to_string()
}

pub const TIMESTAMP_FORMAT: &str = "%Y%m%d %H:%M:%S";

pub fn now_str() -> String {
    let now = Utc::now();
    now.format(TIMESTAMP_FORMAT).to_string()
}
