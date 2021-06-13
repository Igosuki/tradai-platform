use tempdir::TempDir;

pub fn test_dir() -> TempDir {
    let basedir = std::env::var("BITCOINS_TEST_RAMFS_DIR").unwrap_or_else(|_| "/media/ramdisk".to_string());
    tempdir::TempDir::new(&format!("{}/test_data", basedir)).unwrap()
}
