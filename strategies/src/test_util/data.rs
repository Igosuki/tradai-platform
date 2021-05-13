#[cfg(test)]
pub mod data_test_util {
    pub fn test_dir() -> String {
        let basedir = std::env::var("BITCOINS_TEST_RAMFS_DIR").unwrap_or_else(|_| "/media/ramdisk".to_string());
        let root = tempdir::TempDir::new(&format!("{}/test_data", basedir)).unwrap();
        let buf = root.into_path();
        let path = buf.to_str().unwrap();
        path.to_string()
    }
}
