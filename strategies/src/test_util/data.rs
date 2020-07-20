#[cfg(test)]
pub mod data_test_util {
    pub fn test_dir() -> String {
        let root = tempdir::TempDir::new("test_data2").unwrap();
        let buf = root.into_path();
        let path = buf.to_str().unwrap();
        path.to_string()
    }
}
