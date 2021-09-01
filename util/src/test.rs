use std::path::{Path, PathBuf};
use tempdir::TempDir;

pub fn test_dir() -> TempDir {
    let basedir = std::env::var("BITCOINS_TEST_RAMFS_DIR").unwrap_or_else(|_| "/media/ramdisk".to_string());
    tempdir::TempDir::new(&format!("{}/test_data", basedir)).unwrap()
}

pub fn repo_dir() -> String {
    std::env::var_os("BITCOINS_REPO")
        .and_then(|oss| oss.into_string().ok())
        .unwrap_or_else(|| "..".to_string())
}

pub fn data_dir() -> PathBuf { Path::new(&repo_dir()).join("data") }

pub fn test_data_dir() -> PathBuf { Path::new(&repo_dir()).join("test_data") }
