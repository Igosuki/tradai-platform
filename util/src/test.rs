use std::path::{Path, PathBuf};

use tempdir::TempDir;

pub fn e2e_test_dir() -> String {
    std::env::var("BITCOINS_E2E_TEST_DIR").unwrap_or_else(|_| "/media/ramdisk/e2e_test_dir".to_string())
}

pub fn test_dir() -> TempDir {
    let basedir = std::env::var("BITCOINS_TEST_RAMFS_DIR").unwrap_or_else(|_| "/media/ramdisk".to_string());
    tempdir::TempDir::new(&format!("{}/test_data", basedir)).unwrap()
}

pub fn repo_dir() -> String {
    std::env::var_os("BITCOINS_REPO")
        .and_then(|oss| oss.into_string().ok())
        .unwrap_or_else(|| "..".to_string())
}

pub fn data_cache_dir() -> PathBuf {
    Path::new(&repo_dir())
        .join("history")
        .join("btc_history_cache")
        .join("data")
}

pub fn data_dir() -> PathBuf { Path::new(&repo_dir()).join("data") }

pub fn test_data_dir() -> PathBuf { Path::new(&repo_dir()).join("test_data") }

pub fn test_results_dir(module_path: &str) -> String {
    let module_path = module_path.replace("::", "_");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let test_results_dir = format!("{}/../target/test_results/{}", manifest_dir, module_path);
    std::fs::create_dir_all(&test_results_dir).unwrap();
    test_results_dir
}
