use std::path::{Path, PathBuf};

use tempdir::TempDir;

/// A base directory for test files and results of end to end tests
#[must_use]
pub fn e2e_test_dir() -> String {
    std::env::var("TRADAI_E2E_TEST_DIR").unwrap_or_else(|_| "/media/ramdisk/e2e_test_dir".to_string())
}

/// A base directory for test files and results
///
/// # Panics
///
/// Will panic if the env var is not set and a temporary directory cannot be created
#[must_use]
pub fn test_dir() -> TempDir {
    let basedir = std::env::var("TRADAI_TEST_RAMFS_DIR").unwrap_or_else(|_| "/media/ramdisk".to_string());
    let dir = tempdir::TempDir::new(&format!("{}/test_data", basedir)).unwrap();
    info!("using test dir {:?}", dir);
    dir
}

/// The base directory of the code repository
#[must_use]
pub fn repo_dir() -> String {
    std::env::var_os("TRADAI_REPO")
        .and_then(|oss| oss.into_string().ok())
        .unwrap_or_else(|| "..".to_string())
}

/// The base directory of test data
#[must_use]
pub fn test_data_dir() -> PathBuf {
    Path::new(&std::env::var("TRADAI_DATA_TEST_DIR").unwrap_or_else(|_| "test_data".to_string())).to_path_buf()
}

/// The base directory of test results
/// # Panics
///
/// Panics if the test results directory cannot be created
#[must_use]
pub fn test_results_dir(module_path: &str) -> String {
    let module_path = module_path.replace("::", "_");
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let test_results_dir = format!("{}/../target/test_results/{}", manifest_dir, module_path);
    std::fs::create_dir_all(&test_results_dir).unwrap();
    test_results_dir
}
