use std::path::PathBuf;

/// # Panics
///
/// if the parent directories to the destination file cannot be crated
pub fn copy_file(from: &str, to: &str) {
    let pb = PathBuf::from(from);
    let pbt = PathBuf::from(to);
    let pbt_parent = pbt.parent().unwrap();
    std::fs::create_dir_all(pbt_parent).unwrap();
    std::fs::File::create(to).unwrap();
    let copied = std::fs::copy(std::fs::canonicalize(pb).unwrap(), to);
    assert!(copied.is_ok(), "{}", format!("{:?} : {}", copied, from));
}
