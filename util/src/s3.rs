use std::io::Write;
use std::path::PathBuf;
use std::process::Command;

pub async fn download_file(key: &str, dest: PathBuf) -> std::io::Result<()> {
    let profile = "btcfeed";
    let from_path = format!("s3://btcfeed/{}", key);
    Command::new("aws")
        .arg("s3")
        .arg("cp")
        .arg("--profile")
        .arg(profile)
        .arg("--endpoint")
        .arg("https://nyc3.digitaloceanspaces.com")
        .arg(from_path)
        .arg(dest)
        .output()?;
    Ok(())
}
