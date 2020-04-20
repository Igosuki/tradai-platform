#[cfg(test)]
pub mod test {
    use rusoto_core::HttpClient;
    use rusoto_credential::ProfileProvider;
    use rusoto_s3::{GetObjectRequest, S3};
    use std::io::Write;
    use std::path::PathBuf;
    use tokio::io::AsyncReadExt;

    pub async fn download_file(key: &str, dest: PathBuf) -> std::io::Result<()> {
        let dispatcher = HttpClient::new().expect("failed to create request dispatcher");
        let mut provider = ProfileProvider::new().unwrap();
        provider.set_profile("btcfeed");
        let client = rusoto_s3::S3Client::new_with(
            dispatcher,
            provider,
            rusoto_core::Region::Custom {
                endpoint: "https://nyc3.digitaloceanspaces.com".to_string(),
                name: "do_nyc3".to_string(),
            },
        );
        let object = client
            .get_object(GetObjectRequest {
                bucket: "btcfeed".to_string(),
                key: key.to_string(),
                ..GetObjectRequest::default()
            })
            .await
            .unwrap();
        let mut result = std::fs::File::create(dest)?;
        let mut read = object.body.unwrap().into_async_read();
        let mut buf = Vec::new();
        read.read_to_end(&mut buf).await?;
        result.write(&buf)?;
        result.flush()
    }
}
