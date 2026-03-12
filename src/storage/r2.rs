use s3::bucket::Bucket;
use s3::creds::Credentials;
use s3::Region;

use super::{StorageBackend, StorageError};

pub struct R2Backend {
    bucket: Box<Bucket>,
    bucket_name: String,
    public_url_base: String,
}

impl R2Backend {
    pub fn new(
        bucket_name: String,
        account_id: String,
        access_key: String,
        secret_key: String,
        public_url_base: Option<String>,
    ) -> Result<Self, StorageError> {
        let region = Region::Custom {
            region: "auto".to_string(),
            endpoint: format!("https://{}.r2.cloudflarestorage.com", account_id),
        };

        let credentials = Credentials::new(
            Some(&access_key),
            Some(&secret_key),
            None,
            None,
            None,
        )
        .map_err(|e| StorageError::Config(format!("R2 credentials: {e}")))?;

        let bucket = Bucket::new(&bucket_name, region, credentials)
            .map_err(|e| StorageError::Config(format!("R2 bucket: {e}")))?;

        let public_url_base = public_url_base
            .unwrap_or_else(|| format!("https://{}.r2.dev/{}", account_id, bucket_name));

        Ok(Self {
            bucket,
            bucket_name,
            public_url_base,
        })
    }
}

#[async_trait::async_trait]
impl StorageBackend for R2Backend {
    async fn upload(
        &self,
        key: &str,
        data: &[u8],
        content_type: &str,
    ) -> Result<String, StorageError> {
        self.bucket
            .put_object_with_content_type(key, data, content_type)
            .await
            .map_err(|e| StorageError::Upload(format!("R2 upload: {e}")))?;

        tracing::info!("R2 upload: bucket={}, key={}", self.bucket_name, key);
        Ok(self.public_url(key))
    }

    fn public_url(&self, key: &str) -> String {
        format!("{}/{}", self.public_url_base, key)
    }

    async fn download(&self, key: &str) -> Result<Vec<u8>, StorageError> {
        let response = self
            .bucket
            .get_object(key)
            .await
            .map_err(|e| StorageError::Upload(format!("R2 download: {e}")))?;

        let status = response.status_code();
        if status < 200 || status >= 300 {
            return Err(StorageError::Upload(format!(
                "R2 download status {}: {}",
                status,
                String::from_utf8_lossy(response.as_slice())
            )));
        }

        Ok(response.to_vec())
    }

    fn extract_key(&self, url: &str) -> Option<String> {
        let prefix = format!("{}/", self.public_url_base);
        url.strip_prefix(&prefix).map(|s| s.to_string())
    }

    fn bucket(&self) -> &str {
        &self.bucket_name
    }
}
