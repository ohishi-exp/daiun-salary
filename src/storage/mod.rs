pub mod r2;

pub use r2::R2Backend;

#[derive(Debug, thiserror::Error)]
pub enum StorageError {
    #[error("Upload failed: {0}")]
    Upload(String),
    #[error("Config error: {0}")]
    Config(String),
}

#[async_trait::async_trait]
pub trait StorageBackend: Send + Sync {
    async fn upload(
        &self,
        key: &str,
        data: &[u8],
        content_type: &str,
    ) -> Result<String, StorageError>;

    fn public_url(&self, key: &str) -> String;

    async fn download(&self, key: &str) -> Result<Vec<u8>, StorageError>;

    fn extract_key(&self, url: &str) -> Option<String>;

    fn bucket(&self) -> &str;
}
