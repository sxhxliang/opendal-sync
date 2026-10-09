use super::{FileInfo, FileMeta, Storage};
use anyhow::Result;
use futures::{Stream, StreamExt, TryStreamExt};
use opendal::{ErrorKind, Operator};
use std::{collections::HashSet, pin::Pin};

/// Adapt any enabled `OpenDAL` operator to the synchronization interface.
///
/// Requires list, stat and read support. Read-only backends can be sources;
/// destinations additionally need write, and mirror destinations need delete.
/// Capability support does not guarantee service credentials or permissions.
/// Caller-provided operator roots must be distinct and non-overlapping.
pub struct OperatorStorage {
    operator: Operator,
    name: String,
}
impl OperatorStorage {
    /// Wrap an operator, preserving its configuration and middleware layers.
    ///
    /// When an HTTP transport feature is enabled, lazily installs that default
    /// transport. A previously installed process-wide transport takes precedence.
    ///
    /// # Errors
    /// Returns an error if list, stat or read is unsupported. No network I/O occurs.
    pub fn new(operator: Operator) -> Result<Self> {
        super::init_http_transport();
        let info = operator.info();
        let capability = info.capability();
        anyhow::ensure!(
            capability.list && capability.stat && capability.read,
            "backend {} needs list, stat and read support for synchronization",
            info.scheme()
        );
        Ok(Self {
            name: format!("opendal:{}", info.scheme()),
            operator,
        })
    }
    /// Borrow the underlying operator for backend-specific operations.
    #[must_use]
    pub fn operator(&self) -> &Operator {
        &self.operator
    }
}
#[crate::async_trait]
impl Storage for OperatorStorage {
    async fn list_files(&self, prefix: Option<&str>) -> Result<Vec<FileInfo>> {
        let recursive = self.operator.info().capability().list_with_recursive;
        let mut pending = vec![prefix.unwrap_or("").to_owned()];
        let mut visited = HashSet::new();
        let mut seen = HashSet::new();
        let mut files = Vec::new();
        while let Some(directory) = pending.pop() {
            if !visited.insert(directory.clone()) {
                continue;
            }
            let mut entries = self
                .operator
                .lister_with(&directory)
                .recursive(recursive)
                .await?;
            while let Some(entry) = entries.try_next().await? {
                let path = entry.path().trim_end_matches('/');
                if path.is_empty() || path == directory.trim_end_matches('/') {
                    continue;
                }
                super::validate_path(path)?;
                if !seen.insert(path.to_owned()) {
                    continue;
                }
                // Some services only return keys from list. Fetch authoritative metadata.
                let meta = self.operator.stat(entry.path()).await?;
                if meta.is_dir() && !recursive {
                    pending.push(format!("{path}/"));
                }
                files.push(FileInfo {
                    path: path.to_owned(),
                    size: meta.content_length(),
                    modified_time: meta
                        .last_modified()
                        .map_or(0, |time| time.into_inner().as_second()),
                    is_dir: meta.is_dir(),
                    checksum: None,
                });
            }
        }
        Ok(files)
    }
    async fn stat(&self, path: &str) -> Result<Option<FileMeta>> {
        match self.operator.stat(path).await {
            Ok(meta) => Ok(Some(FileMeta {
                size: meta.content_length(),
                modified_time: meta
                    .last_modified()
                    .map_or(0, |time| time.into_inner().as_second()),
                is_dir: meta.is_dir(),
                etag: meta.etag().map(str::to_owned),
            })),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error.into()),
        }
    }
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        Ok(self.operator.read(path).await?.to_vec())
    }
    async fn read_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let end = offset
            .checked_add(length)
            .ok_or_else(|| anyhow::anyhow!("range overflow"))?;
        if length == 0 {
            return Ok(Vec::new());
        }
        Ok(self
            .operator
            .read_with(path)
            .range(offset..end)
            .await?
            .to_vec())
    }
    async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.operator.write(path, data).await?;
        Ok(())
    }
    async fn write_stream<'stream>(
        &self,
        path: &str,
        mut chunks: Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send + 'stream>>,
        total_size: Option<u64>,
    ) -> Result<()> {
        let mut writer = self.operator.writer(path).await?;
        let result: Result<()> = async {
            let mut written = 0u64;
            while let Some(chunk) = chunks.next().await {
                let chunk = chunk?;
                written = written
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| anyhow::anyhow!("size overflow"))?;
                writer.write(chunk).await?;
            }
            anyhow::ensure!(
                total_size.is_none_or(|size| size == written),
                "incomplete stream"
            );
            writer.close().await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = writer.abort().await;
        }
        result
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.operator.delete(path).await?;
        Ok(())
    }
    async fn create_dir(&self, path: &str) -> Result<()> {
        self.operator.create_dir(path).await?;
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
}
