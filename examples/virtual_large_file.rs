//! Transfer a synthetic 5 GiB file through the real engine without allocating it all
//! or writing it to disk. This checks u64 offsets and streaming, not disk/network speed.
use anyhow::Result;
use futures::{Stream, StreamExt};
use std::{
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use opendal_sync::{
    Storage, SyncConfig, SyncEngine,
    storage::{FileInfo, FileMeta},
};

const SIZE: u64 = 5 * 1024 * 1024 * 1024 + 17;
const CHUNK: usize = 2 * 1024 * 1024;
struct VirtualStorage {
    source: bool,
    bytes: AtomicU64,
}
#[opendal_sync::async_trait]
impl Storage for VirtualStorage {
    async fn list_files(&self, _: Option<&str>) -> Result<Vec<FileInfo>> {
        Ok(if self.source {
            vec![FileInfo {
                path: "large.bin".into(),
                size: SIZE,
                modified_time: 0,
                is_dir: false,
                checksum: None,
            }]
        } else {
            vec![]
        })
    }
    async fn stat(&self, path: &str) -> Result<Option<FileMeta>> {
        Ok(self.source.then_some(FileMeta {
            size: if path.is_empty() { 0 } else { SIZE },
            modified_time: 0,
            is_dir: path.is_empty(),
            etag: None,
        }))
    }
    async fn read(&self, _: &str) -> Result<Vec<u8>> {
        anyhow::bail!("whole-file read forbidden in this streaming demo")
    }
    async fn read_range(&self, _: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        anyhow::ensure!(
            self.source && length <= CHUNK as u64 && offset + length <= SIZE,
            "invalid read"
        );
        // One file is copied sequentially. Verify exact offsets across the 4 GiB boundary.
        anyhow::ensure!(
            self.bytes.fetch_add(length, Ordering::Relaxed) == offset,
            "non-contiguous offset"
        );
        Ok(vec![0; usize::try_from(length)?])
    }
    async fn write(&self, _: &str, _: Vec<u8>) -> Result<()> {
        anyhow::bail!("whole-file write forbidden")
    }
    async fn write_stream<'stream>(
        &self,
        _: &str,
        mut chunks: Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send + 'stream>>,
        total_size: Option<u64>,
    ) -> Result<()> {
        anyhow::ensure!(
            !self.source && total_size == Some(SIZE),
            "invalid destination or length"
        );
        while let Some(chunk) = chunks.next().await {
            let chunk = chunk?;
            anyhow::ensure!(chunk.len() <= CHUNK, "unbounded chunk");
            self.bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
        }
        anyhow::ensure!(
            self.bytes.load(Ordering::Relaxed) == SIZE,
            "truncated transfer"
        );
        Ok(())
    }
    async fn delete(&self, _: &str) -> Result<()> {
        anyhow::bail!("delete not expected")
    }
    async fn create_dir(&self, _: &str) -> Result<()> {
        anyhow::bail!("create_dir not expected")
    }
    fn name(&self) -> &str {
        if self.source {
            "virtual-source"
        } else {
            "counting-sink"
        }
    }
}
#[tokio::main]
async fn main() -> Result<()> {
    let source = Arc::new(VirtualStorage {
        source: true,
        bytes: AtomicU64::new(0),
    });
    let destination = Arc::new(VirtualStorage {
        source: false,
        bytes: AtomicU64::new(0),
    });
    let engine = SyncEngine::from_storages(
        source.clone(),
        destination.clone(),
        SyncConfig {
            chunk_size: CHUNK,
            max_concurrent_transfers: 1,
            ..SyncConfig::default()
        },
    )?;
    let report = engine.run().await?;
    anyhow::ensure!(
        report.is_success() && report.bytes_transferred == SIZE,
        "{report:?}"
    );
    assert_eq!(source.bytes.load(Ordering::Relaxed), SIZE);
    assert_eq!(destination.bytes.load(Ordering::Relaxed), SIZE);
    println!(
        "Verified {SIZE} bytes (>4 GiB), chunks <= {CHUNK} bytes; no large disk file created."
    );
    Ok(())
}
