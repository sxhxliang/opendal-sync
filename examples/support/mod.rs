//! Utilities shared by examples, not part of the library's public API.
// Each example uses only part of this support module.
#![allow(dead_code)]

use anyhow::{Context, Result};
use futures::{Stream, StreamExt};
use std::{
    collections::HashMap,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use opendal_sync::{
    Storage, SyncConfig, SyncEngine, SyncMode,
    storage::{FileInfo, FileMeta, LocalStorage},
};

type ListingCache = HashMap<Option<String>, (Instant, Vec<FileInfo>)>;

/// Example-only adapter: remote listing TTL plus a small-file buffering threshold.
/// Mutations invalidate its cache. It does not cache file contents or checksums.
pub struct ExampleStorage {
    inner: Arc<dyn Storage>,
    ttl: Duration,
    cache: Mutex<ListingCache>,
    stream_threshold: u64,
}
impl ExampleStorage {
    pub fn new(
        inner: Arc<dyn Storage>,
        remote: bool,
        ttl: Duration,
        stream_threshold: u64,
    ) -> Result<Self> {
        anyhow::ensure!(
            ttl <= Duration::from_secs(7200),
            "CACHE_TTL_SECS must be in 0..=7200"
        );
        Ok(Self {
            inner,
            ttl: if remote { ttl } else { Duration::ZERO },
            cache: Mutex::new(HashMap::new()),
            stream_threshold,
        })
    }
    pub fn clear_cache(&self) {
        // Poisoned entries must never be reused for deletion decisions.
        self.cache
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
    }
}
#[opendal_sync::async_trait]
impl Storage for ExampleStorage {
    async fn list_files(&self, prefix: Option<&str>) -> Result<Vec<FileInfo>> {
        let key = prefix.map(str::to_owned);
        if !self.ttl.is_zero() {
            let cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if let Some((created, files)) = cache.get(&key)
                && created.elapsed() < self.ttl
            {
                eprintln!("listing cache HIT: {}", self.name());
                return Ok(files.clone());
            }
        }
        let files = self.inner.list_files(prefix).await?;
        if !self.ttl.is_zero() {
            let mut cache = self
                .cache
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            // Demonstration uses whole-root listings; bound custom-prefix accumulation.
            if cache.len() >= 16 {
                cache.clear();
            }
            cache.insert(key, (Instant::now(), files.clone()));
            eprintln!("listing cache MISS: {}", self.name());
        }
        Ok(files)
    }
    async fn stat(&self, path: &str) -> Result<Option<FileMeta>> {
        self.inner.stat(path).await
    }
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        self.inner.read(path).await
    }
    async fn read_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        self.inner.read_range(path, offset, length).await
    }
    async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.clear_cache();
        let result = self.inner.write(path, data).await;
        self.clear_cache();
        result
    }
    async fn write_stream<'stream>(
        &self,
        path: &str,
        mut stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send + 'stream>>,
        total_size: Option<u64>,
    ) -> Result<()> {
        self.clear_cache();
        let result = if let Some(size) = total_size.filter(|size| *size <= self.stream_threshold) {
            eprintln!("buffered write: {path}, {size} bytes");
            let mut data = Vec::new();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                anyhow::ensure!(
                    data.len() as u64 + chunk.len() as u64 <= size,
                    "stream exceeds declared size"
                );
                data.extend(chunk);
            }
            anyhow::ensure!(data.len() as u64 == size, "incomplete stream");
            self.inner.write(path, data).await
        } else {
            eprintln!("streaming write: {path}, size={total_size:?}");
            self.inner.write_stream(path, stream, total_size).await
        };
        self.clear_cache();
        result
    }
    async fn delete(&self, path: &str) -> Result<()> {
        self.clear_cache();
        let result = self.inner.delete(path).await;
        self.clear_cache();
        result
    }
    async fn create_dir(&self, path: &str) -> Result<()> {
        self.clear_cache();
        let result = self.inner.create_dir(path).await;
        self.clear_cache();
        result
    }
    fn name(&self) -> &str {
        self.inner.name()
    }
}

pub fn number(name: &str, default: u64) -> Result<u64> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .with_context(|| format!("{name} must be a nonnegative integer")),
        Err(std::env::VarError::NotPresent) => Ok(default),
        Err(error) => Err(error.into()),
    }
}
fn required(name: &str) -> Result<String> {
    std::env::var(name).with_context(|| format!("set {name}"))
}
fn optional(name: &str) -> Option<String> {
    std::env::var(name).ok().filter(|s| !s.is_empty())
}

fn endpoint(prefix: &str) -> Result<(Arc<dyn Storage>, bool)> {
    let kind = std::env::var(format!("{prefix}_KIND")).unwrap_or_else(|_| "local".into());
    match kind.as_str() {
        "local" => {
            let path = required(&format!("{prefix}_PATH"))?;
            if prefix == "SRC" {
                anyhow::ensure!(
                    std::path::Path::new(&path).is_dir(),
                    "source directory must exist"
                );
            }
            Ok((Arc::new(LocalStorage::new(path)?), false))
        }
        #[cfg(feature = "services-s3")]
        "s3" => Ok((
            Arc::new(opendal_sync::storage::S3Storage::new(
                &required(&format!("{prefix}_BUCKET"))?,
                &required(&format!("{prefix}_REGION"))?,
                &required(&format!("{prefix}_ACCESS_KEY"))?,
                &required(&format!("{prefix}_SECRET_KEY"))?,
                optional(&format!("{prefix}_ENDPOINT")).as_deref(),
                optional(&format!("{prefix}_PREFIX")).as_deref(),
            )?),
            true,
        )),
        #[cfg(feature = "services-webdav")]
        "webdav" => Ok((
            Arc::new(opendal_sync::storage::WebDavStorage::new(
                &required(&format!("{prefix}_ENDPOINT"))?,
                &required(&format!("{prefix}_USERNAME"))?,
                &required(&format!("{prefix}_PASSWORD"))?,
                optional(&format!("{prefix}_ROOT")).as_deref(),
            )?),
            true,
        )),
        _ => {
            anyhow::bail!("unsupported {prefix}_KIND={kind}; enable the matching s3/webdav feature")
        }
    }
}

pub struct Pair {
    pub source: Arc<ExampleStorage>,
    pub destination: Arc<ExampleStorage>,
}
impl Pair {
    pub fn from_env() -> Result<Self> {
        let (source, source_remote) = endpoint("SRC")?;
        let (destination, destination_remote) = endpoint("DST")?;
        // Custom storage injection bypasses the core's local-root overlap check.
        if !source_remote && !destination_remote {
            let source_path = std::fs::canonicalize(required("SRC_PATH")?)?;
            let dest_path = absolute_destination(std::path::Path::new(&required("DST_PATH")?))?;
            anyhow::ensure!(
                !source_path.starts_with(&dest_path) && !dest_path.starts_with(&source_path),
                "local roots must not overlap"
            );
        }
        let ttl = Duration::from_secs(number("CACHE_TTL_SECS", 0)?);
        let threshold = number("STREAM_THRESHOLD_MIB", 8)?
            .checked_mul(1024 * 1024)
            .context("threshold overflow")?;
        Ok(Self {
            source: Arc::new(ExampleStorage::new(source, source_remote, ttl, threshold)?),
            destination: Arc::new(ExampleStorage::new(
                destination,
                destination_remote,
                ttl,
                threshold,
            )?),
        })
    }
    pub fn engine(&self, config: SyncConfig) -> Result<SyncEngine> {
        Ok(SyncEngine::from_storages(
            self.source.clone(),
            self.destination.clone(),
            config,
        )?)
    }
    pub fn refresh(&self) {
        self.source.clear_cache();
        self.destination.clear_cache();
    }
}
fn absolute_destination(path: &std::path::Path) -> Result<std::path::PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut ancestor = absolute.as_path();
    let mut suffix = Vec::new();
    while !ancestor.exists() {
        suffix.push(
            ancestor
                .file_name()
                .context("invalid destination")?
                .to_owned(),
        );
        ancestor = ancestor.parent().context("invalid destination")?;
    }
    let mut result = ancestor.canonicalize()?;
    for part in suffix.into_iter().rev() {
        result.push(part);
    }
    Ok(result)
}
pub fn config() -> Result<SyncConfig> {
    let mode = match std::env::var("SYNC_MODE")
        .unwrap_or_else(|_| "backup".into())
        .as_str()
    {
        "backup" => SyncMode::Backup,
        "mirror" => SyncMode::Mirror,
        "bidirectional" => SyncMode::Bidirectional,
        _ => anyhow::bail!("SYNC_MODE must be backup, mirror or bidirectional"),
    };
    let concurrency = number("MAX_CONCURRENT", 4)?;
    anyhow::ensure!(
        (1..=128).contains(&concurrency),
        "MAX_CONCURRENT must be in 1..=128"
    );
    let chunk = number("CHUNK_MIB", 8)?
        .checked_mul(1024 * 1024)
        .context("chunk size overflow")?;
    anyhow::ensure!(chunk > 0, "CHUNK_MIB must be positive");
    Ok(SyncConfig {
        mode,
        max_concurrent_transfers: usize::try_from(concurrency)?,
        chunk_size: usize::try_from(chunk)?,
        ..SyncConfig::default()
    })
}
pub fn apply_requested() -> Result<bool> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    anyhow::ensure!(
        args.is_empty() || args == ["--apply"],
        "only optional argument: --apply"
    );
    Ok(!args.is_empty())
}
pub fn print_plan(plan: &opendal_sync::SyncPlan) {
    for action in plan.actions() {
        println!("{action:?}");
    }
    println!("Summary: {:?}", plan.summary());
}
pub fn check_report(report: &opendal_sync::SyncReport) -> Result<()> {
    println!("{report:?}");
    anyhow::ensure!(report.is_success(), "sync cancelled or partially failed");
    Ok(())
}
/// Streaming copy for manual conflict resolution; parent creation belongs to the backend.
pub async fn copy_file(
    from: &dyn Storage,
    to: &dyn Storage,
    source: &str,
    target: &str,
    chunk_size: usize,
) -> Result<()> {
    let meta = from.stat(source).await?.context("file disappeared")?;
    anyhow::ensure!(!meta.is_dir, "expected a file");
    let chunks = futures::stream::try_unfold(0u64, |offset| async move {
        if offset == meta.size {
            return Ok(None);
        }
        let len = (meta.size - offset).min(chunk_size as u64);
        let data = from.read_range(source, offset, len).await?;
        anyhow::ensure!(data.len() as u64 == len, "file changed during copy");
        Ok(Some((data, offset + len)))
    });
    to.write_stream(target, Box::pin(chunks), Some(meta.size))
        .await
}

pub async fn keep_both(pair: &Pair, path: &str, chunk_size: usize) -> Result<String> {
    let mut index = 1u64;
    let backup = loop {
        let candidate = format!("{path}.conflict-destination-{index}");
        if !pair.source.exists(&candidate).await? && !pair.destination.exists(&candidate).await? {
            break candidate;
        }
        index = index.checked_add(1).context("conflict suffix overflow")?;
    };
    // Preserve the destination version on BOTH sides before replacing its original path.
    copy_file(
        pair.destination.as_ref(),
        pair.source.as_ref(),
        path,
        &backup,
        chunk_size,
    )
    .await?;
    copy_file(
        pair.destination.as_ref(),
        pair.destination.as_ref(),
        path,
        &backup,
        chunk_size,
    )
    .await?;
    copy_file(
        pair.source.as_ref(),
        pair.destination.as_ref(),
        path,
        path,
        chunk_size,
    )
    .await?;
    Ok(backup)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn remote_cache_expires_and_writes_invalidate_but_local_never_caches() -> Result<()> {
        let directory = tempfile::tempdir()?;
        let inner = Arc::new(LocalStorage::new(directory.path())?);
        let remote = ExampleStorage::new(inner.clone(), true, Duration::from_secs(600), 4)?;
        assert!(remote.list_files(None).await?.is_empty());
        inner.write("a", vec![1]).await?;
        assert!(remote.list_files(None).await?.is_empty());
        // Age the entry without sleeps to exercise expiry deterministically.
        remote.cache.lock().unwrap().get_mut(&None).unwrap().0 =
            Instant::now() - Duration::from_secs(601);
        assert_eq!(remote.list_files(None).await?.len(), 1);
        remote.write("b", vec![2]).await?;
        assert_eq!(remote.list_files(None).await?.len(), 2);
        let local = ExampleStorage::new(inner.clone(), false, Duration::from_secs(7200), 0)?;
        assert_eq!(local.list_files(None).await?.len(), 2);
        inner.write("c", vec![3]).await?;
        assert_eq!(local.list_files(None).await?.len(), 3);
        let disabled = ExampleStorage::new(inner.clone(), true, Duration::ZERO, 0)?;
        disabled.list_files(None).await?;
        assert!(disabled.cache.lock().unwrap().is_empty());
        assert!(ExampleStorage::new(inner, true, Duration::from_secs(7201), 0).is_err());
        Ok(())
    }

    #[tokio::test]
    async fn both_stream_paths_validate_and_preserve_existing_files() -> Result<()> {
        let directory = tempfile::tempdir()?;
        for threshold in [0, 16] {
            let storage = ExampleStorage::new(
                Arc::new(LocalStorage::new(directory.path())?),
                false,
                Duration::ZERO,
                threshold,
            )?;
            storage.write("a", b"old".to_vec()).await?;
            let bad = futures::stream::iter(vec![
                Ok(b"new".to_vec()),
                Err(anyhow::anyhow!("injected error")),
            ]);
            assert!(
                storage
                    .write_stream("a", Box::pin(bad), Some(6))
                    .await
                    .is_err()
            );
            assert_eq!(storage.read("a").await?, b"old");
            let good = futures::stream::iter(vec![Ok(b"new".to_vec())]);
            storage.write_stream("a", Box::pin(good), Some(3)).await?;
            assert_eq!(storage.read("a").await?, b"new");
        }
        Ok(())
    }

    #[tokio::test]
    async fn keep_both_preserves_versions_without_overwriting_existing_backups() -> Result<()> {
        let root = tempfile::tempdir()?;
        let a = root.path().join("a");
        let b = root.path().join("b");
        std::fs::create_dir(&a)?;
        std::fs::create_dir(&b)?;
        let pair = Pair {
            source: Arc::new(ExampleStorage::new(
                Arc::new(LocalStorage::new(&a)?),
                false,
                Duration::ZERO,
                0,
            )?),
            destination: Arc::new(ExampleStorage::new(
                Arc::new(LocalStorage::new(&b)?),
                false,
                Duration::ZERO,
                0,
            )?),
        };
        pair.source.write("file.txt", b"source".to_vec()).await?;
        pair.destination
            .write("file.txt", b"destination".to_vec())
            .await?;
        pair.source
            .write("file.txt.conflict-destination-1", b"keep".to_vec())
            .await?;
        let backup = keep_both(&pair, "file.txt", 2).await?;
        assert_eq!(backup, "file.txt.conflict-destination-2");
        for side in [&pair.source, &pair.destination] {
            assert_eq!(side.read("file.txt").await?, b"source");
            assert_eq!(side.read(&backup).await?, b"destination");
        }
        assert_eq!(
            pair.source.read("file.txt.conflict-destination-1").await?,
            b"keep"
        );
        Ok(())
    }
}
