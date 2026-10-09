use std::{fs, sync::Arc};
use opendal_sync::{
    BlockingSyncEngine, CancellationToken, ConflictPolicy, Error, StorageConfig, SyncConfig,
    SyncEngine, SyncMode,
};

struct Fixture {
    _root: tempfile::TempDir,
    source: std::path::PathBuf,
    dest: std::path::PathBuf,
}
impl Fixture {
    fn new() -> Self {
        let root = tempfile::tempdir().unwrap();
        let source = root.path().join("source");
        let dest = root.path().join("dest");
        fs::create_dir(&source).unwrap();
        fs::create_dir(&dest).unwrap();
        Self {
            _root: root,
            source,
            dest,
        }
    }
    fn engine(&self, config: SyncConfig) -> SyncEngine {
        SyncEngine::new(
            StorageConfig::local(&self.source),
            StorageConfig::local(&self.dest),
            config,
        )
        .unwrap()
    }
}

#[tokio::test]
async fn backup_detects_equal_size_changes_and_is_idempotent() {
    let f = Fixture::new();
    fs::create_dir(f.source.join("nested")).unwrap();
    fs::write(f.source.join("nested/file"), "new!").unwrap();
    fs::create_dir(f.dest.join("nested")).unwrap();
    fs::write(f.dest.join("nested/file"), "old!").unwrap();
    fs::write(f.dest.join("keep"), "keep").unwrap();
    fs::write(f.source.join("empty"), "").unwrap();
    let engine = f.engine(SyncConfig {
        chunk_size: 2,
        ..SyncConfig::default()
    });
    let plan = engine.analyze().await.unwrap();
    assert_eq!(plan.summary().copy_count, 2);
    assert_eq!(
        fs::read_to_string(f.dest.join("nested/file")).unwrap(),
        "old!"
    );
    let report = engine.run().await.unwrap();
    assert!(report.is_success());
    assert_eq!(report.files_copied, 2);
    assert_eq!(report.bytes_transferred, 4);
    assert_eq!(
        fs::read_to_string(f.dest.join("nested/file")).unwrap(),
        "new!"
    );
    assert!(f.dest.join("keep").exists());
    assert_eq!(engine.run().await.unwrap().files_copied, 0);
}
#[tokio::test]
async fn mirror_preserves_excluded_files_and_deletes_extras() {
    let f = Fixture::new();
    fs::write(f.source.join("a"), "a").unwrap();
    fs::write(f.dest.join("extra"), "b").unwrap();
    fs::write(f.dest.join("secret.tmp"), "c").unwrap();
    let report = f
        .engine(
            SyncConfig::default()
                .with_mode(SyncMode::Mirror)
                .exclude("*.tmp"),
        )
        .run()
        .await
        .unwrap();
    assert_eq!(report.files_deleted, 1);
    assert!(!f.dest.join("extra").exists());
    assert!(f.dest.join("secret.tmp").exists());
}
#[tokio::test]
async fn missing_source_and_overlapping_roots_are_rejected() {
    let f = Fixture::new();
    fs::write(f.dest.join("keep"), "keep").unwrap();
    let config = SyncConfig::default().with_mode(SyncMode::Mirror);
    let engine = SyncEngine::new(
        StorageConfig::local(f.source.join("missing")),
        StorageConfig::local(&f.dest),
        config.clone(),
    )
    .unwrap();
    assert!(matches!(engine.run().await, Err(Error::InvalidConfig(_))));
    assert!(f.dest.join("keep").exists());
    let engine = SyncEngine::new(
        StorageConfig::local(&f.source),
        StorageConfig::local(f.source.join("nested")),
        config,
    )
    .unwrap();
    assert!(matches!(
        engine.analyze().await,
        Err(Error::InvalidConfig(_))
    ));
    assert!(!f.source.join("nested").exists());
}
#[tokio::test]
async fn preview_does_not_create_missing_destination() {
    let f = Fixture::new();
    let dest = f.dest.join("missing");
    fs::write(f.source.join("a"), "a").unwrap();
    let engine = SyncEngine::new(
        StorageConfig::local(&f.source),
        StorageConfig::local(&dest),
        SyncConfig::default(),
    )
    .unwrap();
    assert_eq!(engine.analyze().await.unwrap().summary().copy_count, 1);
    assert!(!dest.exists());
    engine.run().await.unwrap();
    assert_eq!(fs::read(dest.join("a")).unwrap(), b"a");
}
#[tokio::test]
async fn bidirectional_conflicts_fail_before_any_write_and_can_be_resolved() {
    let f = Fixture::new();
    fs::write(f.source.join("conflict"), "a").unwrap();
    fs::write(f.dest.join("conflict"), "b").unwrap();
    fs::write(f.source.join("forward"), "1").unwrap();
    fs::write(f.dest.join("reverse"), "2").unwrap();
    let config = SyncConfig::default().with_mode(SyncMode::Bidirectional);
    assert!(matches!(
        f.engine(config.clone()).run().await,
        Err(Error::Conflicts(1))
    ));
    assert!(!f.dest.join("forward").exists());
    assert!(!f.source.join("reverse").exists());
    let report = f
        .engine(config.with_conflict_policy(ConflictPolicy::PreferDestination))
        .run()
        .await
        .unwrap();
    assert_eq!(report.files_copied, 3);
    assert_eq!(fs::read(f.source.join("conflict")).unwrap(), b"b");
    assert!(f.source.join("reverse").exists());
    assert!(f.dest.join("forward").exists());
}
#[tokio::test]
async fn cancellation_and_progress_do_not_leave_detached_work() {
    let f = Fixture::new();
    fs::write(f.source.join("a"), "a").unwrap();
    let cancel = CancellationToken::new();
    cancel.cancel();
    assert!(matches!(
        f.engine(SyncConfig::default())
            .run_with_progress(&cancel, |_| {})
            .await,
        Err(Error::Cancelled)
    ));
    let cancel = CancellationToken::new();
    let report = f
        .engine(SyncConfig::default())
        .run_with_progress(&cancel, |_| cancel.cancel())
        .await
        .unwrap();
    assert!(report.cancelled);
    assert!(!f.dest.join("a").exists());
    assert!(
        f.engine(SyncConfig::default())
            .run()
            .await
            .unwrap()
            .is_success()
    );
}
#[test]
fn blocking_facade_and_configuration_validation() {
    let f = Fixture::new();
    fs::write(f.source.join("a"), "a").unwrap();
    let engine = BlockingSyncEngine::from_engine(f.engine(SyncConfig::default())).unwrap();
    assert_eq!(engine.analyze().unwrap().summary().copy_count, 1);
    assert!(engine.run().unwrap().is_success());
    let invalid = SyncConfig {
        max_concurrent_transfers: 0,
        ..SyncConfig::default()
    };
    assert!(
        SyncEngine::new(
            StorageConfig::local(&f.source),
            StorageConfig::local(&f.dest),
            invalid
        )
        .is_err()
    );
    assert!(
        SyncEngine::new(
            StorageConfig::local(&f.source),
            StorageConfig::local(&f.dest),
            SyncConfig::default().exclude("[bad")
        )
        .is_err()
    );
}
#[tokio::test]
async fn blocking_inside_tokio_returns_error() {
    let f = Fixture::new();
    assert!(matches!(
        BlockingSyncEngine::from_engine(f.engine(SyncConfig::default())),
        Err(Error::BlockingInAsyncContext)
    ));
}
#[tokio::test]
async fn file_directory_collisions_are_not_overwritten() {
    let f = Fixture::new();
    fs::write(f.source.join("same"), "a").unwrap();
    fs::create_dir(f.dest.join("same")).unwrap();
    fs::write(f.dest.join("same/keep"), "keep").unwrap();
    assert!(matches!(
        f.engine(SyncConfig::default().with_mode(SyncMode::Mirror))
            .run()
            .await,
        Err(Error::InvalidConfig(_))
    ));
    assert!(f.dest.join("same/keep").exists());
}
#[tokio::test]
async fn failed_local_stream_leaves_original_and_cleans_temp() {
    use opendal_sync::storage::{LocalStorage, Storage};
    let f = Fixture::new();
    fs::write(f.dest.join("a"), "old").unwrap();
    let storage = LocalStorage::new(&f.dest).unwrap();
    let chunks = futures::stream::iter(vec![
        Ok(b"new".to_vec()),
        Err(anyhow::anyhow!("read failed")),
    ]);
    assert!(
        storage
            .write_stream("a", Box::pin(chunks), Some(6))
            .await
            .is_err()
    );
    assert_eq!(fs::read(f.dest.join("a")).unwrap(), b"old");
    assert_eq!(fs::read_dir(&f.dest).unwrap().count(), 1);
    assert!(storage.write("../escape", vec![]).await.is_err());
}
#[cfg(unix)]
#[tokio::test]
async fn symlinks_abort_scan_before_mirror_deletion() {
    let f = Fixture::new();
    std::os::unix::fs::symlink(&f.dest, f.source.join("link")).unwrap();
    fs::write(f.dest.join("keep"), "keep").unwrap();
    assert!(
        f.engine(SyncConfig::default().with_mode(SyncMode::Mirror))
            .run()
            .await
            .is_err()
    );
    assert!(f.dest.join("keep").exists());
}

// Force a copy failure after analysis to verify that mirror does not delete extras.
struct FailingWriter(opendal_sync::storage::LocalStorage);
#[async_trait::async_trait]
impl opendal_sync::storage::Storage for FailingWriter {
    async fn list_files(
        &self,
        prefix: Option<&str>,
    ) -> anyhow::Result<Vec<opendal_sync::storage::FileInfo>> {
        self.0.list_files(prefix).await
    }
    async fn stat(&self, path: &str) -> anyhow::Result<Option<opendal_sync::storage::FileMeta>> {
        self.0.stat(path).await
    }
    async fn read(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        self.0.read(path).await
    }
    async fn read_range(&self, path: &str, offset: u64, length: u64) -> anyhow::Result<Vec<u8>> {
        self.0.read_range(path, offset, length).await
    }
    async fn write(&self, _: &str, _: Vec<u8>) -> anyhow::Result<()> {
        anyhow::bail!("injected failure")
    }
    async fn delete(&self, path: &str) -> anyhow::Result<()> {
        self.0.delete(path).await
    }
    async fn create_dir(&self, path: &str) -> anyhow::Result<()> {
        self.0.create_dir(path).await
    }
    fn name(&self) -> &'static str {
        "failing-writer"
    }
}
#[tokio::test]
async fn copy_failure_prevents_mirror_deletions() {
    let f = Fixture::new();
    fs::write(f.source.join("new"), "new").unwrap();
    fs::write(f.dest.join("extra"), "keep").unwrap();
    let engine = SyncEngine::from_storages(
        Arc::new(opendal_sync::storage::LocalStorage::new(&f.source).unwrap()),
        Arc::new(FailingWriter(
            opendal_sync::storage::LocalStorage::new(&f.dest).unwrap(),
        )),
        SyncConfig::default().with_mode(SyncMode::Mirror),
    )
    .unwrap();
    let report = engine.run().await.unwrap();
    assert!(!report.is_success());
    assert_eq!(report.files_failed, 1);
    assert_eq!(report.files_deleted, 0);
    assert!(f.dest.join("extra").exists());
    assert_eq!(report.errors[0].path, "new");
}
