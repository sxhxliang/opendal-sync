#![cfg(all(feature = "services-memory", feature = "services-fs"))]
use std::sync::Arc;
use opendal_sync::{
    Storage, StorageConfig, SyncConfig, SyncEngine, SyncMode,
    opendal::{Operator, services},
    storage::{LocalStorage, OperatorStorage},
};

#[tokio::test]
async fn memory_to_filesystem_mirror_and_repeat() -> anyhow::Result<()> {
    let source = Operator::new(services::Memory::default())?;
    let directory = tempfile::tempdir()?;
    let destination =
        Operator::new(services::Fs::default().root(directory.path().to_str().unwrap()))?;
    source.write("nested/file", "new").await?;
    source.write("empty", "").await?;
    destination.write("extra", "old").await?;
    let engine = SyncEngine::from_storages(
        Arc::new(OperatorStorage::new(source)?),
        Arc::new(OperatorStorage::new(destination.clone())?),
        SyncConfig {
            mode: SyncMode::Mirror,
            chunk_size: 2,
            ..SyncConfig::default()
        },
    )?;
    assert_eq!(engine.analyze().await?.summary().delete_count, 1);
    assert!(destination.exists("extra").await?);
    let report = engine.run().await?;
    assert!(report.is_success());
    assert_eq!(report.files_copied, 2);
    assert_eq!(report.files_deleted, 1);
    assert_eq!(std::fs::read(directory.path().join("nested/file"))?, b"new");
    assert_eq!(engine.run().await?.files_copied, 0);
    Ok(())
}

#[tokio::test]
async fn local_to_operator_overwrites_equal_size_content_and_stream_failure_propagates()
-> anyhow::Result<()> {
    let directory = tempfile::tempdir()?;
    std::fs::write(directory.path().join("a"), "new")?;
    let operator = Operator::new(services::Memory::default())?;
    operator.write("a", "old").await?;
    let adapter = Arc::new(OperatorStorage::new(operator.clone())?);
    let engine = SyncEngine::from_storages(
        Arc::new(LocalStorage::new(directory.path())?),
        adapter.clone(),
        SyncConfig::default(),
    )?;
    assert_eq!(engine.run().await?.files_copied, 1);
    assert_eq!(operator.read("a").await?.to_vec(), b"new");
    assert!(adapter.read_range("a", u64::MAX, 1).await.is_err());
    let chunks = futures::stream::iter(vec![
        Ok(vec![1]),
        Err(anyhow::anyhow!("injected read failure")),
    ]);
    assert!(
        adapter
            .write_stream("failed", Box::pin(chunks), Some(2))
            .await
            .is_err()
    );
    // Existing local convenience API stays independent of any OpenDAL service.
    let _ = StorageConfig::local(directory.path());
    Ok(())
}

#[cfg(feature = "services-http")]
#[test]
fn backend_without_listing_is_rejected_before_io() -> anyhow::Result<()> {
    let operator = Operator::new(services::Http::default().endpoint("https://example.invalid"))?;
    let result = OperatorStorage::new(operator);
    assert!(result.is_err());
    assert!(
        result
            .err()
            .unwrap()
            .to_string()
            .contains("list, stat and read")
    );
    Ok(())
}
