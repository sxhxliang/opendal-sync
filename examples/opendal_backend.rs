//! No credentials required: two user-selected `OpenDAL` memory operators.
use std::sync::Arc;
use opendal_sync::{
    SyncConfig, SyncEngine, SyncMode,
    opendal::{Operator, services},
    storage::OperatorStorage,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Replace Memory with any enabled service builder and supply its configuration.
    let source = Operator::new(services::Memory::default())?;
    let destination = Operator::new(services::Memory::default())?;
    source.write("nested/example.txt", "source version").await?;
    destination.write("extra.txt", "destination only").await?;
    let engine = SyncEngine::from_storages(
        Arc::new(OperatorStorage::new(source)?),
        Arc::new(OperatorStorage::new(destination.clone())?),
        SyncConfig::default().with_mode(SyncMode::Mirror),
    )?;
    println!("Preview: {:?}", engine.analyze().await?.actions());
    let report = engine.run().await?;
    anyhow::ensure!(report.is_success(), "{report:?}");
    assert_eq!(
        destination.read("nested/example.txt").await?.to_vec(),
        b"source version"
    );
    assert!(!destination.exists("extra.txt").await?);
    assert_eq!(engine.run().await?.files_copied, 0);
    println!("{report:?}");
    Ok(())
}
