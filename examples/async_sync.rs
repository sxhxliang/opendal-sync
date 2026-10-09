use opendal_sync::{StorageConfig, SyncConfig, SyncEngine};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let paths: Vec<_> = std::env::args_os().skip(1).collect();
    let [source, destination] = paths.as_slice() else {
        return Err("Usage: cargo run --example async_sync -- SOURCE DEST".into());
    };
    let engine = SyncEngine::new(
        StorageConfig::local(source),
        StorageConfig::local(destination),
        SyncConfig::default(),
    )?;
    let report = engine.run().await?;
    println!("{report:?}");
    if !report.is_success() {
        return Err("synchronization failed".into());
    }
    Ok(())
}
