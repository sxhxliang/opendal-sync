//! Self-contained local demo. Uses temporary directories and verifies all three modes.
use opendal_sync::{BlockingSyncEngine, StorageConfig, SyncConfig, SyncMode};

fn main() -> anyhow::Result<()> {
    for mode in [SyncMode::Backup, SyncMode::Mirror, SyncMode::Bidirectional] {
        let root = tempfile::tempdir()?;
        let source = root.path().join("source");
        let destination = root.path().join("destination");
        std::fs::create_dir(&source)?;
        std::fs::create_dir(&destination)?;
        std::fs::write(source.join("source-only.txt"), "source")?;
        std::fs::write(destination.join("destination-only.txt"), "destination")?;
        std::fs::write(source.join("common.txt"), "new")?;
        std::fs::write(
            destination.join("common.txt"),
            if mode == SyncMode::Bidirectional {
                "new"
            } else {
                "old"
            },
        )?;
        let engine = BlockingSyncEngine::new(
            StorageConfig::local(&source),
            StorageConfig::local(&destination),
            SyncConfig::default().with_mode(mode),
        )?;
        println!("{mode:?} preview: {:?}", engine.analyze()?.actions());
        let report = engine.run()?;
        anyhow::ensure!(report.is_success(), "{report:?}");
        assert!(destination.join("source-only.txt").exists());
        assert_eq!(std::fs::read(destination.join("common.txt"))?, b"new");
        assert_eq!(
            destination.join("destination-only.txt").exists(),
            mode != SyncMode::Mirror
        );
        assert_eq!(
            source.join("destination-only.txt").exists(),
            mode == SyncMode::Bidirectional
        );
        assert_eq!(engine.run()?.files_copied, 0);
        println!("{mode:?}: {report:?}; second run copied no files");
    }
    Ok(())
}
