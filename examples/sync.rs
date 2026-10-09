use std::{path::PathBuf, process::ExitCode};
use opendal_sync::{BlockingSyncEngine, CancellationToken, StorageConfig, SyncConfig, SyncMode};

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("error: {error}");
            ExitCode::FAILURE
        }
    }
}
fn run() -> Result<(), Box<dyn std::error::Error>> {
    let mut paths = Vec::new();
    let mut config = SyncConfig::default();
    let mut dry_run = false;
    for arg in std::env::args_os().skip(1) {
        match arg.to_str() {
            Some("--help" | "-h") => {
                println!(
                    "Usage: cargo run --example sync -- SOURCE DEST [--dry-run] [--mirror | --bidirectional]\nDefault: backup (preserve destination-only files).\n--mirror deletes destination-only files.\n--bidirectional refuses differing shared files."
                );
                return Ok(());
            }
            Some("--dry-run") => dry_run = true,
            Some("--mirror") if config.mode == SyncMode::Backup => config.mode = SyncMode::Mirror,
            Some("--bidirectional") if config.mode == SyncMode::Backup => {
                config.mode = SyncMode::Bidirectional;
            }
            Some(value) if value.starts_with('-') => {
                return Err(format!("unknown or conflicting option: {value}").into());
            }
            _ => paths.push(PathBuf::from(arg)),
        }
    }
    let [source, destination] = paths.as_slice() else {
        return Err("expected SOURCE and DEST; use --help".into());
    };
    let engine = BlockingSyncEngine::new(
        StorageConfig::local(source),
        StorageConfig::local(destination),
        config,
    )?;
    if dry_run {
        let plan = engine.analyze()?;
        for action in plan.actions() {
            println!("{action:?}");
        }
        println!("Summary: {:?}", plan.summary());
        return Ok(());
    }
    let report = engine.run_with_progress(&CancellationToken::new(), |p| {
        if let Some(path) = &p.current_path {
            eprintln!("[{}/{}] {path}", p.operations_completed, p.operations_total);
        }
    })?;
    println!(
        "copied={} deleted={} skipped={} failed={} bytes={}",
        report.files_copied,
        report.files_deleted,
        report.files_skipped,
        report.files_failed,
        report.bytes_transferred
    );
    for failure in &report.errors {
        eprintln!("{}: {}", failure.path, failure.message);
    }
    if !report.is_success() {
        return Err("synchronization did not complete successfully".into());
    }
    Ok(())
}
