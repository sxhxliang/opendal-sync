//! Local ↔ Cloudflare R2, using the S3 API with signing region `auto`.
//! Run with --help for arguments. Defaults to a read-only preview.
use anyhow::{Context, Result};
use opendal_sync::{StorageConfig, SyncConfig, SyncEngine, SyncMode};

const HELP: &str = "Usage: cargo run --features s3 --example cloudflare_r2 -- \
<upload|download> LOCAL_PATH [--apply] [--mirror|--bidirectional]

Default: backup mode, read-only preview. --apply performs synchronization.
Mirror deletes destination-only files. Bidirectional conflicts stop execution.

Required environment: R2_BUCKET, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY
Endpoint: R2_ACCOUNT_ID, or explicit R2_ENDPOINT (for jurisdiction-specific buckets)
Optional: R2_PREFIX (bucket subdirectory; default is the bucket root)
Region is always auto. Use R2 S3 credentials, not a Cloudflare bearer API token.";

fn required(name: &str) -> Result<String> {
    let value = std::env::var(name).with_context(|| format!("set {name}"))?;
    anyhow::ensure!(!value.trim().is_empty(), "{name} must not be empty");
    Ok(value)
}
fn optional(name: &str) -> Result<Option<String>> {
    match std::env::var(name) {
        Ok(value) if !value.trim().is_empty() => Ok(Some(value)),
        Ok(_) | Err(std::env::VarError::NotPresent) => Ok(None),
        Err(error) => Err(error.into()),
    }
}
fn r2_config() -> Result<StorageConfig> {
    let endpoint = if let Some(endpoint) = optional("R2_ENDPOINT")? {
        endpoint
    } else {
        let account = required("R2_ACCOUNT_ID")?;
        anyhow::ensure!(
            account.len() == 32 && account.bytes().all(|c| c.is_ascii_hexdigit()),
            "R2_ACCOUNT_ID must be a 32-character hexadecimal account ID"
        );
        format!("https://{account}.r2.cloudflarestorage.com")
    };
    Ok(StorageConfig::S3 {
        bucket: required("R2_BUCKET")?,
        region: "auto".into(),
        access_key: required("R2_ACCESS_KEY_ID")?,
        secret_key: required("R2_SECRET_ACCESS_KEY")?,
        endpoint: Some(endpoint),
        prefix: optional("R2_PREFIX")?,
    })
}

#[tokio::main]
async fn main() -> Result<()> {
    let args: Vec<_> = std::env::args_os().skip(1).collect();
    if args.iter().any(|arg| arg == "--help" || arg == "-h") {
        println!("{HELP}");
        return Ok(());
    }
    anyhow::ensure!(args.len() >= 2, "{HELP}");
    let upload = match args[0].to_str() {
        Some("upload") => true,
        Some("download") => false,
        _ => anyhow::bail!("expected upload or download\n{HELP}"),
    };
    let mut apply = false;
    let mut mode = SyncMode::Backup;
    for arg in &args[2..] {
        match arg.to_str() {
            Some("--apply") if !apply => apply = true,
            Some("--mirror") if mode == SyncMode::Backup => mode = SyncMode::Mirror,
            Some("--bidirectional") if mode == SyncMode::Backup => mode = SyncMode::Bidirectional,
            _ => anyhow::bail!(
                "unknown, duplicate or conflicting option: {}",
                arg.display()
            ),
        }
    }
    let local = StorageConfig::local(std::path::PathBuf::from(&args[1]));
    let r2 = r2_config()?;
    let (source, destination) = if upload { (local, r2) } else { (r2, local) };
    let engine = SyncEngine::new(source, destination, SyncConfig::default().with_mode(mode))?;
    println!(
        "Direction: {}, mode: {mode:?}",
        if upload { "local -> R2" } else { "R2 -> local" }
    );
    let plan = engine.analyze().await?;
    for action in plan.actions() {
        println!("{action:?}");
    }
    println!("Summary: {:?}", plan.summary());
    if !apply {
        println!("Preview only. Pass --apply to synchronize.");
        return Ok(());
    }
    let report = engine.run().await?; // Fresh scan, no cached preview decisions.
    println!("{report:?}");
    anyhow::ensure!(
        report.is_success(),
        "synchronization cancelled or partially failed"
    );
    Ok(())
}
