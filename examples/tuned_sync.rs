//! Configure `MAX_CONCURRENT=1..128`, `CHUNK_MIB` and `STREAM_THRESHOLD_MIB`.
mod support;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let apply = support::apply_requested()?;
    let pair = support::Pair::from_env()?;
    let config = support::config()?;
    println!("Config: {config:?}");
    let engine = pair.engine(config)?;
    support::print_plan(&engine.analyze().await?);
    if apply {
        pair.refresh();
        let report = engine
            .run_with_progress(&opendal_sync::CancellationToken::new(), |p| {
                println!(
                    "{}/{}: {:?}, committed bytes={}",
                    p.operations_completed, p.operations_total, p.current_path, p.bytes_transferred
                );
            })
            .await?;
        support::check_report(&report)?;
        pair.refresh();
        println!("Incremental preview after synchronization:");
        support::print_plan(&engine.analyze().await?);
    }
    Ok(())
}
