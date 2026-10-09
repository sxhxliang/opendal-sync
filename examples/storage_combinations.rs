//! Any local/S3/WebDAV pairing. Defaults to preview; --apply performs synchronization.
mod support;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let apply = support::apply_requested()?;
    let pair = support::Pair::from_env()?;
    let engine = pair.engine(support::config()?)?;
    support::print_plan(&engine.analyze().await?);
    if apply {
        pair.refresh(); // Never execute mirror using a cached preview listing.
        support::check_report(&engine.run().await?)?;
    } else {
        println!("Preview only. Pass --apply to synchronize.");
    }
    Ok(())
}
