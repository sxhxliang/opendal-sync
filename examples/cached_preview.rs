//! Run two previews on one engine. Set `CACHE_TTL_SECS=600` for remote listing reuse.
mod support;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let apply = support::apply_requested()?;
    let pair = support::Pair::from_env()?;
    let engine = pair.engine(support::config()?)?;
    println!("First preview:");
    support::print_plan(&engine.analyze().await?);
    println!("Second preview (remote listing may be cached; content hashes are fresh):");
    support::print_plan(&engine.analyze().await?);
    if apply {
        println!("Invalidating preview cache before execution");
        pair.refresh();
        support::check_report(&engine.run().await?)?;
    }
    Ok(())
}
