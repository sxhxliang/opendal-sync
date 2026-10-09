//! Read-only preview for any pair of configured endpoints, including conflicts.
mod support;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    anyhow::ensure!(
        std::env::args_os().len() == 1,
        "this example is preview-only; no arguments"
    );
    let pair = support::Pair::from_env()?;
    let engine = pair.engine(support::config()?)?;
    support::print_plan(&engine.analyze().await?);
    Ok(())
}
