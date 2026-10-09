//! Inspect each conflict and choose source, destination, both or skip before syncing.
mod support;
use std::io::{self, Write};
use opendal_sync::{ConflictPolicy, SyncAction, SyncMode};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let apply = support::apply_requested()?;
    let pair = support::Pair::from_env()?;
    let config = support::config()?.with_mode(SyncMode::Bidirectional);
    let engine = pair.engine(config.clone())?;
    let plan = engine.analyze().await?;
    support::print_plan(&plan);
    if !apply {
        println!("Use --apply to choose a resolution for each conflict.");
        return Ok(());
    }
    pair.refresh();
    // Refresh again before prompting, since preview listings may have been cached.
    let plan = engine.analyze().await?;
    for action in plan.actions() {
        let SyncAction::Conflict { path, .. } = action else {
            continue;
        };
        loop {
            print!("{path}: [s]ource / [d]estination / [b]oth / [k]skip > ");
            io::stdout().flush()?;
            let mut answer = String::new();
            anyhow::ensure!(
                io::stdin().read_line(&mut answer)? != 0,
                "input ended; stopping without further changes"
            );
            match answer.trim() {
                "s" => {
                    support::copy_file(
                        pair.source.as_ref(),
                        pair.destination.as_ref(),
                        path,
                        path,
                        config.chunk_size,
                    )
                    .await?;
                }
                "d" => {
                    support::copy_file(
                        pair.destination.as_ref(),
                        pair.source.as_ref(),
                        path,
                        path,
                        config.chunk_size,
                    )
                    .await?;
                }
                "b" => println!(
                    "Preserved destination version as {}",
                    support::keep_both(&pair, path, config.chunk_size).await?
                ),
                "k" => {}
                _ => {
                    eprintln!("Enter s, d, b or k");
                    continue;
                }
            }
            break;
        }
    }
    pair.refresh();
    // Approved conflicts now have equal contents; skipped/new conflicts remain untouched.
    let engine = pair.engine(config.with_conflict_policy(ConflictPolicy::Skip))?;
    support::check_report(&engine.run().await?)?;
    Ok(())
}
