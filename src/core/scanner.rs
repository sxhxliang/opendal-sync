use crate::{
    Error, Result,
    storage::{FileInfo, Storage},
};
use globset::{Glob, GlobSet, GlobSetBuilder};
use std::collections::HashMap;

pub(crate) fn exclusions(patterns: &[String]) -> Result<GlobSet> {
    let mut builder = GlobSetBuilder::new();
    for pattern in patterns {
        builder.add(Glob::new(pattern).map_err(|e| Error::InvalidConfig(e.to_string()))?);
    }
    builder
        .build()
        .map_err(|e| Error::InvalidConfig(e.to_string()))
}
pub(crate) async fn scan(
    storage: &dyn Storage,
    exclude: &GlobSet,
) -> Result<HashMap<String, FileInfo>> {
    let mut files = HashMap::new();
    for mut file in storage.list_files(None).await? {
        file.checksum = None;
        let path = file.path.trim_end_matches('/').to_owned();
        if path.is_empty() {
            continue;
        }
        crate::storage::validate_path(&path)?;
        // Matching any ancestor excludes the complete subtree too.
        if std::path::Path::new(&path)
            .ancestors()
            .any(|p| exclude.is_match(p))
        {
            continue;
        }
        if files.insert(path.clone(), file).is_some() {
            return Err(Error::InvalidConfig(format!(
                "duplicate storage path: {path}"
            )));
        }
    }
    Ok(files)
}
