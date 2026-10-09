use super::{FileInfo, FileMeta, Storage};
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::StreamExt;
use std::path::{Component, Path, PathBuf};
use tokio::{
    fs,
    io::{AsyncReadExt, AsyncSeekExt, AsyncWriteExt},
};

/// Local directory storage. Symbolic links and non-UTF-8 paths are rejected.
pub struct LocalStorage {
    base_path: PathBuf,
    name: String,
}

/// Resolve existing ancestors without creating a missing destination.
pub(crate) fn absolute_root(path: &Path) -> Result<PathBuf> {
    let absolute = std::path::absolute(path)?;
    let mut ancestor = absolute.as_path();
    let mut tail = Vec::new();
    while !ancestor.exists() {
        tail.push(
            ancestor
                .file_name()
                .context("invalid local root")?
                .to_owned(),
        );
        ancestor = ancestor.parent().context("invalid local root")?;
    }
    let mut resolved = ancestor.canonicalize()?;
    for part in tail.into_iter().rev() {
        resolved.push(part);
    }
    Ok(resolved)
}
impl LocalStorage {
    /// 打开本地根路径，不创建缺失目录。
    ///
    /// 立即解析路径并规范化已存在的祖先；相对路径相对于构造时的工作目录。
    /// 文件 I/O 需要 Tokio runtime。符号链接、特殊文件、不安全相对键及
    /// 非 UTF-8 文件名在扫描/访问时拒绝；写入使用同目录临时文件提交。
    ///
    /// # Errors
    /// 根已存在但不是目录、路径无法解析或父目录不可访问时返回错误。
    pub fn new(path: impl AsRef<Path>) -> Result<Self> {
        let base_path = absolute_root(path.as_ref())?;
        anyhow::ensure!(
            !base_path.exists() || base_path.is_dir(),
            "local root must be a directory"
        );
        let name = format!("local:{}", base_path.display());
        Ok(Self { base_path, name })
    }
    fn resolve_path(&self, path: &str) -> Result<PathBuf> {
        if !path.is_empty() {
            super::validate_path(path)?;
        }
        let mut resolved = self.base_path.clone();
        // Check the root as well: callers must prevent concurrent path replacement.
        for ancestor in self.base_path.ancestors() {
            match std::fs::symlink_metadata(ancestor) {
                Ok(meta) => anyhow::ensure!(
                    !meta.file_type().is_symlink(),
                    "symbolic link in storage root"
                ),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        for part in Path::new(path).components() {
            anyhow::ensure!(
                matches!(part, Component::Normal(_)),
                "invalid relative path"
            );
            resolved.push(part);
            match std::fs::symlink_metadata(&resolved) {
                Ok(meta) => anyhow::ensure!(
                    !meta.file_type().is_symlink(),
                    "symbolic links are not supported: {}",
                    resolved.display()
                ),
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => return Err(e.into()),
            }
        }
        Ok(resolved)
    }
}
#[async_trait]
impl Storage for LocalStorage {
    async fn list_files(&self, prefix: Option<&str>) -> Result<Vec<FileInfo>> {
        let base = self.resolve_path(prefix.unwrap_or(""))?;
        let root = self.base_path.clone();
        anyhow::ensure!(
            base.try_exists()?,
            "storage directory does not exist: {}",
            base.display()
        );
        tokio::task::spawn_blocking(move || {
            let mut files = Vec::new();
            for entry in walkdir::WalkDir::new(base).follow_links(false) {
                let entry = entry?;
                anyhow::ensure!(
                    !entry.file_type().is_symlink(),
                    "symbolic links are not supported: {}",
                    entry.path().display()
                );
                let relative = entry
                    .path()
                    .strip_prefix(&root)?
                    .to_str()
                    .context("non-UTF-8 file name")?;
                if relative.is_empty() {
                    continue;
                }
                let meta = entry.metadata()?;
                anyhow::ensure!(
                    meta.is_dir() || meta.is_file(),
                    "unsupported file type: {relative}"
                );
                let path = if cfg!(windows) {
                    relative.replace('\\', "/")
                } else {
                    relative.to_owned()
                };
                super::validate_path(&path)?;
                files.push(FileInfo {
                    path,
                    size: meta.len(),
                    modified_time: timestamp(&meta)?,
                    is_dir: meta.is_dir(),
                    checksum: None,
                });
            }
            Ok(files)
        })
        .await?
    }
    async fn stat(&self, path: &str) -> Result<Option<FileMeta>> {
        match fs::metadata(self.resolve_path(path)?).await {
            Ok(meta) => Ok(Some(FileMeta {
                size: meta.len(),
                modified_time: timestamp(&meta)?,
                is_dir: meta.is_dir(),
                etag: None,
            })),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }
    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        Ok(fs::read(self.resolve_path(path)?).await?)
    }
    async fn read_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let mut file = fs::File::open(self.resolve_path(path)?).await?;
        file.seek(std::io::SeekFrom::Start(offset)).await?;
        let mut bytes = Vec::new();
        file.take(length).read_to_end(&mut bytes).await?;
        Ok(bytes)
    }
    async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        let size = data.len() as u64;
        self.write_stream(
            path,
            Box::pin(futures::stream::once(async { Ok(data) })),
            Some(size),
        )
        .await
    }
    async fn write_stream<'stream>(
        &self,
        path: &str,
        mut stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<Vec<u8>>> + Send + 'stream>,
        >,
        total_size: Option<u64>,
    ) -> Result<()> {
        let destination = self.resolve_path(path)?;
        let parent = destination.parent().context("missing parent directory")?;
        fs::create_dir_all(parent).await?;
        // Unique temporary files avoid collisions between equal stems and concurrent copies.
        let temp = tempfile::NamedTempFile::new_in(parent)?;
        let (file, temp_path) = temp.into_parts();
        let mut file = fs::File::from_std(file);
        let mut written = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk?;
            file.write_all(&chunk).await?;
            written += chunk.len() as u64;
        }
        anyhow::ensure!(
            total_size.is_none_or(|size| size == written),
            "incomplete file stream"
        );
        file.flush().await?;
        drop(file);
        // TempPath removes uncommitted files on errors and cancellation.
        temp_path.persist(&destination)?;
        Ok(())
    }
    async fn delete(&self, path: &str) -> Result<()> {
        anyhow::ensure!(!path.is_empty(), "cannot delete storage root");
        match fs::remove_file(self.resolve_path(path)?).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }
    async fn create_dir(&self, path: &str) -> Result<()> {
        fs::create_dir_all(self.resolve_path(path)?).await?;
        Ok(())
    }
    fn name(&self) -> &str {
        &self.name
    }
}
fn timestamp(meta: &std::fs::Metadata) -> Result<i64> {
    let duration = meta
        .modified()?
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    Ok(i64::try_from(duration.as_secs())?)
}
