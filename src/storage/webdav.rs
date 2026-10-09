use super::{FileInfo, FileMeta, IO_TIMEOUT_SECS, OP_TIMEOUT_SECS, Storage};
use anyhow::Result;
use async_trait::async_trait;
use futures::TryStreamExt;
use opendal::{Operator, layers::TimeoutLayer};
use std::collections::HashSet;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

const MAX_DIR_CACHE_SIZE: usize = 10_000;

/// 通过 `OpenDAL` 访问的远程存储；流式写入缓冲和原子性由后端决定。
///
/// 使用 [`crate::Storage`] 的方法进行 I/O；构造不发送验证请求。
pub struct WebDavStorage {
    operator: Operator,
    /// 已创建的目录缓存（避免重复创建导致 423 Locked）
    created_dirs: Arc<RwLock<HashSet<String>>>,
    name: String,
}

impl WebDavStorage {
    /// 构造 `WebDAV` 后端，不创建远程根目录或发送验证请求。
    ///
    /// endpoint 是基础 URL，root 是其下的同步根（None 使用 `/`），
    /// username/password 为认证信息。避免把 root 重复写入 endpoint。
    /// 调用方法需要 Tokio runtime，网络/权限错误在 I/O 时返回。
    ///
    /// 启用 HTTP 传输 feature 时惰性安装默认传输，应用已安装的传输优先。
    ///
    /// # Errors
    /// 返回无效端点或其他后端配置错误。
    pub fn new(endpoint: &str, username: &str, password: &str, root: Option<&str>) -> Result<Self> {
        use opendal::services::Webdav;
        super::init_http_transport();

        let builder = Webdav::default()
            .endpoint(endpoint)
            .root(root.unwrap_or("/"))
            .username(username)
            .password(password);

        // 添加超时层
        let operator = Operator::new(builder)?.layer(
            TimeoutLayer::default()
                .with_timeout(Duration::from_secs(OP_TIMEOUT_SECS))
                .with_io_timeout(Duration::from_secs(IO_TIMEOUT_SECS)),
        );

        let name = "webdav".to_owned();
        let initial_dirs = HashSet::new();

        Ok(Self {
            operator,
            created_dirs: Arc::new(RwLock::new(initial_dirs)),
            name,
        })
    }

    /// 规范化路径：统一使用正斜杠，去除前导斜杠
    #[inline]
    fn normalize_path(path: &str) -> String {
        path.replace('\\', "/").trim_start_matches('/').to_string()
    }

    /// 确保目录存在（带缓存，避免重复创建）
    async fn ensure_parent_dirs(&self, file_path: &str) -> Result<()> {
        let path = file_path.replace('\\', "/");
        let path = path.trim_start_matches('/');

        if let Some(parent) = std::path::Path::new(path).parent() {
            let parent_str = parent.to_string_lossy().replace('\\', "/");
            if parent_str.is_empty() || parent_str == "." {
                return Ok(());
            }

            // 收集需要创建的目录
            let parts: Vec<&str> = parent_str.split('/').filter(|s| !s.is_empty()).collect();
            let mut dirs_to_create = Vec::new();
            let mut current_path = String::new();

            {
                let cache = self.created_dirs.read().await;
                for part in &parts {
                    current_path.push_str(part);
                    current_path.push('/');
                    if !cache.contains(&current_path) {
                        dirs_to_create.push(current_path.clone());
                    }
                }
            }

            // 只创建不在缓存中的目录
            if !dirs_to_create.is_empty() {
                let mut cache = self.created_dirs.write().await;

                // 缓存大小限制检查，防止内存泄漏
                if cache.len() >= MAX_DIR_CACHE_SIZE {
                    tracing::debug!("目录缓存已满({})，清空重建", MAX_DIR_CACHE_SIZE);
                    cache.clear();
                }

                for dir in dirs_to_create {
                    if !cache.contains(&dir) {
                        // 忽略创建目录的错误（可能已存在或并发创建）
                        self.operator.create_dir(&dir).await?;
                        cache.insert(dir);
                    }
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Storage for WebDavStorage {
    async fn list_files(&self, prefix: Option<&str>) -> Result<Vec<FileInfo>> {
        let mut files = Vec::new();
        let start_path = prefix.unwrap_or("").to_string();

        // 使用栈进行手动递归扫描（某些 WebDAV 服务器不支持 recursive）
        let mut dirs_to_scan = vec![start_path];
        let mut scanned_dirs = std::collections::HashSet::new();

        while let Some(current_dir) = dirs_to_scan.pop() {
            // 避免重复扫描
            if scanned_dirs.contains(&current_dir) {
                continue;
            }
            scanned_dirs.insert(current_dir.clone());

            // 列出当前目录
            let mut lister = self.operator.lister_with(&current_dir).await?;

            while let Some(entry) = lister.try_next().await? {
                let path_str = entry.path().to_string();

                // 跳过根目录
                if path_str.is_empty() || path_str == "/" {
                    continue;
                }

                let relative_path = path_str.trim_start_matches('/');

                let meta = entry.metadata();
                let is_dir = meta.is_dir() || path_str.ends_with('/');

                if is_dir {
                    // 将子目录加入待扫描队列（使用相对路径）
                    let dir_path = relative_path.trim_end_matches('/').to_string() + "/";
                    if !scanned_dirs.contains(&dir_path) && !dir_path.is_empty() && dir_path != "/"
                    {
                        dirs_to_scan.push(dir_path);
                    }
                }

                // 跳过空路径（root 本身）
                let final_path = relative_path.trim_end_matches('/');
                if final_path.is_empty() {
                    continue;
                }

                files.push(FileInfo {
                    path: final_path.to_string(),
                    size: meta.content_length(),
                    modified_time: meta
                        .last_modified()
                        .map_or(0, |t| t.into_inner().as_second()),
                    is_dir,
                    checksum: meta.etag().map(|s| s.trim_matches('"').to_string()),
                });
            }
        }

        tracing::info!(
            "WebDAV 扫描完成: {} 个条目 ({} 个目录已扫描)",
            files.len(),
            scanned_dirs.len()
        );
        Ok(files)
    }

    async fn stat(&self, path: &str) -> Result<Option<FileMeta>> {
        let normalized_path = Self::normalize_path(path);
        match self.operator.stat(&normalized_path).await {
            Ok(meta) => Ok(Some(FileMeta {
                size: meta.content_length(),
                modified_time: meta
                    .last_modified()
                    .map_or(0, |t| t.into_inner().as_second()),
                is_dir: meta.is_dir(),
                etag: meta.etag().map(|s| s.trim_matches('"').to_string()),
            })),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    async fn read(&self, path: &str) -> Result<Vec<u8>> {
        // 规范化路径，移除可能的前缀（如 webdav/Sync/...）
        let normalized_path = Self::normalize_path(path);
        let data = self.operator.read(&normalized_path).await?;
        Ok(data.to_vec())
    }

    async fn read_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        // 规范化路径
        let normalized_path = Self::normalize_path(path);
        let data = self
            .operator
            .read_with(&normalized_path)
            .range(offset..offset + length)
            .await?;
        Ok(data.to_vec())
    }

    async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        // 规范化路径
        let normalized_path = Self::normalize_path(path);

        // 确保父目录存在（使用缓存避免重复创建）
        self.ensure_parent_dirs(&normalized_path).await?;

        self.operator.write(&normalized_path, data).await?;
        Ok(())
    }

    async fn write_stream<'stream>(
        &self,
        path: &str,
        mut stream: Pin<Box<dyn futures::Stream<Item = Result<Vec<u8>>> + Send + 'stream>>,
        total_size: Option<u64>,
    ) -> Result<()> {
        use futures::StreamExt;
        self.ensure_parent_dirs(path).await?;
        let mut writer = self.operator.writer(path).await?;
        let result: Result<()> = async {
            let mut written = 0;
            while let Some(chunk) = stream.next().await {
                let chunk = chunk?;
                written += chunk.len() as u64;
                writer.write(chunk).await?;
            }
            anyhow::ensure!(
                total_size.is_none_or(|size| size == written),
                "incomplete file stream"
            );
            writer.close().await?;
            Ok(())
        }
        .await;
        if result.is_err() {
            let _ = writer.abort().await;
        }
        result
    }

    async fn delete(&self, path: &str) -> Result<()> {
        let normalized_path = Self::normalize_path(path);
        match self.operator.delete(&normalized_path).await {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == opendal::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    async fn create_dir(&self, path: &str) -> Result<()> {
        let normalized_path = Self::normalize_path(path);
        let dir_path = if normalized_path.ends_with('/') {
            normalized_path
        } else {
            format!("{normalized_path}/")
        };
        self.operator.create_dir(&dir_path).await?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}
