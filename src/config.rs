use crate::{
    Error, Result,
    storage::{LocalStorage, Storage},
};
use std::{path::PathBuf, sync::Arc};

/// 存储端点配置，按值交给 [`crate::SyncEngine::new`]。
///
/// 默认仅编译本地变体；远程变体需要相应 feature。
/// 远程 Debug 输出隐藏字段，但凭据本身是普通内存字符串。
/// 构造配置不会访问网络或创建目录，也不会自动读取环境变量。
#[derive(Clone)]
#[non_exhaustive]
pub enum StorageConfig {
    /// 本地目录；源必须存在，目标可在首次文件写入时创建。
    Local {
        /// 同步根路径；相对路径在分析/运行时按当前工作目录解析。
        path: PathBuf,
    },
    #[cfg(feature = "services-s3")]
    /// S3 或兼容对象存储，需要 `services-s3` feature（兼容别名 `s3`）。
    S3 {
        /// 已有存储桶名称；不会自动创建桶。
        bucket: String,
        /// 签名使用的区域。
        region: String,
        /// 显式访问密钥 ID。
        access_key: String,
        /// 显式访问密钥。
        secret_key: String,
        /// 服务端点 URL；S3 的 None 使用后端默认端点。
        endpoint: Option<String>,
        /// 桶内同步根前缀；None 表示桶根。
        prefix: Option<String>,
    },
    #[cfg(feature = "services-webdav")]
    /// `WebDAV` 服务，需要 `services-webdav` feature（兼容别名 `webdav`）。
    WebDav {
        /// 必填的 `WebDAV` 基础服务 URL；root 单独描述其下的目录。
        endpoint: String,
        /// `WebDAV` 用户名。
        username: String,
        /// `WebDAV` 密码。
        password: String,
        /// 基础端点下的同步根目录；None 使用 `/`。
        root: Option<String>,
    },
}
impl std::fmt::Debug for StorageConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Local { path } => f.debug_struct("Local").field("path", path).finish(),
            #[cfg(feature = "services-s3")]
            Self::S3 { .. } => f.write_str("S3 { .. }"),
            #[cfg(feature = "services-webdav")]
            Self::WebDav { .. } => f.write_str("WebDav { .. }"),
        }
    }
}
impl StorageConfig {
    /// 创建本地端点配置，不访问文件系统。
    ///
    /// 路径在分析/执行时解析；建议长期配置使用绝对路径。
    pub fn local(path: impl Into<PathBuf>) -> Self {
        Self::Local { path: path.into() }
    }

    pub(crate) fn connect(&self) -> Result<Arc<dyn Storage>> {
        Ok(match self {
            Self::Local { path } => Arc::new(LocalStorage::new(path)?),
            #[cfg(feature = "services-s3")]
            Self::S3 {
                bucket,
                region,
                access_key,
                secret_key,
                endpoint,
                prefix,
            } => Arc::new(crate::storage::S3Storage::new(
                bucket,
                region,
                access_key,
                secret_key,
                endpoint.as_deref(),
                prefix.as_deref(),
            )?),
            #[cfg(feature = "services-webdav")]
            Self::WebDav {
                endpoint,
                username,
                password,
                root,
            } => Arc::new(crate::storage::WebDavStorage::new(
                endpoint,
                username,
                password,
                root.as_deref(),
            )?),
        })
    }
}

/// How files present on only one side are treated.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum SyncMode {
    /// Copy source changes, preserving destination-only files.
    #[default]
    Backup,
    /// Make destination match source, deleting destination-only files.
    Mirror,
    /// Copy unique files both ways; differing shared files are conflicts.
    /// Without historical state, deletions are not propagated.
    Bidirectional,
}

/// Resolution for differing shared files during bidirectional synchronization.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum ConflictPolicy {
    /// Reject the entire run before making changes.
    #[default]
    Fail,
    /// 共有文件内容不同时，以引擎源端为准覆盖目标端。
    PreferSource,
    /// 共有文件内容不同时，以引擎目标端为准覆盖源端。
    PreferDestination,
    /// 保留冲突两端内容并计为 Skip；不使报告变为失败。
    Skip,
}

/// Synchronization settings. Content hashes are always used for equal-size files.
#[derive(Debug, Clone)]
pub struct SyncConfig {
    /// 同步模式，默认为 [`SyncMode::Backup`]。
    pub mode: SyncMode,
    /// 双向共有文件冲突策略，默认 [`ConflictPolicy::Fail`]；对其他模式无效。
    pub conflict_policy: ConflictPolicy,
    /// 大小写敏感的 glob，默认空；相对根和所有祖先匹配，两端共同应用。
    /// 不支持 gitignore 的否定规则；排除条目不会被镜像删除。
    pub exclude_patterns: Vec<String>,
    /// 执行阶段并发复制/删除操作上限，默认 4；必须大于零。
    /// 不控制分析阶段的扫描或哈希并发。
    pub max_concurrent_transfers: usize,
    /// 哈希和复制读取块的最大字节数，默认 8 `MiB`；必须大于零。
    /// 不限制后端、文件列表或默认自定义流式写入的总缓冲内存。
    pub chunk_size: usize,
}
impl Default for SyncConfig {
    fn default() -> Self {
        Self {
            mode: SyncMode::Backup,
            conflict_policy: ConflictPolicy::Fail,
            exclude_patterns: Vec::new(),
            max_concurrent_transfers: 4,
            chunk_size: 8 * 1024 * 1024,
        }
    }
}
impl SyncConfig {
    /// 替换同步模式并返回配置，可链式调用。
    #[must_use]
    pub fn with_mode(mut self, mode: SyncMode) -> Self {
        self.mode = mode;
        self
    }
    /// 替换双向冲突策略并返回配置。
    #[must_use]
    pub fn with_conflict_policy(mut self, policy: ConflictPolicy) -> Self {
        self.conflict_policy = policy;
        self
    }
    /// 追加一条 glob 规则并返回配置，不替换已有规则。
    ///
    /// 语法在构造引擎时验证，非法规则返回 [`Error::InvalidConfig`]。
    #[must_use]
    pub fn exclude(mut self, pattern: impl Into<String>) -> Self {
        self.exclude_patterns.push(pattern.into());
        self
    }
    pub(crate) fn validate(&self) -> Result<()> {
        if self.max_concurrent_transfers == 0 || self.chunk_size == 0 {
            return Err(Error::InvalidConfig(
                "concurrency and chunk size must be positive".into(),
            ));
        }
        Ok(())
    }
}
