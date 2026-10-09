/// 本地目录后端，按文件原子提交写入。
pub mod local;
#[cfg(feature = "services-s3")]
/// S3 兼容对象存储后端，需要 `services-s3` feature（兼容别名 `s3`）。
pub mod s3;
#[cfg(feature = "services-webdav")]
/// `WebDAV` 后端，需要 `services-webdav` feature（兼容别名 `webdav`）。
pub mod webdav;

use anyhow::Result;
use async_trait::async_trait;
use futures::Stream;
use serde::{Deserialize, Serialize};
use std::pin::Pin;

pub use local::LocalStorage;
#[cfg(feature = "services-s3")]
pub use s3::S3Storage;
#[cfg(feature = "services-webdav")]
pub use webdav::WebDavStorage;

// ============ 公共常量 ============

/// 内置远程后端 ``OpenDAL`` 普通操作超时配置（秒），不是整次同步期限。
pub const OP_TIMEOUT_SECS: u64 = 60;
/// 内置远程后端 ``OpenDAL`` I/O 超时配置（秒），不是整次同步期限。
pub const IO_TIMEOUT_SECS: u64 = 300;

/// 文件信息
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInfo {
    /// 以 `/` 分隔的存储根相对路径；目录可带末尾斜杠。
    pub path: String,
    /// 文件大小，单位字节；目录大小不参与比较。
    pub size: u64,
    /// 修改时间，Unix 秒；高层引擎不据此决定复制方向。
    pub modified_time: i64,
    /// 是否为目录；应列出目录以检测文件/目录冲突。
    pub is_dir: bool,
    /// 可选的后端校验字符串；高层引擎丢弃并自行计算 BLAKE3。
    pub checksum: Option<String>,
}

/// 文件元数据（用于快速检查）
#[derive(Debug, Clone)]
pub struct FileMeta {
    /// 文件大小，单位字节。
    pub size: u64,
    /// 修改时间，Unix 秒。
    pub modified_time: i64,
    /// 是否目录；存在的根目录必须返回 true。
    pub is_dir: bool,
    /// 可选后端 `ETag`，高层引擎不据此判断内容相同。
    pub etag: Option<String>,
}

/// 文件块（用于分块传输）
#[derive(Debug, Clone)]
pub struct FileChunk {
    /// 块内容；该辅助类型不由当前引擎或流式接口消费。
    pub data: Vec<u8>,
    /// 块在文件中的字节偏移。
    pub offset: u64,
    /// 调用方记录的块大小；此结构体不自动验证其等于 `data.len()`。
    pub size: usize,
}

/// 可注入高层引擎的异步存储接口，必须支持 Send + Sync。
///
/// 方法使用 `anyhow::Result`，不同于高层的 [`crate::Result`]。
/// 普通文件路径必须是安全、唯一、UTF-8 的相对键，`stat("")` 表示根。
/// 写入实现负责父目录创建；列表不得把权限/网络错误伪装成空目录。
/// 自定义实现及 Arc 注入示例见 crate 首页“自定义存储”。
///
/// 默认 `write_stream` 与 `copy` 完整缓存文件；大文件后端应覆盖它们。
/// 直接调用低层方法不会应用高层的模式、排除、冲突或根重叠校验。
#[async_trait]
pub trait Storage: Send + Sync {
    /// 递归列出根或 prefix 下的完整文件/目录集合。
    ///
    /// None 表示根；返回路径仍相对于整个存储根，不相对于 prefix。
    /// 必须传播任何列举失败，不能返回看似成功的不完整列表。
    /// 根条目可以省略。
    async fn list_files(&self, prefix: Option<&str>) -> Result<Vec<FileInfo>>;

    /// 查询元数据，空字符串表示存储根。
    ///
    /// 仅不存在时返回 None；权限、认证与网络问题返回 Err。
    /// 存在的根应为目录，否则高层分析会拒绝目标端。
    async fn stat(&self, path: &str) -> Result<Option<FileMeta>>;

    /// 读取完整文件到内存。默认 copy 使用它，高层主复制流程使用 `read_range`。
    async fn read(&self, path: &str) -> Result<Vec<u8>>;

    /// 按字节读取半开区间 `[offset, offset + length)`。
    ///
    /// 高层引擎对已扫描文件的有效区间要求恰好 length 字节；短读视为文件变化。
    /// 本方法不表示引擎支持持久化断点续传。
    async fn read_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>>;

    /// 覆盖写入整个文件，并创建必要的父目录；成功应表示数据已完整提交。
    async fn write(&self, path: &str, data: Vec<u8>) -> Result<()>;

    /// 消费输入块并覆盖写入文件。
    ///
    /// 默认实现完整收集到 Vec 后调用 write，忽略 `total_size`；
    /// 自定义大文件后端应覆盖，校验长度并处理流错误、临时对象和取消。
    /// 输入可以借用非 static 数据，必须在返回前消费完成。
    /// 内置本地后端通过同目录临时文件提交，远程原子性由服务端决定。
    async fn write_stream<'stream>(
        &self,
        path: &str,
        mut stream: Pin<Box<dyn Stream<Item = Result<Vec<u8>>> + Send + 'stream>>,
        _total_size: Option<u64>,
    ) -> Result<()> {
        // 默认实现：收集所有数据后写入
        use futures::StreamExt;
        let mut data = Vec::new();
        while let Some(chunk) = stream.next().await {
            data.extend(chunk?);
        }
        self.write(path, data).await
    }

    /// 删除指定文件。引擎不会请求递归删除目录。
    ///
    /// 内置本地实现拒绝目录及根删除；远程细节由后端决定。
    /// 不存在的文件建议视为成功。
    async fn delete(&self, path: &str) -> Result<()>;

    /// 调用 stat，存在时返回 true；不会吞掉查询错误。
    async fn exists(&self, path: &str) -> Result<bool> {
        Ok(self.stat(path).await?.is_some())
    }

    /// 创建目录。引擎不直接调用；write/`write_stream` 必须自行保证父目录可用。
    async fn create_dir(&self, path: &str) -> Result<()>;

    /// 同一后端内部复制；默认 read 后 write，完整文件驻留内存。
    /// 高层引擎当前不调用这个优化入口。
    async fn copy(&self, from: &str, to: &str) -> Result<()> {
        let data = self.read(from).await?;
        self.write(to, data).await
    }

    /// 返回诊断名称，不应包含密码、密钥或其他凭据。
    fn name(&self) -> &str;
}

/// Validate a portable, relative file key before accessing a storage root.
pub(crate) fn validate_path(path: &str) -> Result<()> {
    anyhow::ensure!(
        !path.is_empty() && !path.contains(['\\', ':', '\0']) && !path.starts_with('/'),
        "invalid storage path: {path:?}"
    );
    anyhow::ensure!(
        path.split('/')
            .all(|part| !part.is_empty() && part != "." && part != ".."),
        "invalid storage path: {path:?}"
    );
    Ok(())
}

/// Generic adapter for user-selected `OpenDAL` services.
#[cfg(feature = "opendal")]
mod operator;
#[cfg(feature = "opendal")]
pub use operator::OperatorStorage;

// OpenDAL 0.59 separates HTTP transport from services. Install lazily when an
// adapter is constructed; an application-installed global transport wins.
#[cfg(feature = "opendal")]
fn init_http_transport() {
    #[cfg(any(
        feature = "http-transport-reqwest-rustls",
        feature = "http-transport-reqwest-native-tls",
        feature = "http-transport-reqwest-rustls-no-provider",
    ))]
    opendal_http_transport_reqwest::install_default();
}
