/// 高层配置与预执行失败，详见 crate 首页的错误处理指南。
///
/// 执行中的单文件失败记录在 [`crate::SyncReport::errors`] 中。
/// 本枚举非穷尽，外部 match 应包含兜底分支。
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// 数值/glob 不合法，或分析发现无效路径、根重叠、重复条目等。
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
    /// 后端构造、扫描、stat 或哈希失败，保留 anyhow 错误链。
    #[error("storage operation failed: {0}")]
    Storage(#[source] anyhow::Error),
    /// 分析阶段被取消；执行阶段取消使用报告中的 cancelled 标记。
    #[error("synchronization cancelled")]
    Cancelled,
    /// 执行前存在未解决冲突，内含数量，本次未开始写入。
    #[error("{0} unresolved conflicts; no files were changed")]
    Conflicts(usize),
    /// 在 Tokio async 上下文中构造或调用阻塞 API。
    #[error("blocking API cannot run inside Tokio; use SyncEngine or spawn_blocking")]
    BlockingInAsyncContext,
    /// 阻塞引擎的私有 runtime 创建失败。
    #[error("could not create runtime: {0}")]
    Runtime(#[source] std::io::Error),
}
impl From<anyhow::Error> for Error {
    fn from(error: anyhow::Error) -> Self {
        Self::Storage(error)
    }
}
/// Result returned by the high-level API.
pub type Result<T> = std::result::Result<T, Error>;
