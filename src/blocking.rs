use crate::{
    CancellationToken, Error, Result, StorageConfig, SyncConfig, SyncEngine, SyncPlan,
    SyncProgress, SyncReport,
};

/// 与 [`SyncEngine`] 共享同步逻辑的阻塞接口，持有一个可复用的私有 runtime。
///
/// 在普通线程使用，或在 Tokio `spawn_blocking` 闭包内构造并调用。
/// 不在进程中安装全局 runtime；丢弃引擎会关闭其私有 runtime。
pub struct BlockingSyncEngine {
    engine: SyncEngine,
    runtime: Option<tokio::runtime::Runtime>,
}
impl BlockingSyncEngine {
    /// 创建端点配置引擎及其私有 Tokio runtime，不创建同步目录。
    ///
    /// # Errors
    /// 返回数值/模式规则验证错误、[`Error::BlockingInAsyncContext`] 或 [`Error::Runtime`]。
    /// 目录检查与网络错误在分析/执行时返回。
    pub fn new(
        source: StorageConfig,
        destination: StorageConfig,
        config: SyncConfig,
    ) -> Result<Self> {
        Self::from_engine(SyncEngine::new(source, destination, config)?)
    }
    /// 消费异步引擎，包装为持有私有 runtime 的阻塞引擎。
    ///
    /// 适合将 [`SyncEngine::from_storages`] 的自定义后端用于阻塞调用。
    ///
    /// # Errors
    /// 返回 [`Error::BlockingInAsyncContext`] 或 [`Error::Runtime`]。
    pub fn from_engine(engine: SyncEngine) -> Result<Self> {
        check_context()?;
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()
            .map_err(Error::Runtime)?;
        Ok(Self {
            engine,
            runtime: Some(runtime),
        })
    }
    /// 阻塞等待只读预览；未解决冲突保留在计划中。
    ///
    /// 参数和结果语义与 [`SyncEngine::analyze`] 相同。
    ///
    /// # Errors
    /// 返回对应异步方法的错误，并额外拒绝 Tokio async 上下文，
    /// 此时返回 [`Error::BlockingInAsyncContext`]。
    pub fn analyze(&self) -> Result<SyncPlan> {
        self.block_on(self.engine.analyze())
    }
    /// 可取消的阻塞分析；由其他线程取消传入 token。
    ///
    /// 参数和结果语义与 [`SyncEngine::analyze_with_cancellation`] 相同。
    ///
    /// # Errors
    /// 返回对应异步方法的错误，并额外拒绝 Tokio async 上下文，
    /// 此时返回 [`Error::BlockingInAsyncContext`]。
    pub fn analyze_with_cancellation(&self, cancel: &CancellationToken) -> Result<SyncPlan> {
        self.block_on(self.engine.analyze_with_cancellation(cancel))
    }
    /// 阻塞等待重新分析及执行，须检查返回报告的 `is_success`()。
    ///
    /// 参数和结果语义与 [`SyncEngine::run`] 相同。
    ///
    /// # Errors
    /// 返回对应异步方法的错误，并额外拒绝 Tokio async 上下文，
    /// 此时返回 [`Error::BlockingInAsyncContext`]。
    pub fn run(&self) -> Result<SyncReport> {
        self.block_on(self.engine.run())
    }
    /// 阻塞执行并同步调用回调，支持其他线程取消 token。
    ///
    /// 参数和结果语义与 [`SyncEngine::run_with_progress`] 相同。
    ///
    /// # Errors
    /// 返回对应异步方法的错误，并额外拒绝 Tokio async 上下文，
    /// 此时返回 [`Error::BlockingInAsyncContext`]。
    ///
    /// # Panics
    /// 回调 panic 会传播。
    pub fn run_with_progress<F>(
        &self,
        cancel: &CancellationToken,
        progress: F,
    ) -> Result<SyncReport>
    where
        F: FnMut(&SyncProgress) + Send,
    {
        self.block_on(self.engine.run_with_progress(cancel, progress))
    }
    fn block_on<T>(&self, future: impl std::future::Future<Output = Result<T>>) -> Result<T> {
        check_context()?;
        self.runtime
            .as_ref()
            .expect("runtime exists until drop")
            .block_on(future)
    }
}
impl Drop for BlockingSyncEngine {
    fn drop(&mut self) {
        // Safe even if ownership was moved into an async context before dropping.
        if let Some(runtime) = self.runtime.take() {
            runtime.shutdown_background();
        }
    }
}
fn check_context() -> Result<()> {
    if tokio::runtime::Handle::try_current().is_ok() {
        Err(Error::BlockingInAsyncContext)
    } else {
        Ok(())
    }
}
