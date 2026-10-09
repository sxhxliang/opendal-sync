use super::scanner;
use crate::{
    CancellationToken, ConflictPolicy, Error, Result, StorageConfig, SyncConfig,
    core::comparator::{ActionSummary, FileComparator, SyncAction},
    storage::Storage,
};
use futures::{StreamExt, stream};
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

/// Read-only preview. A run scans again, so a stale preview cannot be executed.
#[derive(Debug, Clone)]
pub struct SyncPlan {
    actions: Vec<SyncAction>,
}
impl SyncPlan {
    /// 借用计划动作；当前实现按相对路径排序。
    ///
    /// 计划不可直接执行；[`SyncEngine::run`] 会重新分析。
    #[must_use]
    pub fn actions(&self) -> &[SyncAction] {
        &self.actions
    }
    /// 根据当前计划计算新的统计值，包括跳过和冲突。
    ///
    /// 正向与反向复制分别计数，详见 [`ActionSummary`]。
    #[must_use]
    pub fn summary(&self) -> ActionSummary {
        FileComparator::summarize_actions(&self.actions)
    }
}
/// A file operation that failed. The remaining copies are still attempted.
#[derive(Debug, Clone)]
pub struct SyncFailure {
    /// 失败操作的相对路径。
    pub path: String,
    /// 底层错误的诊断文本，不是稳定的机器错误码。
    pub message: String,
}
/// Outcome of execution; partial failures are represented here, not discarded.
#[derive(Debug, Clone, Default)]
pub struct SyncReport {
    /// 成功复制的文件数，包含两个方向。
    pub files_copied: usize,
    /// 成功删除的文件操作数。
    pub files_deleted: usize,
    /// 计划中 Skip 的数量，不含取消/失败后未尝试的操作。
    pub files_skipped: usize,
    /// 失败的复制或删除操作数，与 `errors.len()` 对应。
    pub files_failed: usize,
    /// Bytes in successfully copied files, excluding hashing and failed transfers.
    pub bytes_transferred: u64,
    /// 运行总耗时，包含分析与进度回调。
    pub elapsed: Duration,
    /// 执行阶段观察到取消；已完成操作不回滚。
    pub cancelled: bool,
    /// 失败操作详情，按完成顺序记录。
    pub errors: Vec<SyncFailure>,
}
impl SyncReport {
    /// 当且仅当未取消且无执行失败时返回 true。
    ///
    /// 按策略跳过的冲突不算失败；成功不保证两端完全相同。
    #[must_use]
    pub fn is_success(&self) -> bool {
        !self.cancelled && self.files_failed == 0
    }
}
/// Progress emitted before execution and after each completed operation.
#[derive(Debug, Clone)]
pub struct SyncProgress {
    /// 初始事件为 None，其后为刚完成操作的路径，不表示实时传输路径。
    pub current_path: Option<String>,
    /// 计划中的双向复制数加删除数，不含跳过/冲突，可为零。
    pub operations_total: usize,
    /// 已返回结果的操作数，包含成功和失败。
    pub operations_completed: usize,
    /// 已成功复制的完整文件大小之和，不含哈希或失败传输字节。
    pub bytes_transferred: u64,
    /// 截至本事件的失败操作数。
    pub files_failed: usize,
}

/// Reusable async engine. No filesystem changes occur during construction or analysis.
/// Requires a Tokio runtime. Independent runs have independent cancellation tokens.
#[derive(Clone)]
pub struct SyncEngine {
    source: Endpoint,
    destination: Endpoint,
    config: SyncConfig,
    exclude: globset::GlobSet,
}
#[derive(Clone)]
enum Endpoint {
    Config(StorageConfig),
    Custom(Arc<dyn Storage>),
}
impl Endpoint {
    fn connect(&self) -> Result<Arc<dyn Storage>> {
        match self {
            Self::Config(config) => config.connect(),
            Self::Custom(storage) => Ok(storage.clone()),
        }
    }
}
impl SyncEngine {
    /// 从两个端点配置创建可复用引擎。
    ///
    /// 只验证数值配置与 glob，不检查目录、网络或凭据。
    /// 本地源存在性、路径重叠和文件/目录冲突在分析阶段验证。
    ///
    /// # Errors
    /// 并发数或块大小为零、glob 语法错误时返回 [`Error::InvalidConfig`]。
    pub fn new(
        source: StorageConfig,
        destination: StorageConfig,
        config: SyncConfig,
    ) -> Result<Self> {
        Self::build(
            Endpoint::Config(source),
            Endpoint::Config(destination),
            config,
        )
    }
    /// 注入共享的自定义后端。
    ///
    /// 调用方负责确保两个根目录互不重叠。只检查 Arc 指针不相同，
    /// 不解析自定义后端的物理根目录；克隆引擎会共享这些 Arc。
    ///
    /// # Errors
    /// 指针相同、数值配置或 glob 无效时返回 [`Error::InvalidConfig`]。
    pub fn from_storages(
        source: Arc<dyn Storage>,
        destination: Arc<dyn Storage>,
        config: SyncConfig,
    ) -> Result<Self> {
        if Arc::ptr_eq(&source, &destination) {
            return Err(Error::InvalidConfig("storage endpoints must differ".into()));
        }
        Self::build(
            Endpoint::Custom(source),
            Endpoint::Custom(destination),
            config,
        )
    }
    fn build(source: Endpoint, destination: Endpoint, config: SyncConfig) -> Result<Self> {
        config.validate()?;
        let exclude = scanner::exclusions(&config.exclude_patterns)?;
        Ok(Self {
            source,
            destination,
            config,
            exclude,
        })
    }
    fn validate_roots(&self) -> Result<()> {
        if let Endpoint::Config(StorageConfig::Local { path }) = &self.source
            && !path.is_dir()
        {
            return Err(Error::InvalidConfig(format!(
                "source is not an existing directory: {}",
                path.display()
            )));
        }
        if let (
            Endpoint::Config(StorageConfig::Local { path: source }),
            Endpoint::Config(StorageConfig::Local { path: dest }),
        ) = (&self.source, &self.destination)
        {
            let source = crate::storage::local::absolute_root(source)?;
            let dest = crate::storage::local::absolute_root(dest)?;
            if source.starts_with(&dest) || dest.starts_with(&source) {
                return Err(Error::InvalidConfig("local roots must not overlap".into()));
            }
        }
        Ok(())
    }
    /// 只读预览差异，不创建目标目录，也不复制或删除文件。
    ///
    /// 会列举两端并读取同大小共有文件计算 BLAKE3。
    /// 未解决冲突保留在计划中，不返回 [`Error::Conflicts`]。
    /// 冲突偏好和 Skip 策略已应用到返回动作。
    ///
    /// # Errors
    /// 返回路径/配置检查、连接、扫描或哈希错误。
    ///
    /// # Panics
    /// 调用产生的 future 必须在 Tokio runtime 中轮询，否则底层异步 I/O 可能 panic。
    pub async fn analyze(&self) -> Result<SyncPlan> {
        self.analyze_with_cancellation(&CancellationToken::new())
            .await
    }
    /// 带协作取消的 [`Self::analyze`]。
    ///
    /// 预取消或分析期间取消返回 [`Error::Cancelled`]，不返回部分计划。
    /// 令牌不会被重置；新的操作应使用未取消令牌。
    ///
    /// # Errors
    /// 除 analyze 的错误外，可返回 [`Error::Cancelled`]。
    ///
    /// # Panics
    /// 需要 Tokio runtime，参见 [`Self::analyze`]。
    pub async fn analyze_with_cancellation(&self, cancel: &CancellationToken) -> Result<SyncPlan> {
        tokio::select! {
            biased;
            () = cancel.cancelled() => Err(Error::Cancelled),
            result = async {
                self.validate_roots()?;
                let source = self.source.connect()?;
                let dest = self.destination.connect()?;
                self.plan(source.as_ref(), dest.as_ref()).await
            } => result,
        }
    }
    async fn plan(&self, source: &dyn Storage, dest: &dyn Storage) -> Result<SyncPlan> {
        let (mut source_files, mut dest_files) =
            tokio::try_join!(scanner::scan(source, &self.exclude), async {
                match dest.stat("").await? {
                    None => Ok(std::collections::HashMap::new()),
                    Some(meta) if meta.is_dir => scanner::scan(dest, &self.exclude).await,
                    Some(_) => Err(Error::InvalidConfig(
                        "destination root is not a directory".into(),
                    )),
                }
            })?;
        // Refuse file/directory replacement rather than recursively deleting contents.
        for (path, src) in &source_files {
            if let Some(dst) = dest_files.get(path)
                && src.is_dir != dst.is_dir
            {
                return Err(Error::InvalidConfig(format!(
                    "file/directory collision: {path}"
                )));
            }
        }
        // Backend ETags have incompatible meanings. Compute our own BLAKE3 hashes.
        for (path, src) in &mut source_files {
            if src.is_dir {
                continue;
            }
            if let Some(dst) = dest_files.get_mut(path)
                && src.size == dst.size
            {
                let (a, b) = tokio::try_join!(
                    hash(source, path, src.size, self.config.chunk_size),
                    hash(dest, path, dst.size, self.config.chunk_size)
                )?;
                src.checksum = Some(a);
                dst.checksum = Some(b);
            }
        }
        let mut actions =
            FileComparator::compare_trees(&source_files, &dest_files, self.config.mode);
        for action in &mut actions {
            if let SyncAction::Conflict {
                path,
                source_info,
                dest_info,
                ..
            } = action
            {
                let reverse = self.config.conflict_policy == ConflictPolicy::PreferDestination;
                let info = if reverse {
                    dest_info.as_ref()
                } else {
                    source_info.as_ref()
                };
                *action = match self.config.conflict_policy {
                    ConflictPolicy::Fail => continue,
                    ConflictPolicy::Skip => SyncAction::Skip { path: path.clone() },
                    _ => SyncAction::Copy {
                        source_path: path.clone(),
                        dest_path: path.clone(),
                        size: info.map_or(0, |i| i.size),
                        reverse,
                    },
                };
            }
        }
        Ok(SyncPlan { actions })
    }
    /// 重新分析后执行，不安装回调或接受外部取消令牌。
    ///
    /// 先完成全部复制，再执行删除；复制失败会阻止整个删除阶段。
    /// 执行中的单文件错误存放在报告中，请检查 [`SyncReport::is_success`]。
    ///
    /// # Errors
    /// 预执行阶段的配置、存储和扫描错误直接返回；存在未解决冲突时
    /// 返回 [`Error::Conflicts`]，本次未开始写入。
    ///
    /// # Panics
    /// 需要 Tokio runtime，参见 [`Self::analyze`]。
    pub async fn run(&self) -> Result<SyncReport> {
        self.run_with_progress(&CancellationToken::new(), |_| {})
            .await
    }
    /// 重新分析后执行，接受进度回调及取消令牌。
    ///
    /// 分析成功且冲突检查通过后发送初始事件，此后每个操作返回结果时发送事件。
    /// 没有分析阶段进度、逐块进度或额外终止事件；最终状态以返回报告为准。
    /// 回调同步运行在本 future 中，无需 static 生命周期，不能长时间阻塞。
    ///
    /// 分析阶段取消返回 [`Error::Cancelled`]；执行阶段取消返回带 cancelled 标志
    /// 的部分报告。已经完成的操作不回滚，令牌不会被重置。
    ///
    /// # Errors
    /// 预执行阶段可返回配置/存储错误、[`Error::Conflicts`] 或 [`Error::Cancelled`]。
    /// 执行阶段的单文件错误存放在 [`SyncReport::errors`]。
    ///
    /// # Panics
    /// 回调 panic 会传播。底层异步 I/O 需要 Tokio runtime。
    pub async fn run_with_progress<F>(
        &self,
        cancel: &CancellationToken,
        mut progress: F,
    ) -> Result<SyncReport>
    where
        F: FnMut(&SyncProgress) + Send,
    {
        let started = Instant::now();
        let (source, dest, plan) = tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(Error::Cancelled),
            result = async {
                self.validate_roots()?;
                let source = self.source.connect()?;
                let dest = self.destination.connect()?;
                let plan = self.plan(source.as_ref(), dest.as_ref()).await?;
                Ok::<_, Error>((source, dest, plan))
            } => result?,
        };
        let summary = plan.summary();
        if summary.conflict_count > 0 {
            return Err(Error::Conflicts(summary.conflict_count));
        }
        let mut report = SyncReport {
            files_skipped: summary.skip_count,
            ..SyncReport::default()
        };
        let total = summary.copy_count + summary.reverse_copy_count + summary.delete_count;
        let mut completed = 0;
        progress(&SyncProgress {
            current_path: None,
            operations_total: total,
            operations_completed: 0,
            bytes_transferred: 0,
            files_failed: 0,
        });
        // Complete every copy before starting deletions. Never delete after a failed copy.
        for deleting in [false, true] {
            if deleting && (report.files_failed > 0 || report.cancelled) {
                break;
            }
            let actions: Vec<_> = plan
                .actions
                .iter()
                .filter(|action| {
                    if deleting {
                        matches!(action, SyncAction::Delete { .. })
                    } else {
                        matches!(action, SyncAction::Copy { .. })
                    }
                })
                .collect();
            let mut pending = stream::iter(actions.into_iter().map(|action| {
                let source = source.clone();
                let dest = dest.clone();
                async move {
                    (
                        action,
                        execute(action, source, dest, self.config.chunk_size).await,
                    )
                }
            }))
            .buffer_unordered(self.config.max_concurrent_transfers);
            loop {
                let next = tokio::select! {
                    biased;
                    () = cancel.cancelled() => { report.cancelled = true; break; },
                    next = pending.next() => next,
                };
                let Some((action, result)) = next else {
                    break;
                };
                let path = action_path(action).to_owned();
                match result {
                    Ok(bytes) => {
                        if deleting {
                            report.files_deleted += 1;
                        } else {
                            report.files_copied += 1;
                        }
                        report.bytes_transferred += bytes;
                    }
                    Err(error) => {
                        report.files_failed += 1;
                        report.errors.push(SyncFailure {
                            path: path.clone(),
                            message: error.to_string(),
                        });
                    }
                }
                completed += 1;
                progress(&SyncProgress {
                    current_path: Some(path),
                    operations_total: total,
                    operations_completed: completed,
                    bytes_transferred: report.bytes_transferred,
                    files_failed: report.files_failed,
                });
            }
        }
        report.cancelled |= cancel.is_cancelled();
        report.elapsed = started.elapsed();
        Ok(report)
    }
}
fn action_path(action: &SyncAction) -> &str {
    match action {
        SyncAction::Copy { source_path, .. } => source_path,
        SyncAction::Delete { path, .. }
        | SyncAction::Skip { path }
        | SyncAction::Conflict { path, .. } => path,
    }
}
async fn hash(storage: &dyn Storage, path: &str, size: u64, chunk_size: usize) -> Result<String> {
    let mut hasher = blake3::Hasher::new();
    let mut offset = 0;
    while offset < size {
        let len = (size - offset).min(chunk_size as u64);
        let data = storage.read_range(path, offset, len).await?;
        if data.len() as u64 != len {
            return Err(anyhow::anyhow!("file changed while hashing: {path}").into());
        }
        hasher.update(&data);
        offset += len;
    }
    Ok(hasher.finalize().to_hex().to_string())
}
async fn execute(
    action: &SyncAction,
    source: Arc<dyn Storage>,
    dest: Arc<dyn Storage>,
    chunk_size: usize,
) -> anyhow::Result<u64> {
    match action {
        SyncAction::Copy {
            source_path,
            dest_path,
            size,
            reverse,
        } => {
            let (from, to) = if *reverse {
                (dest, source)
            } else {
                (source, dest)
            };
            let before = from
                .stat(source_path)
                .await?
                .ok_or_else(|| anyhow::anyhow!("source disappeared: {source_path}"))?;
            anyhow::ensure!(
                !before.is_dir && before.size == *size,
                "source changed: {source_path}"
            );
            let chunks = stream::try_unfold(0_u64, |offset| {
                let from = from.clone();
                async move {
                    if offset == *size {
                        return Ok(None);
                    }
                    let length = (*size - offset).min(chunk_size as u64);
                    let data = from.read_range(source_path, offset, length).await?;
                    anyhow::ensure!(data.len() as u64 == length, "source changed: {source_path}");
                    Ok(Some((data, offset + length)))
                }
            });
            to.write_stream(dest_path, Box::pin(chunks), Some(*size))
                .await?;
            Ok(*size)
        }
        SyncAction::Delete { path, from_dest } => {
            if *from_dest { dest } else { source }.delete(path).await?;
            Ok(0)
        }
        _ => Ok(0),
    }
}
