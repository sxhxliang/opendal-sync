# opendal-sync API 指南

`opendal-sync` 是文件内容同步库。主要入口是 [`SyncEngine`]（async）和 [`BlockingSyncEngine`]（阻塞）；二者使用相同的配置、计划、进度和报告类型。库不创建应用数据库，不初始化全局日志，也不启动任务调度器。

## 目录

- [依赖与运行时](#依赖与运行时)
- [快速开始](#快速开始)
- [存储配置](#存储配置)
- [同步配置与模式](#同步配置与模式)
- [引擎方法](#引擎方法)
- [预览计划与动作](#预览计划与动作)
- [执行报告与错误处理](#执行报告与错误处理)
- [进度回调与取消](#进度回调与取消)
- [自定义存储](#自定义存储)
- [行为边界与常见问题](#行为边界与常见问题)

## 依赖与运行时

在使用本地 checkout 的应用中声明依赖：

```toml
[dependencies]
opendal-sync = { path = "/path/to/SyncTools" }
# 使用 async API 的应用自行声明 Tokio：
tokio = { version = "1", features = ["rt-multi-thread", "macros", "time"] }
```

| Feature | 默认 | 作用 |
| --- | --- | --- |
| 本地存储 | 启用，无单独 feature | `StorageConfig::local`、`storage::LocalStorage` |
| `s3` | 关闭 | `StorageConfig::S3`、`storage::S3Storage` |
| `webdav` | 关闭 | `StorageConfig::WebDav`、`storage::WebDavStorage` |

启用远程存储：`opendal-sync = { path = "/path/to/SyncTools", features = ["s3", "webdav"] }`。没有启用相应 feature 时，对应枚举变体和类型不存在。另提供上游同名的全部 `services-*` 开关、通用 `storage::OperatorStorage` 与 `opendal_sync::opendal` 重导出，详见仓库 `docs/FEATURES.md`。canonical 服务开关不隐式启用 TLS；HTTPS 服务可选择 `tls-rustls` 或 `tls-native`。

阻塞接口内部也使用 Tokio，但调用者不需要创建 runtime。`SyncEngine::new` 本身是普通函数；其分析/执行 future 必须在 Tokio runtime 中轮询。`BlockingSyncEngine` 的构造和方法调用必须位于 Tokio async 上下文之外；误用返回 `Error::BlockingInAsyncContext`。

## 快速开始

### 阻塞调用

```rust,no_run
use opendal_sync::{BlockingSyncEngine, StorageConfig, SyncConfig};

fn main() -> opendal_sync::Result<()> {
    let engine = BlockingSyncEngine::new(
        StorageConfig::local("./source"),
        StorageConfig::local("./backup"),
        SyncConfig::default(),
    )?;
    let report = engine.run()?;
    if report.is_success() {
        println!("复制 {} 个文件，共 {} 字节", report.files_copied, report.bytes_transferred);
    } else {
        eprintln!("cancelled={}, errors={:?}", report.cancelled, report.errors);
    }
    Ok(())
}
```

### 异步调用

```rust,no_run
use opendal_sync::{StorageConfig, SyncConfig, SyncEngine};

#[tokio::main]
async fn main() -> opendal_sync::Result<()> {
    let engine = SyncEngine::new(
        StorageConfig::local("./source"),
        StorageConfig::local("./backup"),
        SyncConfig::default(),
    )?;
    let report = engine.run().await?;
    if !report.is_success() {
        eprintln!("同步未全部完成：{report:?}");
    }
    Ok(())
}
```

源目录必须存在。不存在的本地目标目录在实际写入文件时创建；只分析或者同步空目录时不会创建它。构造引擎并不验证源目录存在或远程凭据有效，这些检查发生在分析/运行阶段。

## 存储配置

[`StorageConfig`] 按值传给引擎，可 `Clone`。引擎从配置创建后端；重复运行会重新连接、扫描。远程配置的 `Debug` 隐藏全部字段，但凭据仍以普通 `String` 保存在内存中。此类型未实现 Serde 序列化。

### 本地目录

`StorageConfig::local(path: impl Into<PathBuf>) -> StorageConfig` 接受字符串或路径，也可直接使用 `StorageConfig::Local { path }`。

本地配置中的相对路径在分析/执行时相对于进程当前工作目录解析，因此长期保存的配置建议使用绝对路径。本地源/目标不能相同，也不能互相包含；已有祖先的规范化路径参与重叠检查。通过自定义存储入口接入的本地后端不享有配置入口的根目录重叠检查。

### S3

需要 `s3` feature。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `bucket` | `String` | 已有存储桶名称；库不创建 bucket |
| `region` | `String` | AWS 区域或兼容服务要求的区域 |
| `access_key` / `secret_key` | `String` | 显式提供的凭据，不自动读取环境变量 |
| `endpoint` | `Option<String>` | `None` 使用后端默认端点；兼容服务可指定 URL |
| `prefix` | `Option<String>` | 同步根前缀，如 `project/`；`None` 使用桶根 |

```rust,no_run
#[cfg(feature = "services-s3")]
fn s3_config() -> Result<opendal_sync::StorageConfig, std::env::VarError> {
    Ok(opendal_sync::StorageConfig::S3 {
        bucket: "backups".into(),
        region: "us-east-1".into(),
        access_key: std::env::var("AWS_ACCESS_KEY_ID")?,
        secret_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
        endpoint: Some("https://s3.example.com".into()),
        prefix: Some("project/".into()),
    })
}
# fn main() {}
```

当前高层配置没有 session token、自动凭据链或后端专属调优字段。需要额外能力时使用自定义 `Storage` 实现。

### `WebDAV`

需要 `webdav` feature。

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `endpoint` | `String` | `WebDAV` 服务基础 URL，例如 `https://dav.example.com/dav/` |
| `username` / `password` | `String` | HTTP 认证信息 |
| `root` | `Option<String>` | 基础 URL 下的同步根，如 `backup/`；`None` 使用 `/` |

```rust,no_run
#[cfg(feature = "services-webdav")]
fn webdav_config() -> Result<opendal_sync::StorageConfig, std::env::VarError> {
    Ok(opendal_sync::StorageConfig::WebDav {
        endpoint: "https://dav.example.com/dav/".into(),
        username: std::env::var("WEBDAV_USER")?,
        password: std::env::var("WEBDAV_PASSWORD")?,
        root: Some("backup/".into()),
    })
}
# fn main() {}
```

`endpoint` 与 `root` 分别描述基础 URL 和其下的目录，不要重复包含同一个路径前缀。构造后端不会发送创建目录请求；确保服务端根目录可用，远程写入/建目录权限不足会在调用时返回错误。

## 同步配置与模式

[`SyncConfig`] 的全部字段都可直接设置，字段不存在隐式环境变量覆盖。

| 字段 | 默认值 | 作用和限制 |
| --- | --- | --- |
| `mode` | `SyncMode::Backup` | 同步模式 |
| `conflict_policy` | `ConflictPolicy::Fail` | 仅影响双向同步的共有文件冲突 |
| `exclude_patterns` | `Vec::new()` | 两端共同应用的 glob，无默认忽略项 |
| `max_concurrent_transfers` | `4` | 执行阶段同时处理的操作上限，复制和删除阶段都使用；必须大于 0 |
| `chunk_size` | `8 * 1024 * 1024` | 校验和复制的读取块大小，单位字节；必须大于 0 |

```rust
use opendal_sync::{ConflictPolicy, SyncConfig, SyncMode};

let config = SyncConfig {
    max_concurrent_transfers: 2,
    chunk_size: 1024 * 1024,
    ..SyncConfig::default()
}
.with_mode(SyncMode::Bidirectional)
.with_conflict_policy(ConflictPolicy::PreferSource)
.exclude(".git/**")
.exclude("**/*.tmp");
assert_eq!(config.exclude_patterns.len(), 2);
```

三个链式方法均消费并返回 `Self`。`with_mode`、`with_conflict_policy` 替换当前值，`exclude` 追加规则。参数和 glob 语法在引擎构造时验证。

### 模式决策表

| 同一路径的状态 | `Backup` | `Mirror` | `Bidirectional` |
| --- | --- | --- | --- |
| 只有源端有文件 | 源 → 目标 | 源 → 目标 | 源 → 目标 |
| 只有目标端有文件 | 保留并记为 Skip | 删除目标文件 | 目标 → 源 |
| 两端内容相同 | Skip | Skip | Skip |
| 两端内容不同 | 源覆盖目标 | 源覆盖目标 | 按冲突策略处理 |
| 两端均为目录 | 不生成动作 | 不生成动作 | 不生成动作 |
| 文件与目录同名 | 分析失败 | 分析失败 | 分析失败 |

内容比较先比较大小：大小不同视为不同；大小相同时，分块读取两端完整文件并计算 BLAKE3。时间戳和后端 `ETag` 不参与最终相等判断。

双向同步没有历史基线，因此“目标独有文件”会复制回源；无法识别它是新文件还是源端曾删除的文件。不要把它理解为传播删除的双向镜像。

### 冲突策略

| `ConflictPolicy` | 分析结果 | 执行结果 |
| --- | --- | --- |
| `Fail` | 保留 `SyncAction::Conflict` | 任何写入之前返回 `Error::Conflicts(n)` |
| `PreferSource` | 转成正向 Copy | 源覆盖目标 |
| `PreferDestination` | 转成反向 Copy | 目标覆盖源 |
| `Skip` | 转成 Skip | 两端均保留，算作跳过 |

`Skip` 冲突不会使 `report.is_success()` 返回 false。这里的成功表示本次配置要求的操作均成功，不表示两端最终逐文件相同。

### 排除规则

使用 `globset` 的默认 glob 语义，大小写敏感；支持 `*`、`**`、`?`、字符类和花括号候选。不是 `.gitignore` 解析器，不支持用 `!pattern` 重新包含文件。规则作用于根目录下的相对路径以及该路径的所有祖先。

例如 `.exclude("cache")` 排除根目录 `cache` 整棵子树，`.exclude("**/*.tmp")` 排除临时文件。两端应用同样的规则，所以被排除的目标文件不会被镜像删除。规则作用于列举完成后的文件集合，不保证减少远程列举请求；本地遍历发现符号链接或权限错误可能早于规则过滤并导致分析失败。

## 引擎方法

### `SyncEngine`

| 方法 | 返回值 | 说明 |
| --- | --- | --- |
| `new(source, destination, config)` | `Result<SyncEngine>` | 同步构造，验证数字配置和 glob；不建立连接、不写文件 |
| `from_storages(Arc<dyn Storage>, Arc<dyn Storage>, config)` | `Result<SyncEngine>` | 注入已有后端；拒绝完全相同的 Arc 指针 |
| `analyze().await` | `Result<SyncPlan>` | 只读分析；未解决冲突保留在计划中，不返回 `Error::Conflicts` |
| `analyze_with_cancellation(&token).await` | `Result<SyncPlan>` | 可取消的只读分析 |
| `run().await` | `Result<SyncReport>` | 从头分析后执行，无回调，也不提供外部取消 token |
| `run_with_progress(&token, callback).await` | `Result<SyncReport>` | 从头分析后执行，提供进度与协作取消 |

引擎实现 `Clone`，但克隆不会给文件系统加锁。不要并发运行会修改同一批路径的引擎。不同、不重叠的根目录可独立同步。自定义后端的 `Arc` 会在克隆之间共享。

### `BlockingSyncEngine`

方法名称与参数基本相同，分析/执行不加 `.await`。额外提供 `from_engine(engine: SyncEngine) -> Result<BlockingSyncEngine>`，适合将自定义后端引擎包装成阻塞 API。它拥有一个可复用的私有 Tokio runtime，未实现 `Clone`。

从异步代码调用已有阻塞工作流：

```rust,no_run
use opendal_sync::{BlockingSyncEngine, StorageConfig, SyncConfig};

async fn call_blocking() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let report = tokio::task::spawn_blocking(|| {
        let engine = BlockingSyncEngine::new(
            StorageConfig::local("./source"),
            StorageConfig::local("./backup"),
            SyncConfig::default(),
        )?;
        engine.run()
    }).await??;
    if !report.is_success() {
        return Err("同步未全部完成".into());
    }
    Ok(())
}
# fn main() {}
```

## 预览计划与动作

[`SyncPlan::actions`] 返回借用的 `&[SyncAction]`，[`SyncPlan::summary`] 每次根据动作计算并返回新的 [`ActionSummary`]。计划不能直接执行、编辑后提交或绑定某个版本的文件。`run` 始终重新分析，因此预览后发生的文件变化会影响实际操作。

```rust,no_run
use opendal_sync::{StorageConfig, SyncAction, SyncConfig, SyncEngine};

async fn preview() -> opendal_sync::Result<()> {
    let engine = SyncEngine::new(
        StorageConfig::local("./source"), StorageConfig::local("./backup"),
        SyncConfig::default(),
    )?;
    let plan = engine.analyze().await?;
    for action in plan.actions() {
        match action {
            SyncAction::Copy { source_path, dest_path, size, reverse } =>
                println!("copy {source_path} -> {dest_path}, {size} bytes, reverse={reverse}"),
            SyncAction::Delete { path, from_dest } =>
                println!("delete {path}, from_dest={from_dest}"),
            SyncAction::Skip { path } => println!("skip {path}"),
            SyncAction::Conflict { path, conflict_type, .. } =>
                println!("conflict {path}: {conflict_type:?}"),
        }
    }
    println!("预计复制 {} 字节", plan.summary().total_transfer_bytes());
    Ok(())
}
# fn main() {}
```

| 动作 | 字段语义 |
| --- | --- |
| `Copy` | `source_path` 是读取端的相对路径，`dest_path` 是写入端的相对路径；`reverse = true` 表示从引擎目标端复制到源端；`size` 为计划时的文件字节数 |
| `Delete` | `path` 为相对路径；`from_dest = true` 表示删除引擎目标端文件。当前引擎仅镜像模式生成此方向的删除 |
| `Skip` | 内容相同、备份模式保留目标独有文件，或按策略跳过冲突；不只代表内容相同 |
| `Conflict` | 包含相对路径、两端 `Option<FileInfo>` 和 `ConflictType`；当前比较器的冲突两端均有文件 |

当前比较器只生成 `ConflictType::BothModified`，含义是“两端内容不同”，不能证明双方都发生过修改。`SameSizeDifferentTime` 和 `ModifiedVsDeleted` 是保留变体，当前引擎不会生成。

[`ActionSummary`] 中 `copy_count/copy_bytes` 仅统计正向复制，`reverse_copy_count/reverse_copy_bytes` 仅统计反向复制；另有 `delete_count`、`skip_count`、`conflict_count`。`total_files()` 是全部动作数，包括跳过和冲突；`total_transfer_bytes()` 是双向计划复制字节数之和，不包含校验读取流量。

计划动作当前按相对路径排序；执行时先完成全部复制，再开始删除，并发操作完成顺序不保证与计划一致。任意复制失败或取消会阻止删除阶段开始；删除阶段中单项失败不会回滚或阻止其他删除尝试。

## 执行报告与错误处理

成功获得 `Ok(report)` 不代表全部文件同步成功。请始终检查 [`SyncReport::is_success`]。

| 报告字段 | 类型 | 精确含义 |
| --- | --- | --- |
| `files_copied` | `usize` | 成功复制文件数，包含两个方向 |
| `files_deleted` | `usize` | 成功删除操作数 |
| `files_skipped` | `usize` | 分析计划中的 Skip 数；不含因取消/失败而未尝试的操作 |
| `files_failed` | `usize` | 返回错误的复制/删除操作数，与 `errors.len()` 对应 |
| `bytes_transferred` | `u64` | 成功复制文件的完整大小之和；不含哈希读取和失败/取消传输字节 |
| `elapsed` | `Duration` | 从本次 run 开始到返回报告的耗时，包含分析和回调时间 |
| `cancelled` | `bool` | 执行阶段观察到取消；即使若干文件已完成，也可为 true |
| `errors` | `Vec<SyncFailure>` | 每项含 `path: String` 和 `message: String`，顺序按完成顺序，不是路径排序 |

`is_success()` 等价于 `!cancelled && files_failed == 0`。因复制失败而未执行的删除不会另外记为 `files_failed`；报告也没有未尝试操作计数字段。

### 错误类型

[`Result<T>`] 是 `std::result::Result<T, opendal_sync::Error>` 的别名。`Error` 是 `#[non_exhaustive]` 枚举，外部 `match` 应保留兜底分支。

| 错误 | 常见发生阶段 | 含义 |
| --- | --- | --- |
| `InvalidConfig(String)` | 构造、分析、run 的预分析 | 并发/块大小为零、glob 无效、源目录不存在、根重叠、重复存储路径、文件/目录冲突等 |
| `Storage(anyhow::Error)` | 后端构造、列举、stat、哈希读取 | I/O 或后端失败；可通过标准 `Error::source()` 查看底层错误链 |
| `Cancelled` | 分析或 run 的预分析 | 尚未进入执行阶段时被取消 |
| `Conflicts(usize)` | run 的预分析完成后 | 存在指定数量的未解决冲突；本次未开始写入 |
| `BlockingInAsyncContext` | 阻塞构造/方法调用 | 在 Tokio async 上下文中调用阻塞接口 |
| `Runtime(std::io::Error)` | 阻塞引擎构造 | 私有 Tokio runtime 创建失败 |

```rust,no_run
use opendal_sync::{Error, SyncEngine};

async fn run_checked(engine: &SyncEngine) -> opendal_sync::Result<()> {
    match engine.run().await {
        Ok(report) if report.is_success() => println!("同步完成"),
        Ok(report) => {
            eprintln!("取消={}，失败={} 个", report.cancelled, report.files_failed);
            for error in report.errors {
                eprintln!("{}: {}", error.path, error.message);
            }
        }
        Err(Error::Conflicts(count)) => eprintln!("请先为 {count} 个冲突选择策略"),
        Err(Error::Cancelled) => eprintln!("分析已取消"),
        Err(error) => return Err(error),
    }
    Ok(())
}
# fn main() {}
```

示例仅记录部分失败；生产调用方可将 `!is_success()` 映射为自己的业务错误或 CLI 非零退出码。不要根据 `SyncFailure.message` 的文本做稳定的错误分类：它是面向诊断的字符串，不是机器错误码。

## 进度回调与取消

回调类型为 `FnMut(&SyncProgress) + Send`。无需 `'static`，可以借用局部可变状态。async API 在轮询执行 future 的任务中调用回调；任务可能在 runtime 工作线程间迁移，因此不承诺固定 OS 线程。阻塞 API 在阻塞调用过程中同步调用回调。

| 进度字段 | 含义 |
| --- | --- |
| `current_path` | 初始事件为 `None`，其后是刚完成的操作路径；不是实时正在传输的文件 |
| `operations_total` | 计划正向复制 + 反向复制 + 删除数，不包括 Skip 和 Conflict |
| `operations_completed` | 已返回成功或失败结果的操作数，包含失败 |
| `bytes_transferred` | 已成功复制文件的累计字节数，只在完整文件完成后增加 |
| `files_failed` | 已失败的操作数 |

只有分析成功且冲突检查通过后才产生初始事件，其后每项操作完成产生一次事件。没有扫描进度、逐块字节事件、固定时间间隔、ETA 或额外终止事件；取消时的最终状态应读取返回报告。没有可执行操作时总数是 0，百分比计算要处理零分母。回调 panic 会传播，库不会将其转换为 `Error`。

### 使用超时发出协作取消

```rust,no_run
use std::time::Duration;
use opendal_sync::{CancellationToken, Error, SyncEngine};

async fn run_with_deadline(engine: &SyncEngine) -> opendal_sync::Result<()> {
    let cancel = CancellationToken::new();
    let timer_token = cancel.clone();
    let timer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(30)).await;
        timer_token.cancel();
    });
    let mut events = 0;
    let result = engine.run_with_progress(&cancel, |p| {
        events += 1;
        println!("{}/{}，已完成 {} 字节", p.operations_completed,
                 p.operations_total, p.bytes_transferred);
    }).await;
    timer.abort();
    let _ = timer.await;
    match result {
        Ok(report) => println!("events={events}, success={}, cancelled={}",
                              report.is_success(), report.cancelled),
        Err(Error::Cancelled) => println!("分析阶段超时"),
        Err(error) => return Err(error),
    }
    Ok(())
}
# fn main() {}
```

[`CancellationToken`] 重导出自 `tokio_util`。它可跨线程克隆；同一 token 的所有克隆共享取消状态。token 一旦取消不能复位，重跑应创建新的 token。库不会自动重置调用方的 token。

取消是协作式的，不是严格的墙钟时间保证：同步代码或长时间阻塞的回调会延迟响应。已经完成的文件不会回滚。直接 drop/abort 运行 future 不会得到部分报告；需要报告时，应取消 token 后继续等待方法返回。阻塞接口可由另一个线程对 token 调用 `cancel()`。

## 自定义存储

[`Storage`] 是 `Send + Sync` 的异步 trait，可以通过 `Arc<dyn Storage>` 注入。`#[opendal_sync::async_trait]` 宏已重导出，无需再单独引入 `async-trait`。存储层返回 `anyhow::Result`，与高层的 `opendal_sync::Result` 不同；实现自定义后端的应用应直接声明所用的 `anyhow` / `futures` 依赖。

### 方法契约

| 方法 | 必须实现 | 行为 |
| --- | --- | --- |
| `list_files(prefix)` | 是 | 递归返回整个根或前缀下的完整条目，路径始终相对于存储根；列举失败必须返回 Err，不可吞掉错误或返回不完整列表 |
| `stat(path)` | 是 | 存在返回 Some，不存在返回 None；认证、权限、网络失败必须返回 Err；`stat("")` 表示根目录 |
| `read(path)` | 是 | 读取整个文件；引擎主复制流程使用 `read_range` |
| `read_range(path, offset, length)` | 是 | 按字节读取半开区间 `[offset, offset + length)`；引擎对有效区间要求恰好 length 字节，短读视为源变化 |
| `write(path, data)` | 是 | 覆盖写入整个文件，并负责创建所需父目录 |
| `write_stream(path, stream, total_size)` | 否，但推荐覆盖 | 消费所有块并提交；默认实现把全部块缓存为 Vec 后调用 write，且忽略 `total_size` |
| `delete(path)` | 是 | 删除指定文件；引擎不要求递归删目录；本地实现拒绝删除目录 |
| `create_dir(path)` | 是 | 创建目录；引擎不直接调用此方法，写入实现应自行保证父目录可用 |
| `name()` | 是 | 诊断名称，避免含凭据 |
| `exists(path)` | 否 | 默认调用 stat 并检查是否为 Some |
| `copy(from, to)` | 否 | 默认 read 后 write，完整文件驻留内存；引擎不调用此优化接口 |

`write_stream` 的输入允许借用非 `'static` 数据，必须在方法返回前消费完成，不能把它移交给脱离调用生命周期的后台任务。成功仅应在文件完整提交后返回；处理流中的错误，尽可能取消上传并清理临时对象。为大文件提供真正流式的覆盖实现，否则 `chunk_size` 不能限制整个文件缓冲内存。

### 存储元数据

- `FileInfo`：`path` 为相对路径，`size` 为文件字节数，`modified_time` 为 Unix 秒，`is_dir` 区分目录，`checksum` 是可选后端校验字符串。引擎丢弃后端 checksum 并自行计算 BLAKE3。该类型实现 Serde Serialize/Deserialize。
- `FileMeta`：`size`、`modified_time`、`is_dir`、`etag`；根目录存在时 `stat("")` 必须返回 `is_dir = true`。`etag` 不用于引擎内容判等。
- `FileChunk`：`data`、`offset`、`size` 是公开的辅助数据结构；当前引擎与 `write_stream` 使用 `Vec<u8>`，不会消费 `FileChunk`。

引擎要求非空、唯一、UTF-8、以 `/` 分隔的相对文件键，不允许绝对路径、空路径段、`.`、`..`、反斜杠、冒号或 NUL。目录列举可带末尾 `/`，引擎会去掉末尾斜杠后验证；根目录条目可省略。后端也应验证直接调用者提供的路径。远程/自定义存储根的逻辑重叠由调用者检查；引擎只能识别完全相同的 Arc 指针。

### 可编译的适配器示例

下面给现有本地后端增加写入日志，保留其流式写入与原子提交行为。其他自定义后端可按同样的契约实现。

```rust,no_run
use std::{pin::Pin, sync::Arc};
use opendal_sync::{Storage, SyncConfig, SyncEngine};
use opendal_sync::storage::{FileInfo, FileMeta, LocalStorage};

struct LoggedStorage(LocalStorage);

#[opendal_sync::async_trait]
impl Storage for LoggedStorage {
    async fn list_files(&self, prefix: Option<&str>) -> anyhow::Result<Vec<FileInfo>> {
        self.0.list_files(prefix).await
    }
    async fn stat(&self, path: &str) -> anyhow::Result<Option<FileMeta>> {
        self.0.stat(path).await
    }
    async fn read(&self, path: &str) -> anyhow::Result<Vec<u8>> {
        self.0.read(path).await
    }
    async fn read_range(&self, path: &str, offset: u64, length: u64) -> anyhow::Result<Vec<u8>> {
        self.0.read_range(path, offset, length).await
    }
    async fn write(&self, path: &str, data: Vec<u8>) -> anyhow::Result<()> {
        eprintln!("write {path}");
        self.0.write(path, data).await
    }
    async fn write_stream<'stream>(
        &self, path: &str,
        stream: Pin<Box<dyn futures::Stream<Item = anyhow::Result<Vec<u8>>> + Send + 'stream>>,
        total_size: Option<u64>,
    ) -> anyhow::Result<()> {
        eprintln!("stream {path}, size={total_size:?}");
        self.0.write_stream(path, stream, total_size).await
    }
    async fn delete(&self, path: &str) -> anyhow::Result<()> { self.0.delete(path).await }
    async fn create_dir(&self, path: &str) -> anyhow::Result<()> { self.0.create_dir(path).await }
    fn name(&self) -> &str { "logged-local" }
}

async fn custom_backend() -> anyhow::Result<()> {
    let source = Arc::new(LocalStorage::new("./source")?);
    let destination = Arc::new(LoggedStorage(LocalStorage::new("./backup")?));
    let engine = SyncEngine::from_storages(source, destination, SyncConfig::default())?;
    let report = engine.run().await?;
    anyhow::ensure!(report.is_success(), "同步未全部完成: {report:?}");
    Ok(())
}
# fn main() {}
```

### 直接使用内置后端

`LocalStorage::new(path: impl AsRef<Path>)`、`S3Storage::new(bucket, region, access_key, secret_key, endpoint: Option<&str>, prefix: Option<&str>)`、`WebDavStorage::new(endpoint, username, password, root: Option<&str>)` 都是普通函数，返回 `anyhow::Result<Self>`。远程构造不验证网络连接；本地构造会检查和解析路径，但不创建根目录。通过 trait 方法直接使用时，方法自身的后端行为适用，不会执行高层引擎的扫描、冲突和重叠校验。

S3/`WebDAV` 使用 `OpenDAL` 的超时层，配置为 `OP_TIMEOUT_SECS = 60`、`IO_TIMEOUT_SECS = 300`；这些常量不是整次同步的 deadline，也不是高层可变配置。

## 行为边界与常见问题

**为什么没有变化也读取很多数据？** 同大小共有文件需要内容哈希确认相等。没有持久化哈希缓存；先 analyze 再 run 会分析两次。远程校验可能产生下载费用。

**能保证只用 `并发数 × chunk_size` 内存吗？** 不能。读取块受限制，但扫描条目、计划、后端缓冲和网络协议也占内存；默认自定义 `write_stream` 会完整缓存文件。

**会同步空目录、权限或时间戳吗？** 不会。同步普通文件内容，不保留权限、时间戳、扩展属性、符号链接或空目录；镜像不会清理空目录。本地特殊文件和非 UTF-8 文件名会报错。

**失败后可以重试吗？** 可以重新 run，重新分析后跳过内容相同的文件。引擎没有应用层自动重试配置、跨进程断点状态、multipart 恢复 API 或回滚事务。

**取消会恢复原状吗？** 不会。已经成功的文件保留。本地逐文件采用同目录临时文件后提交，流错误会保留该目标原文件并清理临时文件。远程上传的原子性、缓冲和取消后对象/分片清理由后端决定，不能推断与本地相同。

**同步期间可以继续修改源文件吗？** 不应这样使用。引擎会检查部分大小变化和短读，但不是快照、一致性事务或针对并发恶意替换路径的沙箱。尤其是同大小并发修改，不保证能检测到。调用者应避免源/目标在同步期间被其他任务写入。

**可用 JSON 保存配置或报告吗？** 当前高层配置、计划、报告没有实现 Serde；请使用应用自己的 DTO 做转换。只有 `storage::FileInfo` 提供序列化实现。

**怎么生成离线 API 页面？** 在仓库根目录执行 `cargo doc --no-deps --features s3,webdav,services-memory,services-fs`，入口为 `target/doc/opendal_sync/index.html`。本指南同时编入 crate 首页，所有 Rust 示例由 doctest 检查。`no_run` 示例会编译但不会访问本地目录或远程服务。

## 场景示例索引

更多可运行 examples 见仓库 `examples/README.md`。其中 `examples/support` 提供示例层的远程列表缓存、可选小文件缓冲阈值和手动保留两者流程，不是新增的 `SyncConfig` 字段或冲突枚举变体。
