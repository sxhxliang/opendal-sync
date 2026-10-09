# opendal-sync

独立的 Rust 文件同步库，同时提供 async API 和阻塞 API。无需桌面框架、数据库、全局配置或日志初始化。支持本地、S3、WebDAV 之间的文件同步。

最低 Rust 版本：**1.91**。OpenDAL 升级及 feature 迁移说明见 [Feature 指南](docs/FEATURES.md#从-0502-迁移)。

## API 文档

- [详细 API 指南](docs/API.md)：方法签名、配置默认值、模式决策表、报告与错误、进度取消、自定义存储及完整示例。
- 运行 `cargo doc --no-deps --features s3,webdav,services-memory,services-fs` 生成逐项 Rustdoc，入口为 `target/doc/opendal_sync/index.html`。
- 指南同时作为 Rustdoc 首页，Rust 示例纳入 doctest 编译检查。

## 可选存储后端

按 OpenDAL 0.59.4 同名转发全部 68 个 `services-*` 开关；默认不启用 OpenDAL。详细列表、TLS 选择、通用 Operator 接入和平台要求见 [Feature 指南](docs/FEATURES.md)。旧 `s3` / `webdav` 别名继续可用。

## 使用

```toml
[dependencies]
opendal-sync = { path = "/path/to/SyncTools" }
# 远程后端按需启用：features = ["s3", "webdav"]
```

默认仅启用本地后端。阻塞接口内部持有可复用的 Tokio runtime，async 接口使用调用方的 Tokio runtime。

### 阻塞 API

```rust,no_run
use opendal_sync::{BlockingSyncEngine, StorageConfig, SyncConfig, SyncMode};

fn main() -> opendal_sync::Result<()> {
    let engine = BlockingSyncEngine::new(
        StorageConfig::local("./source"),
        StorageConfig::local("./backup"),
        SyncConfig::default()
            .with_mode(SyncMode::Backup)
            .exclude(".git/**"),
    )?;

    let plan = engine.analyze()?; // 只读预览，不创建目标目录
    println!("{:?}", plan.summary());

    let report = engine.run()?; // 重新扫描后执行
    if !report.is_success() {
        eprintln!("取消或部分失败：{:?}", report.errors);
    }
    Ok(())
}
```

### Async API、进度和取消

```rust,no_run
use opendal_sync::{CancellationToken, StorageConfig, SyncConfig, SyncEngine};

async fn synchronize() -> opendal_sync::Result<()> {
    let engine = SyncEngine::new(
        StorageConfig::local("./source"),
        StorageConfig::local("./backup"),
        SyncConfig::default(),
    )?;
    let cancel = CancellationToken::new();
    // 将 cancel.clone() 交给控制线程/任务，调用 cancel() 即可取消。
    let report = engine.run_with_progress(&cancel, |progress| {
        println!("{}/{}", progress.operations_completed, progress.operations_total);
    }).await?;
    println!("复制 {} 个文件", report.files_copied);
    Ok(())
}
```

阻塞接口也提供 `run_with_progress` 和 `analyze_with_cancellation`。进度回调在调用线程/任务内执行，不应长时间阻塞。分析阶段取消返回 `Error::Cancelled`；执行阶段取消返回 `cancelled = true` 的部分报告。每次调用使用独立 token。阻塞 API 在 Tokio 异步上下文中返回明确错误；需要混用时请在 `spawn_blocking` 内构造和调用。

## 同步语义

| 模式 | 行为 |
| --- | --- |
| `Backup`（默认） | 将源端新增/变化的文件复制到目标，保留目标独有文件 |
| `Mirror` | 在复制成功后删除目标独有文件；任何复制失败都会阻止删除阶段 |
| `Bidirectional` | 双向复制独有文件；共有但内容不同的文件视为冲突 |

双向冲突默认 `ConflictPolicy::Fail`，在任何写入前返回 `Error::Conflicts`。可显式选择 `PreferSource`、`PreferDestination` 或 `Skip`。没有历史状态，因此双向模式不传播删除，也不根据时间戳猜测哪一端正确。

同大小文件分块计算 BLAKE3，避免只比较大小/时间戳或不同服务商的 ETag 导致漏同步。分析会读取两端同大小文件的全部内容，远程存储会产生读取流量。执行始终重新扫描；预览不是可直接执行的冻结计划。

复制使用有界并发和分块读取。本地写入通过同目录唯一临时文件提交，错误时保留原文件并清理临时文件。S3/WebDAV 使用 OpenDAL 流式写入；远程服务的缓冲、原子性、取消后残留对象/分片由后端能力决定。统计的 `bytes_transferred` 仅包含成功复制的文件大小，不包含校验流量或失败传输。

`Err` 表示配置、连接、扫描或预执行冲突失败。执行中的单文件失败记录在 `SyncReport.errors`；调用方必须检查 `is_success()`。已经完成的写入不会回滚。

## 存储后端

```rust,ignore
let s3 = StorageConfig::S3 {
    bucket: "backups".into(),
    region: "us-east-1".into(),
    access_key: std::env::var("AWS_ACCESS_KEY_ID")?,
    secret_key: std::env::var("AWS_SECRET_ACCESS_KEY")?,
    endpoint: Some("https://s3.example.com".into()),
    prefix: Some("project/".into()),
};

let webdav = StorageConfig::WebDav {
    endpoint: "https://dav.example.com/".into(),
    username: std::env::var("WEBDAV_USER")?,
    password: std::env::var("WEBDAV_PASSWORD")?,
    root: Some("backup/".into()),
};
```

启用对应 feature 后，可将这些配置直接传入任一引擎。`StorageConfig` 的 Debug 不输出远程凭据。也可实现公开的 `storage::Storage`，通过 `SyncEngine::from_storages` 接入自定义后端。

同步的是普通文件内容，不保留权限、时间戳、扩展属性或空目录；镜像模式不清理空目录。本地符号链接、特殊文件、非 UTF-8 名称、不安全相对路径和文件/目录冲突会报错；源目录必须存在，本地源与目标不得重叠。调用方应保证同步期间没有其他进程修改目录结构或文件；这不是文件系统快照或针对并发恶意替换路径的沙箱。自定义/远程后端的根目录也必须互不重叠。

排除规则是相对根目录的 glob，匹配目录会排除整个子树，两端都适用；被排除的文件不会被镜像删除。默认没有隐式排除。

## 更多 Examples

见 [Examples 场景指南](examples/README.md)：任意存储组合、三种模式、远程列表缓存、并行与流式阈值、5 GiB 虚拟文件、逐文件保留源/目标/两者/跳过，以及只读预览。缓存、阈值与保留两者为示例层实现，核心 API 保持不变。

Cloudflare R2 专用示例：`cargo run --features s3 --example cloudflare_r2 -- --help`，配置和上传/下载流程见 [R2 示例指南](examples/README.md#8-cloudflare-r2)。

## CLI example

```sh
cargo run --example sync -- ./source ./backup --dry-run
cargo run --example sync -- ./source ./backup
cargo run --example sync -- ./source ./mirror --mirror
cargo run --example sync -- ./source ./other --bidirectional
cargo run --example sync -- --help
```

CLI 默认备份模式；失败或取消时退出码非零。示例仅演示本地路径同步，远程配置通过库 API 使用。异步调用示例见 `examples/async_sync.rs`。

## 开发

```sh
cargo test
cargo test --features s3,webdav,services-memory,services-fs
cargo clippy --all-targets --features s3,webdav,services-memory,services-fs
cargo fmt --check
cargo doc --no-deps --features s3,webdav,services-memory,services-fs
```