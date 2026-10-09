# 按需启用 OpenDAL 后端

本 crate 对齐 OpenDAL **0.59.4** 的全部 **68 个 `services-*` feature 开关**（包含 `services-redis-native-tls` 这样的服务变体），名称与上游完全一致。所有 `layers-*`、HTTP transport、blocking、注册及 executor/internal 辅助开关也按同名转发；上游开发测试聚合 feature `tests` 不转发。

## 依赖配置

```toml
# 零 OpenDAL 依赖的内置本地同步，仍是默认行为。
opendal-sync = { path = "/path/to/SyncTools", default-features = false }

# 举例：按实际需求选择 GCS 和 Azure Blob，以及 HTTPS/Tokio 支持。
# opendal-sync = { path = "/path/to/SyncTools", default-features = false,
#              features = ["services-gcs", "services-azblob", "tls-rustls", "executors-tokio"] }
```

`opendal` 依赖配置为 `default-features = false`：不会因启用一个服务就隐式启用 memory 或 TLS。Cargo feature 仍是整个依赖图的并集；其他依赖若启用 OpenDAL feature，也会参与合并。

| Feature | 作用 |
| --- | --- |
| `opendal` | 启用通用适配器和同版本 `opendal_sync::opendal` 重导出，不隐式启用服务 |
| `services-<name>` | 启用上游同名服务开关，同时启用适配器 |
| `tls-rustls` | 别名：`http-transport-reqwest-rustls`，使用上游 Rustls HTTP 传输 |
| `tls-native` | 别名：`http-transport-reqwest-native-tls`，通常与 Rustls 二选一 |
| `executors-tokio` | 转发上游 Tokio executor 支持；Tokio 本身仍是同步引擎的运行时 |
| `s3` | 兼容别名：`services-s3` + `tls-rustls` + `executors-tokio` |
| `webdav` | 兼容别名：`services-webdav` + `tls-rustls` + `executors-tokio` |

对于使用 HTTPS 的服务，请选择适用的 TLS feature。某些协议服务拥有自己的传输配置，例如 `services-redis-native-tls`；它也按上游定义转发。`services-fs` 是 OpenDAL 文件系统实现，与默认可用的 `LocalStorage` 不同。

## 从 0.50.2 迁移

- 最低 Rust 版本提高到 **1.91**；OpenDAL 及其服务组件升级为 **0.59.4**。
- 新增服务开关：`services-foyer`、`services-gcs-grpc`、`services-goosefs`、`services-hf`、`services-opfs`、`services-tos`。`services-huggingface` 是上游保留的 `services-hf` 别名。
- 移除上游已删除的 `services-atomicserver`、`services-chainsafe`、`services-icloud`、`services-libsql`、`services-nebula-graph`、`services-supabase`、`internal-path-cache` 和 `layers-blocking`。OpenDAL 阻塞支持现在是 `blocking`；本 crate 的 `BlockingSyncEngine` 不需要该开关。
- `Operator::new(builder)?` 直接返回 operator，不再调用 `.finish()`。能力查询改为 `info().capability()`；修改时间转换为 `timestamp.into_inner().as_second()`；列表不再接受 `Metakey`。
- HTTP 传输由独立组件提供，移除了本 crate 原有的直接 reqwest 0.12 依赖。`tls-native` 不会主动启用 Rustls；Cargo feature 合并仍可能同时启用两者。

### HTTP 传输与初始化

使用 HTTP/HTTPS 服务时选择 `tls-rustls`、`tls-native` 或同名上游 `http-transport-reqwest-*` 开关。`S3Storage`、`WebDavStorage` 和 `OperatorStorage` 构造时会惰性安装选中的进程级默认传输；应用提前安装的传输优先，传入 operator 的自定义上下文保持不变。未选择传输时，调用者需要自行配置 `HttpTransporter`。`services-s3` / `services-webdav` 还启用 `layers-timeout`，以保留专用适配器的超时行为。

直接使用重导出的 OpenDAL API 时，需要遵循上游初始化规则：`http-transport-reqwest`（Rustls 默认传输）配合 `opendal::install_default()`，或自行安装 `HttpTransporter`。URI 构造需要调用 `opendal::init_default_registry()`；`auto-register-services` 可选择启用上游的进程启动注册。仅启用 native TLS 的调用者可通过独立 `opendal-http-transport-reqwest` 组件安装传输。`http-transport-reqwest-rustls-no-provider` 要求应用配置 Rustls crypto provider；上游旧别名 `reqwest-rustls-no-provider-tls` 同时启用默认 Rustls 传输，想禁用默认 provider 时使用前者。

## 全部服务开关

每一项映射到 `opendal/<同名 feature>`。具体服务配置以该版本的 OpenDAL builder 文档为准。

| 本 crate feature | 上游 feature |
| --- | --- |
| `services-aliyun-drive` | `opendal/services-aliyun-drive` |
| `services-alluxio` | `opendal/services-alluxio` |
| `services-azblob` | `opendal/services-azblob` |
| `services-azdls` | `opendal/services-azdls` |
| `services-azfile` | `opendal/services-azfile` |
| `services-b2` | `opendal/services-b2` |
| `services-cacache` | `opendal/services-cacache` |
| `services-cloudflare-kv` | `opendal/services-cloudflare-kv` |
| `services-compfs` | `opendal/services-compfs` |
| `services-cos` | `opendal/services-cos` |
| `services-d1` | `opendal/services-d1` |
| `services-dashmap` | `opendal/services-dashmap` |
| `services-dbfs` | `opendal/services-dbfs` |
| `services-dropbox` | `opendal/services-dropbox` |
| `services-etcd` | `opendal/services-etcd` |
| `services-foundationdb` | `opendal/services-foundationdb` |
| `services-foyer` | `opendal/services-foyer` |
| `services-fs` | `opendal/services-fs` |
| `services-ftp` | `opendal/services-ftp` |
| `services-gcs` | `opendal/services-gcs` |
| `services-gcs-grpc` | `opendal/services-gcs-grpc` |
| `services-gdrive` | `opendal/services-gdrive` |
| `services-ghac` | `opendal/services-ghac` |
| `services-github` | `opendal/services-github` |
| `services-goosefs` | `opendal/services-goosefs` |
| `services-gridfs` | `opendal/services-gridfs` |
| `services-hdfs` | `opendal/services-hdfs` |
| `services-hdfs-native` | `opendal/services-hdfs-native` |
| `services-hf` | `opendal/services-hf` |
| `services-http` | `opendal/services-http` |
| `services-huggingface` | `opendal/services-huggingface` |
| `services-ipfs` | `opendal/services-ipfs` |
| `services-ipmfs` | `opendal/services-ipmfs` |
| `services-koofr` | `opendal/services-koofr` |
| `services-lakefs` | `opendal/services-lakefs` |
| `services-memcached` | `opendal/services-memcached` |
| `services-memory` | `opendal/services-memory` |
| `services-mini-moka` | `opendal/services-mini-moka` |
| `services-moka` | `opendal/services-moka` |
| `services-mongodb` | `opendal/services-mongodb` |
| `services-monoiofs` | `opendal/services-monoiofs` |
| `services-mysql` | `opendal/services-mysql` |
| `services-obs` | `opendal/services-obs` |
| `services-onedrive` | `opendal/services-onedrive` |
| `services-opfs` | `opendal/services-opfs` |
| `services-oss` | `opendal/services-oss` |
| `services-pcloud` | `opendal/services-pcloud` |
| `services-persy` | `opendal/services-persy` |
| `services-postgresql` | `opendal/services-postgresql` |
| `services-redb` | `opendal/services-redb` |
| `services-redis` | `opendal/services-redis` |
| `services-redis-native-tls` | `opendal/services-redis-native-tls` |
| `services-rocksdb` | `opendal/services-rocksdb` |
| `services-s3` | `opendal/services-s3` |
| `services-seafile` | `opendal/services-seafile` |
| `services-sftp` | `opendal/services-sftp` |
| `services-sled` | `opendal/services-sled` |
| `services-sqlite` | `opendal/services-sqlite` |
| `services-surrealdb` | `opendal/services-surrealdb` |
| `services-swift` | `opendal/services-swift` |
| `services-tikv` | `opendal/services-tikv` |
| `services-tos` | `opendal/services-tos` |
| `services-upyun` | `opendal/services-upyun` |
| `services-vercel-artifacts` | `opendal/services-vercel-artifacts` |
| `services-vercel-blob` | `opendal/services-vercel-blob` |
| `services-webdav` | `opendal/services-webdav` |
| `services-webhdfs` | `opendal/services-webhdfs` |
| `services-yandex-disk` | `opendal/services-yandex-disk` |

## 将任意 Operator 接入同步引擎

启用 `services-memory` 后运行无需凭据的示例：

```sh
cargo run --features services-memory --example opendal_backend
```

```rust
use std::sync::Arc;
use opendal_sync::{SyncConfig, SyncEngine, opendal::{Operator, services}, storage::OperatorStorage};

fn build_engine() -> anyhow::Result<SyncEngine> {
    let source = Operator::new(services::Memory::default())?;
    let destination = Operator::new(services::Memory::default())?;
    Ok(SyncEngine::from_storages(
        Arc::new(OperatorStorage::new(source)?),
        Arc::new(OperatorStorage::new(destination)?),
        SyncConfig::default(),
    )?)
}
```

把 Memory builder 替换为选中服务的 builder，配置 bucket、root、凭据、数据库连接等即可。通过 `opendal_sync::opendal` 构造可避免应用误用其他 OpenDAL 版本的 `Operator`。适配器保留传入 operator 已安装的 layer；`operator()` 可借用底层 operator。

原有 `StorageConfig::S3` / `S3Storage` 和 `StorageConfig::WebDav` / `WebDavStorage` 在 canonical feature `services-s3` / `services-webdav` 下也可用，不强制用户启用旧别名。其他服务统一通过 builder + `OperatorStorage` 接入，避免为每个服务重复建立一套配置枚举。

## 能力限制

启用 feature 只表示可以构造相应后端，不代表后端支持完整的文件同步操作：

- 当前同步分析需要 **list、stat、read**。`OperatorStorage::new` 检查能力并拒绝不满足条件的后端，不会把“无法列举”当作空目录。
- 只读后端可用作源；目标还需要 write，镜像删除需要 delete，双向模式两端都需要可写。权限和服务端限制由真实 I/O 返回错误。
- 有些 KV、数据库或 HTTP 后端不支持列举，或者仅提供部分文件语义。它们的 feature 仍然暴露，可直接通过重导出的 OpenDAL API 使用，但不一定能作为同步根。
- 列举支持递归时直接使用递归列表，否则逐层遍历；为保证大小信息完整，适配器对每个条目调用 stat，远程请求数可能多于专用适配器。
- 通用适配器不提供本地实现的原子替换保证、根路径重叠检测、历史缓存或断点续传。不同 backend 的上传缓冲/取消清理能力由上游决定，调用者需保证源目标根互不重叠。

## 构建验证

```sh
cargo test --no-default-features
cargo test --features services-memory,services-fs,services-http,s3,webdav
cargo run --features services-memory --example opendal_backend
cargo clippy --all-targets --features services-memory,services-fs,services-http,s3,webdav -- -D warnings
```

不要把 `--all-features` 作为通用开发命令：它会同时启用所有数据库、native 库和运行时后端。FoundationDB、HDFS、RocksDB、etcd 等可能需要对应平台、客户端库、JDK、C/C++ 工具链或 protobuf 工具；具体要求按上游服务而定。按实际部署平台选择 feature，并在该环境验证。提供所有 feature 映射不等于已经测试过所有服务端。

可选后端的传递依赖可能要求高于核心 crate 所声明的 Rust 1.91 的编译器；请按所选服务的实际依赖树验证 MSRV。锁文件会记录所有可选依赖，但没有启用的后端不会编译进默认构建。
