# Examples

从仓库根目录运行。除 `sync_modes` 和 `virtual_large_file` 外，新示例默认只读预览，显式添加 `--apply` 才会修改文件。`sync_preview` 始终只读。旧的 `sync`、`async_sync` 示例仍按原有行为直接同步。

| 示例 | 覆盖场景 | 命令 |
| --- | --- | --- |
| [opendal_backend](opendal_backend.rs) | 通用 OpenDAL Operator 接入，内存存储镜像演示 | `cargo run --features services-memory --example opendal_backend` |
| [cloudflare_r2](cloudflare_r2.rs) | Cloudflare R2 上传/下载、备份/镜像/双向、只读预览 | `cargo run --features s3 --example cloudflare_r2 -- --help` |
| [sync_modes](sync_modes.rs) | 本地备份、镜像、双向模式及第二次增量跳过 | `cargo run --example sync_modes` |
| [storage_combinations](storage_combinations.rs) | 本地/S3/WebDAV 任意源目标组合 | `cargo run --features s3,webdav,services-memory,services-fs --example storage_combinations` |
| [sync_preview](sync_preview.rs) | 只读差异、复制方向、删除、冲突和字节汇总 | `cargo run --features s3,webdav,services-memory,services-fs --example sync_preview` |
| [cached_preview](cached_preview.rs) | 远程列表 TTL 0–7200 秒、本地直接扫描、执行前刷新 | `cargo run --features s3,webdav,services-memory,services-fs --example cached_preview` |
| [tuned_sync](tuned_sync.rs) | 并发 1–128、块大小、流式阈值、进度、执行后增量预览 | `cargo run --features s3,webdav,services-memory,services-fs --example tuned_sync` |
| [virtual_large_file](virtual_large_file.rs) | 5 GiB + 17 字节虚拟文件，经真实引擎分块传输 | `cargo run --example virtual_large_file` |
| [manual_conflicts](manual_conflicts.rs) | 逐文件选择源/目标/两者/跳过 | `cargo run --features s3,webdav,services-memory,services-fs --example manual_conflicts -- --apply` |
| [sync](sync.rs) | 简单阻塞 CLI，路径参数 | `cargo run --example sync -- SOURCE DEST --dry-run` |
| [async_sync](async_sync.rs) | 最小 async 调用 | `cargo run --example async_sync -- SOURCE DEST` |

## 示例层能力与库 API

`support/mod.rs` 是可复制到应用中调整的示例代码，不属于 `opendal-sync` 的公开 API：

- `ExampleStorage` 为远程文件列表增加进程内 TTL 缓存，为小文件增加可选缓冲阈值；本地后端始终绕过列表缓存。
- `config()` 读取环境变量，并验证示例的并发范围为 1–128；库本身仍只要求并发数大于零。
- `keep_both()` 演示通过额外文件保存冲突两端版本；没有新增 `ConflictPolicy::KeepBoth`。

核心 `SyncEngine` 仍然重新分析、使用内容哈希跳过相同文件，并以 `chunk_size` 分块读取。这里的“增量”是文件级跳过，不是块级差量上传或跨进程断点续传。不要将本示例的缓存、缓冲阈值误认为 `SyncConfig` 的原生字段。

## 1. 无需凭据的本地模式演示

```sh
cargo run --example sync_modes
```

在自动清理的临时目录中创建测试文件，依次运行三种模式并验证：

- 备份：复制源独有文件、覆盖源端变化，保留目标独有文件。
- 镜像：复制源文件，成功后删除目标独有文件。
- 双向：将各端独有文件复制到另一端。
- 第二次运行：内容相同的文件不再复制。

镜像处理的是文件内容，不同步权限、时间戳或空目录；不会清理多余空目录。双向模式没有历史删除传播，共有文件内容不同时是冲突。

## 2. 配置任意存储组合

`storage_combinations`、`sync_preview`、`cached_preview`、`tuned_sync` 和 `manual_conflicts` 共用环境变量。`SRC_` 配置源端，`DST_` 配置目标端，二者独立选择类型：

| 类型 | 必填变量（把 `SIDE` 换成 `SRC` 或 `DST`） | 可选变量 |
| --- | --- | --- |
| `local`（默认） | `SIDE_PATH` | 无 |
| `s3` | `SIDE_KIND=s3`、`SIDE_BUCKET`、`SIDE_REGION`、`SIDE_ACCESS_KEY`、`SIDE_SECRET_KEY` | `SIDE_ENDPOINT`、`SIDE_PREFIX` |
| `webdav` | `SIDE_KIND=webdav`、`SIDE_ENDPOINT`、`SIDE_USERNAME`、`SIDE_PASSWORD` | `SIDE_ROOT` |

源本地目录必须存在，本地源目标不得相同或互相包含；示例适配器会检查这一点。远程根必须由调用者保证互不重叠，bucket 和服务端根目录应预先配置好。凭据通过环境变量提供，不打印到日志。

### 本地 → 本地

```sh
mkdir -p /tmp/opendal-sync-demo-source
printf 'hello\n' > /tmp/opendal-sync-demo-source/hello.txt
export SRC_KIND=local SRC_PATH=/tmp/opendal-sync-demo-source
export DST_KIND=local DST_PATH=/tmp/opendal-sync-demo-backup
export SYNC_MODE=backup
cargo run --example storage_combinations
cargo run --example storage_combinations -- --apply
```

### 本地 → S3

```sh
export SRC_KIND=local SRC_PATH=/path/to/source
export DST_KIND=s3 DST_BUCKET=my-backup DST_REGION=us-east-1
export DST_ACCESS_KEY="$AWS_ACCESS_KEY_ID" DST_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export DST_PREFIX=project/
# 兼容服务可设置：export DST_ENDPOINT=https://s3.example.com
cargo run --features s3 --example storage_combinations
cargo run --features s3 --example storage_combinations -- --apply
```

S3 → 本地只需反过来配置角色：

```sh
export SRC_KIND=s3 SRC_BUCKET=my-backup SRC_REGION=us-east-1
export SRC_ACCESS_KEY="$AWS_ACCESS_KEY_ID" SRC_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export SRC_PREFIX=project/
export DST_KIND=local DST_PATH=/path/to/downloads
cargo run --features s3 --example storage_combinations -- --apply
```

### S3 → WebDAV（远程 → 远程）

```sh
export SRC_KIND=s3 SRC_BUCKET=my-backup SRC_REGION=us-east-1
export SRC_ACCESS_KEY="$AWS_ACCESS_KEY_ID" SRC_SECRET_KEY="$AWS_SECRET_ACCESS_KEY"
export SRC_PREFIX=project/
export DST_KIND=webdav DST_ENDPOINT=https://dav.example.com/dav/
export DST_USERNAME="$WEBDAV_USER" DST_PASSWORD="$WEBDAV_PASSWORD"
export DST_ROOT=backup/
cargo run --features s3,webdav,services-memory,services-fs --example storage_combinations
cargo run --features s3,webdav,services-memory,services-fs --example storage_combinations -- --apply
```

WebDAV → S3、WebDAV → WebDAV、S3 → S3 同样按两组变量独立配置即可；本地 ↔ WebDAV 只需将其中一端设为 local。远程 → 远程的数据经当前进程中转，不是服务端之间的直传。

## 3. 同步模式和预览

```sh
# 使用上面配置好的 SRC_* / DST_*。
SYNC_MODE=backup cargo run --features s3,webdav,services-memory,services-fs --example sync_preview
SYNC_MODE=mirror cargo run --features s3,webdav,services-memory,services-fs --example storage_combinations
SYNC_MODE=mirror cargo run --features s3,webdav,services-memory,services-fs --example storage_combinations -- --apply
SYNC_MODE=bidirectional cargo run --features s3,webdav,services-memory,services-fs --example sync_preview
```

预览输出 `Copy`（含 `reverse`）、`Delete`、`Skip`、`Conflict`，以及操作数和计划复制字节数。`sync_preview` 不接受 `--apply`。普通组合示例的双向冲突默认阻止全部写入，需使用下面的人工选择示例。

## 4. 智能列表缓存

```sh
CACHE_TTL_SECS=600 cargo run --features s3,webdav,services-memory,services-fs --example cached_preview
CACHE_TTL_SECS=7200 cargo run --features s3,webdav,services-memory,services-fs --example cached_preview -- --apply
CACHE_TTL_SECS=0 cargo run --features s3,webdav,services-memory,services-fs --example cached_preview
```

每次运行连续分析两次。远程列表会打印 MISS/HIT；0 禁用缓存，最大 7200 秒。缓存按后端实例和列举前缀隔离，最多保存 16 个前缀条目，不落盘；跨 CLI 进程不复用。本地始终重新列举，即使 TTL 非零。

缓存只保存文件列表，不保存内容或哈希，因此相同大小的共有文件依然会重新读取计算 BLAKE3。外部新增/删除文件可能暂时不体现在缓存预览中，已有文件的外部变化也可能导致哈希错误；不要把缓存预览当作权威快照。

每个 `--apply` 流程都会清空两端预览缓存，执行重新列举后再决定删除。写入/删除会清理对应适配器缓存；示例没有监听其他进程的修改，不应让多个任务共享相同端点并发修改。

## 5. 并行、增量与流式阈值

```sh
MAX_CONCURRENT=16 CHUNK_MIB=8 STREAM_THRESHOLD_MIB=32 \
  cargo run --features s3,webdav,services-memory,services-fs --example tuned_sync -- --apply
```

| 参数 | 默认 | 语义 |
| --- | --- | --- |
| `MAX_CONCURRENT` | 4 | 1–128 个并行操作，按后端限流和机器资源选择 |
| `CHUNK_MIB` | 8 | 正整数，校验/复制读取块大小，单位 MiB |
| `STREAM_THRESHOLD_MIB` | 8 | 已知文件大小不超过阈值时完整缓冲后 write；更大或长度未知时调用后端 write_stream |
| `CACHE_TTL_SECS` | 0 | 示例远程列表缓存期限，0–7200 秒 |
| `SYNC_MODE` | backup | backup / mirror / bidirectional |

将阈值设为 0，可使所有非空文件走流式写入。小文件缓冲会增加内存消耗；总内存还包括并发块、文件列表和后端缓冲，不能保证等于并发数乘块大小。部分远程服务/驱动仍可能缓冲整个上传。

运行会显示提交进度，结束后再分析一次展示增量跳过。若同步成功且没有外部修改，备份/镜像的内容相同文件不再复制。进度字节是成功复制文件的总大小，不包括校验流量，也不是实时网络吞吐率。

## 6. 4 GiB 以上大文件

```sh
cargo run --example virtual_large_file
```

构造 5 GiB + 17 字节的虚拟源和计数接收端，用真实引擎以最多 2 MiB 的块传输。它检查每次读取偏移连续、跨越 32 位边界、完整接收尾块、报告总字节数正确；若引擎退回全文件读写，示例会失败。不会创建巨型磁盘文件，也不是 I/O 性能基准。

真实大文件使用 `tuned_sync`，把本地源根指向已有大文件所在目录，例如：

```sh
SRC_KIND=local SRC_PATH=/path/to/large-files \
DST_KIND=local DST_PATH=/path/to/backup \
CHUNK_MIB=8 STREAM_THRESHOLD_MIB=0 MAX_CONCURRENT=2 \
  cargo run --example tuned_sync -- --apply
```

源目录中所有普通文件都参与同步。真实远程 4 GiB+ 上传还取决于服务端大小限制和 OpenDAL 后端能力；虚拟示例不代替真实服务端联调。

## 7. 手动解决冲突

```sh
SYNC_MODE=bidirectional cargo run --features s3,webdav,services-memory,services-fs --example manual_conflicts
SYNC_MODE=bidirectional cargo run --features s3,webdav,services-memory,services-fs --example manual_conflicts -- --apply
```

该示例总是使用双向模式，每个共有但内容不同的文件分别询问：

| 输入 | 操作 |
| --- | --- |
| `s` | 源版本覆盖目标 |
| `d` | 目标版本覆盖源 |
| `b` | 将目标版本保存到两端相同的新名字，再用源版本覆盖原目标路径 |
| `k` | 保留两端原样、跳过冲突 |

例如 `notes.txt` 选择 `b` 后，两端 `notes.txt` 都是源版本，两端 `notes.txt.conflict-destination-1` 都是目标版本。若候选名在任一端已存在，则尝试 `-2`、`-3`，不会故意覆盖已有备份。先完成两个备份副本，再覆盖目标原路径；任一步出错会停止，已完成的副本不会回滚。

处理完成后重新分析并双向同步独有文件；最终使用 `ConflictPolicy::Skip`，未选择处理或期间新出现的冲突保留原样。手动复制的计数不包含在最后一次 engine.run 的报告里。输入 EOF 会停止并返回错误；此前完成的选择不会撤销。

整个交互期间应避免其他任务修改这些路径。候选名检查和写入不是原子的跨后端锁，不提供并发竞争下的无覆盖保证。此例重点展示调用流程，生产应用应添加自己的互斥、审计或恢复机制。

## 验证命令

```sh
cargo build --examples
cargo build --features s3,webdav,services-memory,services-fs --examples
cargo test --features s3,webdav,services-memory,services-fs --example cached_preview
cargo run --example sync_modes
cargo run --example virtual_large_file
cargo clippy --all-targets --features s3,webdav,services-memory,services-fs -- -D warnings
```

缓存/阈值/保留两者辅助逻辑在 `cached_preview` 的 example 测试中覆盖；`support` 被多个例子共用，因此 `cargo test --examples` 会在这些示例目标中重复执行相同辅助测试。远程组合需要自行提供真实服务与凭据；自动检查不访问真实 S3/WebDAV。

## 8. Cloudflare R2

专用示例使用现有 S3 后端，签名 region 固定为 `auto`，无需新增依赖。默认只读预览，`--apply` 才执行。`upload` 的源是本地，`download` 的源是 R2；两种方向都支持备份、镜像与双向模式。

### 配置

在 Cloudflare 控制台创建 R2 bucket，并为相应 bucket 生成具有对象读写权限的 R2 S3 API 凭据。这里需要 **Access Key ID / Secret Access Key**，不能直接使用 Cloudflare 通用 bearer API token。

```sh
export R2_ACCOUNT_ID=你的32位CloudflareAccountID
export R2_BUCKET=my-backup
export R2_ACCESS_KEY_ID=你的R2AccessKeyID
export R2_SECRET_ACCESS_KEY=你的R2SecretAccessKey
export R2_PREFIX=project/  # 可选；未设置时同步整个 bucket
```

默认端点为 `https://<ACCOUNT_ID>.r2.cloudflarestorage.com`。示例从 `R2_ACCOUNT_ID` 构造端点；它不是 bucket 名或 Zone ID。也可设置 `R2_ENDPOINT`，此时无需 Account ID。对于 EU/FedRAMP 等 jurisdiction-specific bucket，应使用控制台显示的对应 S3 API endpoint，例如 EU 端点：

```sh
export R2_ENDPOINT=https://你的AccountID.eu.r2.cloudflarestorage.com
```

端点不要包含 bucket 路径，不要使用 `r2.dev` 公共访问 URL 或绑定的自定义下载域名。bucket 和前缀由单独字段配置。示例不创建 bucket，不打印密钥。仅下载也会执行内容比较，因此凭据至少需要所访问对象的读取和列举权限；上传/镜像/双向应具备所需写入和删除权限。

### 上传、下载和预览

```sh
# 本地 → R2，先预览，再执行备份（保留 R2 独有文件）。
cargo run --features s3 --example cloudflare_r2 -- upload ./source
cargo run --features s3 --example cloudflare_r2 -- upload ./source --apply

# R2 → 本地。
cargo run --features s3 --example cloudflare_r2 -- download ./downloads
cargo run --features s3 --example cloudflare_r2 -- download ./downloads --apply

# 镜像：upload 删除 R2 目标前缀内的多余文件；download 删除本地多余文件。
cargo run --features s3 --example cloudflare_r2 -- upload ./source --mirror
cargo run --features s3 --example cloudflare_r2 -- upload ./source --mirror --apply

# 双向：两端独有文件互相复制；共有文件内容不同则在写入前报冲突。
cargo run --features s3 --example cloudflare_r2 -- upload ./source --bidirectional --apply
```

双向同步不传播删除，不支持空目录同步。分析会下载同大小共有文件计算 BLAKE3；预览后执行会再分析一次，可能产生额外 R2 API 请求和读取流量。实际服务端配额、对象大小和分片能力以 R2 与所用 OpenDAL 版本为准。

### R2 ↔ 其他远程服务、缓存与人工冲突

R2 也可用于已有组合/缓存/性能/冲突示例，把该侧配置为 `s3` 并设置 `REGION=auto`：

```sh
export SRC_KIND=s3 SRC_BUCKET="$R2_BUCKET" SRC_REGION=auto
export SRC_ACCESS_KEY="$R2_ACCESS_KEY_ID" SRC_SECRET_KEY="$R2_SECRET_ACCESS_KEY"
export SRC_ENDPOINT="https://${R2_ACCOUNT_ID}.r2.cloudflarestorage.com"
export SRC_PREFIX=project/
# 特殊 jurisdiction 使用对应端点：export SRC_ENDPOINT="$R2_ENDPOINT"

export DST_KIND=webdav DST_ENDPOINT=https://dav.example.com/dav/
export DST_USERNAME="$WEBDAV_USER" DST_PASSWORD="$WEBDAV_PASSWORD" DST_ROOT=backup/
cargo run --features s3,webdav,services-memory,services-fs --example storage_combinations
CACHE_TTL_SECS=600 cargo run --features s3,webdav,services-memory,services-fs --example cached_preview
```

将另一端也设为 `s3` 即可用于 R2 ↔ R2 或 R2 ↔ AWS S3；源目标根不得重叠。`manual_conflicts` 可用于逐文件保留源/目标/两者/跳过；其配置仍使用 `SRC_*` / `DST_*`，而不是专用示例的 `R2_*`。

参考 Cloudflare 官方文档：[S3 API](https://developers.cloudflare.com/r2/api/s3/api/)、[R2 API token](https://developers.cloudflare.com/r2/api/tokens/)。本示例的构建与命令行检查不访问真实 R2；实际上传下载需要自行提供服务与凭据。
