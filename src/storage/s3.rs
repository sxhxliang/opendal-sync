use super::{FileInfo, FileMeta, IO_TIMEOUT_SECS, OP_TIMEOUT_SECS, Storage};
use anyhow::Result;
use async_trait::async_trait;
use futures::TryStreamExt;
use opendal::{Operator, layers::TimeoutLayer};
use std::time::Duration;

/// 通过 `OpenDAL` 访问的远程存储；流式写入缓冲和原子性由后端决定。
///
/// 使用 [`crate::Storage`] 的方法进行 I/O；构造不发送验证请求。
pub struct S3Storage {
    operator: Operator,
    name: String,
}

impl S3Storage {
    /// 构造 S3 后端，不创建存储桶或发送连接验证请求。
    ///
    /// bucket/region 为存储桶及区域，`access_key`/`secret_key` 为显式凭据。
    /// endpoint 为可选兼容服务 URL，prefix 为桶内同步根；两者 None 使用默认值。
    /// 调用方法需要 Tokio runtime。网络/权限错误在 I/O 时返回。
    ///
    /// 启用 HTTP 传输 feature 时惰性安装默认传输，应用已安装的传输优先。
    ///
    /// # Errors
    /// 返回无效端点或其他后端配置错误。
    pub fn new(
        bucket: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        endpoint: Option<&str>,
        prefix: Option<&str>,
    ) -> Result<Self> {
        use opendal::services::S3;
        super::init_http_transport();

        let mut builder = S3::default()
            .bucket(bucket)
            .region(region)
            .access_key_id(access_key)
            .secret_access_key(secret_key);

        if let Some(ep) = endpoint {
            builder = builder.endpoint(ep);
        }

        if let Some(p) = prefix {
            builder = builder.root(p);
        }

        // 添加超时层
        let operator = Operator::new(builder)?.layer(
            TimeoutLayer::default()
                .with_timeout(Duration::from_secs(OP_TIMEOUT_SECS))
                .with_io_timeout(Duration::from_secs(IO_TIMEOUT_SECS)),
        );

        let name = format!(
            "s3://{}{}",
            bucket,
            prefix.map(|p| format!("/{p}")).unwrap_or_default()
        );

        Ok(Self { operator, name })
    }
}

#[async_trait]
impl Storage for S3Storage {
    async fn list_files(&self, prefix: Option<&str>) -> Result<Vec<FileInfo>> {
        let mut files = Vec::new();
        let path = prefix.unwrap_or("");

        // 使用 lister_with 进行递归列表
        let mut lister = self.operator.lister_with(path).recursive(true).await?;

        while let Some(entry) = lister.try_next().await? {
            let path_str = entry.path().to_string();

            // 跳过根目录
            if path_str.is_empty() || path_str == "/" {
                continue;
            }

            let meta = entry.metadata();

            files.push(FileInfo {
                path: path_str.trim_start_matches('/').to_string(),
                size: meta.content_length(),
                modified_time: meta
                    .last_modified()
                    .map_or(0, |t| t.into_inner().as_second()),
                is_dir: meta.is_dir(),
                checksum: meta.etag().map(|s| s.trim_matches('"').to_string()),
            });
        }

        Ok(files)
    }

    async fn stat(&self, path: &str) -> Result<Option<FileMeta>> {
        match self.operator.stat(path).await {
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
        let data = self.operator.read(path).await?;
        Ok(data.to_vec())
    }

    async fn read_range(&self, path: &str, offset: u64, length: u64) -> Result<Vec<u8>> {
        let data = self
            .operator
            .read_with(path)
            .range(offset..offset + length)
            .await?;
        Ok(data.to_vec())
    }

    async fn write(&self, path: &str, data: Vec<u8>) -> Result<()> {
        self.operator.write(path, data).await?;
        Ok(())
    }

    async fn write_stream<'stream>(
        &self,
        path: &str,
        mut stream: std::pin::Pin<
            Box<dyn futures::Stream<Item = Result<Vec<u8>>> + Send + 'stream>,
        >,
        total_size: Option<u64>,
    ) -> Result<()> {
        use futures::StreamExt;
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
        // S3 删除不存在的文件不会报错
        self.operator.delete(path).await?;
        Ok(())
    }

    async fn create_dir(&self, path: &str) -> Result<()> {
        // S3 不需要真正创建目录，但为了兼容性，创建一个占位对象
        let dir_path = if path.ends_with('/') {
            path.to_string()
        } else {
            format!("{path}/")
        };
        self.operator.write(&dir_path, Vec::<u8>::new()).await?;
        Ok(())
    }

    fn name(&self) -> &str {
        &self.name
    }
}
