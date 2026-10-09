#![doc = include_str!("../docs/API.md")]

mod blocking;
mod config;
/// 同步引擎及计划类型；常用类型也从 crate 根导出。
pub mod core;
mod error;
/// 可扩展的异步存储接口、元数据与内置后端。
pub mod storage;

/// 实现自定义 [`Storage`] 时使用的 async trait 宏。
pub use async_trait::async_trait;
pub use blocking::BlockingSyncEngine;
pub use config::{ConflictPolicy, StorageConfig, SyncConfig, SyncMode};
pub use core::comparator::{ActionSummary, ConflictType, SyncAction};
pub use core::{SyncEngine, SyncFailure, SyncPlan, SyncProgress, SyncReport};
pub use error::{Error, Result};
pub use storage::Storage;
/// 可跨任务/线程克隆的取消令牌；取消不可复位，重跑应创建新令牌。
pub use tokio_util::sync::CancellationToken;

/// Re-export of the exact backend library version used by the adapter.
#[cfg(feature = "opendal")]
pub use opendal;
