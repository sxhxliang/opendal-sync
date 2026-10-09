use crate::SyncMode;
use crate::storage::FileInfo;
use std::collections::{BTreeSet, HashMap};

/// 同步动作
#[derive(Debug, Clone)]
pub enum SyncAction {
    /// 复制文件：源路径 -> 目标路径
    Copy {
        /// 读取端相对路径；reverse 为 true 时读取引擎目标端。
        source_path: String,
        /// 写入端相对路径；reverse 为 true 时写入引擎源端。
        dest_path: String,
        /// 分析时文件大小，单位字节。
        size: u64,
        /// 是否是从目标复制到源（双向同步时）
        reverse: bool,
    },
    /// 删除文件
    Delete {
        /// 相对于相应存储根的文件路径。
        path: String,
        /// 删除目标还是源
        from_dest: bool,
    },
    /// 不执行写入：内容相同、保留目标独有文件，或按策略跳过冲突。
    Skip {
        /// 被跳过文件的相对路径。
        path: String,
    },
    /// 冲突（需要用户决定）
    Conflict {
        /// 相对于相应存储根的文件路径。
        path: String,
        /// 引擎源端的文件信息；当前生成的冲突为 Some。
        source_info: Option<FileInfo>,
        /// 引擎目标端的文件信息；当前生成的冲突为 Some。
        dest_info: Option<FileInfo>,
        /// 冲突分类；当前引擎仅生成 `BothModified`。
        conflict_type: ConflictType,
    },
}

/// 冲突类型
#[derive(Debug, Clone)]
pub enum ConflictType {
    /// 两端内容不同；当前引擎只生成此变体，不能证明双方都发生过修改。
    BothModified,
    /// 保留变体：大小相同但时间不同；当前引擎不生成。
    SameSizeDifferentTime,
    /// 保留变体：一边修改一边删除；当前引擎不生成。
    ModifiedVsDeleted,
}

/// Compares fully scanned file trees with verified content checksums.
pub(crate) struct FileComparator;
impl FileComparator {
    pub(crate) fn compare_trees(
        source: &HashMap<String, FileInfo>,
        destination: &HashMap<String, FileInfo>,
        mode: SyncMode,
    ) -> Vec<SyncAction> {
        let paths: BTreeSet<_> = source.keys().chain(destination.keys()).collect();
        paths
            .into_iter()
            .filter_map(|path| {
                let left = source.get(path).filter(|file| !file.is_dir);
                let right = destination.get(path).filter(|file| !file.is_dir);
                let copy = |file: &FileInfo, reverse| SyncAction::Copy {
                    source_path: path.clone(),
                    dest_path: path.clone(),
                    size: file.size,
                    reverse,
                };
                Some(match (left, right) {
                    (Some(a), Some(b))
                        if a.size == b.size && a.checksum.is_some() && a.checksum == b.checksum =>
                    {
                        SyncAction::Skip { path: path.clone() }
                    }
                    (Some(a), Some(b)) if mode == SyncMode::Bidirectional => SyncAction::Conflict {
                        path: path.clone(),
                        source_info: Some(a.clone()),
                        dest_info: Some(b.clone()),
                        conflict_type: ConflictType::BothModified,
                    },
                    (Some(a), _) => copy(a, false),
                    (None, Some(b)) => match mode {
                        SyncMode::Bidirectional => copy(b, true),
                        SyncMode::Mirror => SyncAction::Delete {
                            path: path.clone(),
                            from_dest: true,
                        },
                        SyncMode::Backup => SyncAction::Skip { path: path.clone() },
                    },
                    (None, None) => return None,
                })
            })
            .collect()
    }
    pub(crate) fn summarize_actions(actions: &[SyncAction]) -> ActionSummary {
        let mut summary = ActionSummary::default();
        for action in actions {
            match action {
                SyncAction::Copy {
                    size,
                    reverse: true,
                    ..
                } => {
                    summary.reverse_copy_count += 1;
                    summary.reverse_copy_bytes += size;
                }
                SyncAction::Copy {
                    size,
                    reverse: false,
                    ..
                } => {
                    summary.copy_count += 1;
                    summary.copy_bytes += size;
                }
                SyncAction::Delete { .. } => summary.delete_count += 1,
                SyncAction::Skip { .. } => summary.skip_count += 1,
                SyncAction::Conflict { .. } => summary.conflict_count += 1,
            }
        }
        summary
    }
}

/// 动作统计
#[derive(Debug, Clone, Default)]
pub struct ActionSummary {
    /// 正向复制操作数。
    pub copy_count: usize,
    /// 正向计划复制字节数。
    pub copy_bytes: u64,
    /// 反向复制操作数。
    pub reverse_copy_count: usize,
    /// 反向计划复制字节数。
    pub reverse_copy_bytes: u64,
    /// 删除操作数。
    pub delete_count: usize,
    /// 跳过操作数，包含策略跳过的冲突。
    pub skip_count: usize,
    /// 未解决冲突数。
    pub conflict_count: usize,
}

impl ActionSummary {
    /// 全部动作数，包含两个方向的复制、删除、跳过及未解决冲突。
    #[must_use]
    pub fn total_files(&self) -> usize {
        self.copy_count
            + self.reverse_copy_count
            + self.delete_count
            + self.skip_count
            + self.conflict_count
    }

    /// 两个方向预计复制字节数之和，不含校验读取流量。
    #[must_use]
    pub fn total_transfer_bytes(&self) -> u64 {
        self.copy_bytes + self.reverse_copy_bytes
    }
}
