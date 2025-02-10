// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::collections::HashSet;

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Clone)]
pub struct SimpleLeveledCompactionOptions {
    pub size_ratio_percent: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct SimpleLeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

pub struct SimpleLeveledCompactionController {
    options: SimpleLeveledCompactionOptions,
}

impl SimpleLeveledCompactionController {
    pub fn new(options: SimpleLeveledCompactionOptions) -> Self {
        Self { options }
    }

    /// Generates a compaction task.
    ///
    /// Returns `None` if no compaction needs to be scheduled. The order of SSTs in the compaction task id vector matters.
    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<SimpleLeveledCompactionTask> {
        let mut level_size = vec![0; snapshot.levels.len() + 1];
        level_size[0] = snapshot.l0_sstables.len();
        for (i, (_, sst_ids)) in snapshot.levels.iter().enumerate() {
            level_size[i + 1] = sst_ids.len();
        }

        for level in 0..self.options.max_levels {
            if level == 0
                && snapshot.l0_sstables.len() < self.options.level0_file_num_compaction_trigger
            {
                continue;
            }

            let lower_level = level + 1;
            let size_radio = level_size[lower_level] as f64 / level_size[level] as f64;
            if size_radio < self.options.size_ratio_percent as f64 / 100.0 {
                eprintln!("compaction task: {} -> {}", level, lower_level);

                let upper_level = if level == 0 { None } else { Some(level) };
                let upper_level_sst_ids = if level == 0 {
                    snapshot.l0_sstables.clone()
                } else {
                    snapshot.levels[level - 1].1.clone()
                };
                let lower_level_sst_ids = snapshot.levels[lower_level - 1].1.clone();
                return Some(SimpleLeveledCompactionTask {
                    upper_level,
                    upper_level_sst_ids,
                    lower_level,
                    lower_level_sst_ids,
                    is_lower_level_bottom_level: lower_level >= self.options.max_levels,
                });
            }
        }

        return None;
    }

    /// Apply the compaction result.
    ///
    /// The compactor will call this function with the compaction task and the list of SST ids generated. This function applies the
    /// result and generates a new LSM state. The functions should only change `l0_sstables` and `levels` without changing memtables
    /// and `sstables` hash map. Though there should only be one thread running compaction jobs, you should think about the case
    /// where an L0 SST gets flushed while the compactor generates new SSTs, and with that in mind, you should do some sanity checks
    /// in your implementation.
    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &SimpleLeveledCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut deleted_files = Vec::new();
        if let Some(upper_level) = task.upper_level {
            deleted_files.extend(snapshot.levels[upper_level - 1].1.drain(..));
        } else {
            deleted_files.extend(task.upper_level_sst_ids.iter());

            let mut compacted: HashSet<_> = task.upper_level_sst_ids.iter().copied().collect();
            let new_l0 = snapshot
                .l0_sstables
                .iter()
                .copied()
                .filter(|x| !compacted.remove(x))
                .collect();
            snapshot.l0_sstables = new_l0;
        }

        deleted_files.extend(snapshot.levels[task.lower_level - 1].1.iter());
        snapshot.levels[task.lower_level - 1].1 = output.to_vec();

        (snapshot, deleted_files)
    }
}
