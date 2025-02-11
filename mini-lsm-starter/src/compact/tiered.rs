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

use std::{
    collections::HashMap,
    ops::{Add, Div},
};

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        if snapshot.levels.len() < self.options.num_tiers {
            return None;
        }

        // 全部合并
        let level_len = snapshot.levels.len();
        let last_level_size = snapshot.levels.last().unwrap().1.len();
        let mut size = 0;
        for (_, level) in snapshot.levels.iter().take(level_len - 1) {
            size += level.len();
        }

        let radio = size as f64 / last_level_size as f64 * 100.;
        if radio >= self.options.size_ratio as f64 {
            return Some(TieredCompactionTask {
                tiers: snapshot.levels.clone(),
                bottom_tier_included: true,
            });
        }

        // 在 min_merge_width 之后才计划合并
        let mut size = 0;
        for (i, (_, level)) in snapshot.levels.iter().enumerate() {
            let tier_len = level.len();
            if i + 1 < self.options.min_merge_width {
                size += tier_len;
                continue;
            }

            assert_ne!(tier_len, 0);
            let radio = size as f64 / tier_len as f64;
            if radio >= (self.options.size_ratio as f64).add(100.).div(100.) {
                return Some(TieredCompactionTask {
                    tiers: snapshot.levels.iter().take(i + 1).cloned().collect(),
                    bottom_tier_included: i + 1 == level_len,
                });
            }

            size += tier_len;
        }

        // 维持 num_tiers 合并
        if snapshot.levels.len() == self.options.num_tiers {
            return None;
        }
        let nums = snapshot.levels.len() + 2 - self.options.num_tiers;
        Some(TieredCompactionTask {
            tiers: snapshot.levels.iter().take(nums).cloned().collect(),
            bottom_tier_included: nums == level_len,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();

        let mut tier_to_remove: HashMap<_, _> = task.tiers.iter().map(|(k, v)| (*k, v)).collect();
        let mut levels = Vec::new();
        let mut new_tier_added = false;
        let mut file_to_remove = Vec::new();
        for (tier_id, files) in snapshot.levels.iter() {
            if let Some(fls) = tier_to_remove.remove(tier_id) {
                file_to_remove.extend(fls.iter().copied());
            } else {
                levels.push((*tier_id, files.clone()));
            }

            if tier_to_remove.is_empty() && !new_tier_added {
                new_tier_added = true;
                levels.push((output[0], output.to_vec()));
            }
        }

        snapshot.levels = levels;

        (snapshot, file_to_remove)
    }
}
