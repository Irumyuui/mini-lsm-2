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

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        sst_ids: &[usize],
        in_level: usize,
    ) -> Vec<usize> {
        // // 获取待合并的最大范围
        // let first_key = sst_ids
        //     .iter()
        //     .map(|id| snapshot.sstables.get(id).unwrap().first_key())
        //     .min()
        //     .unwrap()
        //     .clone();
        // let last_key = sst_ids
        //     .iter()
        //     .map(|id| snapshot.sstables.get(id).unwrap().last_key())
        //     .max()
        //     .unwrap()
        //     .clone();

        // let mut overlap_ssts = Vec::new();
        // for sst_id in snapshot.levels[in_level].1.iter() {
        //     let table = snapshot.sstables.get(sst_id).unwrap();
        //     if !(table.last_key() < &first_key || table.first_key() > &last_key) {
        //         overlap_ssts.push(sst_id.clone());
        //     }
        // }

        // overlap_ssts

        // 蛤？

        let begin_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].first_key())
            .min()
            .cloned()
            .unwrap();
        let end_key = sst_ids
            .iter()
            .map(|id| snapshot.sstables[id].last_key())
            .max()
            .cloned()
            .unwrap();
        let mut overlap_ssts = Vec::new();
        for sst_id in &snapshot.levels[in_level - 1].1 {
            let sst = &snapshot.sstables[sst_id];
            let first_key = sst.first_key();
            let last_key = sst.last_key();
            if !(last_key < &begin_key || first_key > &end_key) {
                overlap_ssts.push(*sst_id);
            }
        }
        overlap_ssts
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        // let opt_max_levels = self.options.max_levels;

        // let mut level_size = Vec::with_capacity(opt_max_levels);
        // for i in 0..self.options.max_levels {
        //     let size = snapshot.levels[i]
        //         .1
        //         .iter()
        //         .map(|id| snapshot.sstables.get(id).unwrap().table_size())
        //         .sum::<u64>() as usize;
        //     level_size.push(size);
        // }
        // let level_size = level_size; // to immut

        // let opt_base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        // let mut target_level_size = vec![0_usize; opt_max_levels];
        // target_level_size[opt_max_levels - 1] =
        //     level_size[opt_max_levels - 1].max(opt_base_level_size_bytes);
        // let mut base_level = 0;
        // for i in (0..opt_max_levels - 1).rev() {
        //     let next_level_size = target_level_size[i + 1];
        //     let current_level_size = next_level_size / self.options.level_size_multiplier;
        //     if next_level_size > opt_base_level_size_bytes {
        //         target_level_size[i] = current_level_size;
        //     }
        //     if current_level_size > 0 {
        //         base_level = i + 1;
        //     }
        // }
        // let target_level_size = target_level_size;
        // let base_level = base_level; // to immut

        // // L0 个数多余 level0_file_num_compaction_trigger ，直接合并到 base_level
        // if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
        //     let task = LeveledCompactionTask {
        //         upper_level: None,
        //         upper_level_sst_ids: snapshot.l0_sstables.clone(),
        //         lower_level: base_level,
        //         lower_level_sst_ids: self.find_overlapping_ssts(
        //             snapshot,
        //             &snapshot.l0_sstables,
        //             base_level,
        //         ),
        //         is_lower_level_bottom_level: base_level == self.options.max_levels,
        //     };
        //     return Some(task);
        // }

        // // 查找最高优先级的 level
        // let mut priorities = Vec::with_capacity(self.options.max_levels);
        // for level in 0..self.options.max_levels {
        //     if target_level_size[level] == 0 {
        //         continue; // ?
        //     }
        //     let priority = level_size[level] as f64 / target_level_size[level] as f64;
        //     if priority > 1.0 {
        //         priorities.push((priority, level + 1));
        //     }
        // }
        // priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());

        // let high_prio = priorities.first();
        // // eprintln!("high_prio: {:?}", high_prio);
        // if let Some((_, level)) = high_prio {
        //     let level = *level;
        //     // 找一个最久的
        //     let chosed_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap();
        //     println!("level: {level}, chosed_sst: {chosed_sst}, prio: {priorities:?}",);

        //     let task = LeveledCompactionTask {
        //         upper_level: Some(level),
        //         upper_level_sst_ids: vec![chosed_sst],
        //         lower_level: level + 1,
        //         lower_level_sst_ids: self.find_overlapping_ssts(snapshot, &[chosed_sst], level + 1),
        //         is_lower_level_bottom_level: level + 1 == self.options.max_levels,
        //     };
        //     return Some(task);
        // }

        // None

        // wtf?

        // step 1: compute target level size
        let mut target_level_size = (0..self.options.max_levels).map(|_| 0).collect::<Vec<_>>(); // exclude level 0
        let mut real_level_size = Vec::with_capacity(self.options.max_levels);
        let mut base_level = self.options.max_levels;
        for i in 0..self.options.max_levels {
            real_level_size.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>() as usize,
            );
        }
        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;

        // select base level and compute target level size
        target_level_size[self.options.max_levels - 1] =
            real_level_size[self.options.max_levels - 1].max(base_level_size_bytes);
        for i in (0..(self.options.max_levels - 1)).rev() {
            let next_level_size = target_level_size[i + 1];
            let this_level_size = next_level_size / self.options.level_size_multiplier;
            if next_level_size > base_level_size_bytes {
                target_level_size[i] = this_level_size;
            }
            if target_level_size[i] > 0 {
                base_level = i + 1;
            }
        }

        // Flush L0 SST is the top priority
        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids: snapshot.l0_sstables.clone(),
                lower_level: base_level,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &snapshot.l0_sstables,
                    base_level,
                ),
                is_lower_level_bottom_level: base_level == self.options.max_levels,
            });
        }

        let mut priorities = Vec::with_capacity(self.options.max_levels);
        for level in 0..self.options.max_levels {
            let prio = real_level_size[level] as f64 / target_level_size[level] as f64;
            if prio > 1.0 {
                priorities.push((prio, level + 1));
            }
        }
        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());
        let priority = priorities.first();
        if let Some((_, level)) = priority {
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
                target_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                real_level_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
            );

            let level = *level;
            let selected_sst = snapshot.levels[level - 1].1.iter().min().copied().unwrap(); // select the oldest sst to compact
            println!(
                        "compaction triggered by priority: {level} out of {:?}, select {selected_sst} for compaction",
                        priorities
                    );
            return Some(LeveledCompactionTask {
                upper_level: Some(level),
                upper_level_sst_ids: vec![selected_sst],
                lower_level: level + 1,
                lower_level_sst_ids: self.find_overlapping_ssts(
                    snapshot,
                    &[selected_sst],
                    level + 1,
                ),
                is_lower_level_bottom_level: level + 1 == self.options.max_levels,
            });
        }
        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let mut deleted_files = Vec::new();
        let mut upper_level_sst_ids_set = task
            .upper_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();
        let mut lower_level_sst_ids_set = task
            .lower_level_sst_ids
            .iter()
            .copied()
            .collect::<HashSet<_>>();

        if let Some(upper_level) = task.upper_level {
            let new_upper_level_ssts = snapshot.levels[upper_level - 1]
                .1
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        } else {
            let new_l0_ssts = snapshot
                .l0_sstables
                .iter()
                .filter_map(|x| {
                    if upper_level_sst_ids_set.remove(x) {
                        return None;
                    }
                    Some(*x)
                })
                .collect::<Vec<_>>();
            assert!(upper_level_sst_ids_set.is_empty());
            snapshot.l0_sstables = new_l0_ssts;
        }

        deleted_files.extend_from_slice(&task.upper_level_sst_ids);
        deleted_files.extend_from_slice(&task.lower_level_sst_ids);

        let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
            .1
            .iter()
            .filter_map(|x| match lower_level_sst_ids_set.remove(x) {
                true => None,
                false => Some(*x),
            })
            .collect::<Vec<_>>();
        new_lower_level_ssts.extend(output);

        if !in_recovery {
            new_lower_level_ssts.sort_by(|x, y| {
                snapshot
                    .sstables
                    .get(x)
                    .unwrap()
                    .first_key()
                    .cmp(snapshot.sstables.get(y).unwrap().first_key())
            });
        }

        snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;
        (snapshot, deleted_files)

        // let mut snapshot = snapshot.clone();
        // let mut files_to_remove = Vec::new();
        // let mut upper_level_sst_ids_set = task
        //     .upper_level_sst_ids
        //     .iter()
        //     .copied()
        //     .collect::<HashSet<_>>();
        // let mut lower_level_sst_ids_set = task
        //     .lower_level_sst_ids
        //     .iter()
        //     .copied()
        //     .collect::<HashSet<_>>();
        // if let Some(upper_level) = task.upper_level {
        //     let new_upper_level_ssts = snapshot.levels[upper_level - 1]
        //         .1
        //         .iter()
        //         .filter_map(|x| {
        //             if upper_level_sst_ids_set.remove(x) {
        //                 return None;
        //             }
        //             Some(*x)
        //         })
        //         .collect::<Vec<_>>();
        //     assert!(upper_level_sst_ids_set.is_empty());
        //     snapshot.levels[upper_level - 1].1 = new_upper_level_ssts;
        // } else {
        //     let new_l0_ssts = snapshot
        //         .l0_sstables
        //         .iter()
        //         .filter_map(|x| {
        //             if upper_level_sst_ids_set.remove(x) {
        //                 return None;
        //             }
        //             Some(*x)
        //         })
        //         .collect::<Vec<_>>();
        //     assert!(upper_level_sst_ids_set.is_empty());
        //     snapshot.l0_sstables = new_l0_ssts;
        // }

        // files_to_remove.extend(&task.upper_level_sst_ids);
        // files_to_remove.extend(&task.lower_level_sst_ids);

        // let mut new_lower_level_ssts = snapshot.levels[task.lower_level - 1]
        //     .1
        //     .iter()
        //     .filter_map(|x| {
        //         if lower_level_sst_ids_set.remove(x) {
        //             return None;
        //         }
        //         Some(*x)
        //     })
        //     .collect::<Vec<_>>();
        // assert!(lower_level_sst_ids_set.is_empty());
        // new_lower_level_ssts.extend(output);
        // // Don't sort the SST IDs during recovery because actual SSTs are not loaded at that point
        // if !in_recovery {
        //     new_lower_level_ssts.sort_by(|x, y| {
        //         snapshot
        //             .sstables
        //             .get(x)
        //             .unwrap()
        //             .first_key()
        //             .cmp(snapshot.sstables.get(y).unwrap().first_key())
        //     });
        // }
        // snapshot.levels[task.lower_level - 1].1 = new_lower_level_ssts;
        // (snapshot, files_to_remove)
    }
}
