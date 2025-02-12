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

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact_force_full(
        &self,
        l0_ssts: &Vec<usize>,
        l1_ssts: &Vec<usize>,
    ) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = {
            let guard = self.state.read();
            guard.clone()
        };

        let mut l0_iters = Vec::with_capacity(l0_ssts.len());
        for sst_id in l0_ssts.iter() {
            let table = snapshot.sstables.get(sst_id).unwrap();
            let iter = SsTableIterator::create_and_seek_to_first(table.clone())?;
            l0_iters.push(Box::new(iter));
        }

        let mut l1_iters = Vec::with_capacity(l1_ssts.len());
        for sst_id in l1_ssts.iter() {
            let table = snapshot.sstables.get(sst_id).unwrap();
            let iter = SsTableIterator::create_and_seek_to_first(table.clone())?;
            l1_iters.push(Box::new(iter));
        }

        // l0 first
        let mut iter = TwoMergeIterator::create(
            MergeIterator::create(l0_iters),
            MergeIterator::create(l1_iters),
        )?;

        let mut builder = None;
        let mut new_ssts = Vec::new();

        while iter.is_valid() {
            let key = iter.key();
            let value = iter.value();
            if value.is_empty() {
                iter.next()?;
                continue;
            }

            if builder.is_none() {
                builder.replace(SsTableBuilder::new(self.options.block_size));
            }
            let ref_builder = builder.as_mut().unwrap();
            ref_builder.add(key, value);
            if ref_builder.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let builder = builder.take().unwrap();
                let sst = builder.build(
                    sst_id,
                    self.block_cache.clone().into(),
                    self.path_of_sst(sst_id),
                )?;
                new_ssts.push(Arc::new(sst));
            }

            iter.next()?;
        }

        if let Some(builder) = builder.take() {
            let sst_id = self.next_sst_id();
            let sst = builder.build(
                sst_id,
                self.block_cache.clone().into(),
                self.path_of_sst(sst_id),
            )?;
            new_ssts.push(Arc::new(sst));
        }

        Ok(new_ssts)
    }

    // 这一段我只是直接复制过来的……
    fn compact_generate_sst_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        compact_to_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut builder = None;
        let mut new_sst = Vec::new();

        while iter.is_valid() {
            if builder.is_none() {
                builder = Some(SsTableBuilder::new(self.options.block_size));
            }
            let builder_inner = builder.as_mut().unwrap();
            if compact_to_bottom_level {
                if !iter.value().is_empty() {
                    builder_inner.add(iter.key(), iter.value());
                }
            } else {
                builder_inner.add(iter.key(), iter.value());
            }
            iter.next()?;

            if builder_inner.estimated_size() >= self.options.target_sst_size {
                let sst_id = self.next_sst_id();
                let builder = builder.take().unwrap();
                let sst = Arc::new(builder.build(
                    sst_id,
                    Some(self.block_cache.clone()),
                    self.path_of_sst(sst_id),
                )?);
                new_sst.push(sst);
            }
        }
        if let Some(builder) = builder {
            let sst_id = self.next_sst_id(); // lock dropped here
            let sst = Arc::new(builder.build(
                sst_id,
                Some(self.block_cache.clone()),
                self.path_of_sst(sst_id),
            )?);
            new_sst.push(sst);
        }
        Ok(new_sst)
    }

    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let snapshot = { self.state.read().as_ref().clone() };

        match task {
            CompactionTask::Leveled(t) => {
                if t.upper_level.is_some() {
                    let upper_ssts = t
                        .upper_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect();
                    let lower_ssts = t
                        .lower_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect();
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    return self.compact_generate_sst_from_iter(
                        TwoMergeIterator::create(upper_iter, lower_iter)?,
                        task.compact_to_bottom_level(),
                    );
                } else {
                    let mut upper_iter = Vec::with_capacity(t.upper_level_sst_ids.len());
                    for id in t.upper_level_sst_ids.iter() {
                        let table = snapshot.sstables.get(id).unwrap();
                        let iter = SsTableIterator::create_and_seek_to_first(table.clone())?;
                        upper_iter.push(Box::new(iter));
                    }
                    let lower_ssts = t
                        .lower_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect();
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    let iter =
                        TwoMergeIterator::create(MergeIterator::create(upper_iter), lower_iter)?;
                    return self
                        .compact_generate_sst_from_iter(iter, task.compact_to_bottom_level());
                }
            }
            CompactionTask::Tiered(task) => {
                let mut iters = Vec::with_capacity(task.tiers.len());
                for (_, ids) in task.tiers.iter() {
                    let tables =
                        ids.iter()
                            .map(|id| {
                                snapshot
                                    .sstables
                                    .get(id)
                                    .expect(
                                        format!(
                                        "{} not found, \ntires: {:?}, \nlevel: {:?}, \ntable ids: {:?}",
                                        id, task.tiers, snapshot.levels, snapshot.sstables.keys()
                                    )
                                        .as_str(),
                                    )
                                    .clone()
                            })
                            .collect();
                    let iter = SstConcatIterator::create_and_seek_to_first(tables)?;
                    iters.push(Box::new(iter));
                }

                let iter = MergeIterator::create(iters);
                return self.compact_generate_sst_from_iter(iter, task.bottom_tier_included);
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level,
                lower_level_sst_ids,
                ..
            }) => {
                if upper_level.is_some() {
                    let upper_ssts = upper_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect();
                    let lower_ssts = lower_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect();
                    let upper_iter = SstConcatIterator::create_and_seek_to_first(upper_ssts)?;
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;

                    return self
                        .compact_generate_sst_from_iter(iter, task.compact_to_bottom_level());
                } else {
                    let upper_iters: Result<Vec<Box<_>>> = upper_level_sst_ids
                        .iter()
                        .map(|id| {
                            SsTableIterator::create_and_seek_to_first(
                                snapshot.sstables.get(id).unwrap().clone(),
                            )
                            .map(Box::new)
                        })
                        .collect();
                    let upper_iters = upper_iters?;
                    let upper_iter = MergeIterator::create(upper_iters);

                    let lower_ssts = lower_level_sst_ids
                        .iter()
                        .map(|id| snapshot.sstables.get(id).unwrap().clone())
                        .collect();
                    let lower_iter = SstConcatIterator::create_and_seek_to_first(lower_ssts)?;
                    let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;

                    return self
                        .compact_generate_sst_from_iter(iter, task.compact_to_bottom_level());
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => self.compact_force_full(l0_sstables, l1_sstables),
        }
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let l0_ssts = snapshot.l0_sstables.clone();
        let l1_ssts = snapshot.levels[0].1.clone();
        let task = CompactionTask::ForceFullCompaction {
            l0_sstables: l0_ssts,
            l1_sstables: l1_ssts,
        };

        let compact_ssts = self.compact(&task)?;
        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();

            for del_sst_id in snapshot
                .l0_sstables
                .drain(..)
                .chain(snapshot.levels[0].1.drain(..))
            {
                snapshot.sstables.remove(&del_sst_id);
            }
            for l1_sst in compact_ssts {
                snapshot.levels[0].1.push(l1_sst.sst_id());
                snapshot.sstables.insert(l1_sst.sst_id(), l1_sst);
            }
            *guard = snapshot.into();
        }
        return Ok(());
    }

    fn trigger_compaction(&self) -> Result<()> {
        let snapshot = {
            let guard = self.state.read();
            Arc::clone(&guard)
        };

        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);
        let task = match task {
            Some(task) => task,
            None => return Ok(()),
        };

        // compact
        let sstables = self.compact(&task)?;
        let output: Vec<_> = sstables.iter().map(|x| x.sst_id()).collect();
        {
            let _state_lock = self.state_lock.lock();
            let mut snapshot = { self.state.read().as_ref().clone() };
            for new_sst in sstables {
                let result = snapshot.sstables.insert(new_sst.sst_id(), new_sst);
            }

            let (mut snapshot, deleted_files) = self
                .compaction_controller
                .apply_compaction_result(&snapshot, &task, &output, false);

            for del_sst_id in deleted_files {
                let result = snapshot.sstables.remove(&del_sst_id);
                std::fs::remove_file(self.path_of_sst(del_sst_id))?;
            }

            let mut guard = self.state.write();
            *guard = snapshot.into();
        }

        Ok(())
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let is_limited =
            { self.state.read().imm_memtables.len() >= self.options.num_memtable_limit };
        if is_limited {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }
}
