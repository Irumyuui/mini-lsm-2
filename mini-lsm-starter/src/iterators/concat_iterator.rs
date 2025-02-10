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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let (current, next_sst_idx) = match sstables.first() {
            Some(table) => {
                let iter = SsTableIterator::create_and_seek_to_first(table.clone())?;
                (Some(iter), 1)
            }
            None => (None, 0),
        };

        let mut this = Self {
            current,
            next_sst_idx: next_sst_idx.min(sstables.len()),
            sstables,
        };
        this.to_next_sst_iter_until_valid()?;

        Ok(this)
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let idx = sstables
            .partition_point(|table| table.first_key().as_key_slice() <= key)
            .saturating_sub(1);

        let current = match sstables.get(idx) {
            Some(table) => Some(SsTableIterator::create_and_seek_to_key(table.clone(), key)?),
            None => None,
        };

        let mut this = Self {
            current,
            next_sst_idx: (idx + 1).min(sstables.len()),
            sstables,
        };
        this.to_next_sst_iter_until_valid()?;

        Ok(this)
    }

    fn to_next_sst_iter_until_valid(&mut self) -> Result<()> {
        while let Some(iter) = self.current.as_mut() {
            if iter.is_valid() {
                break;
            }
            if self.next_sst_idx >= self.sstables.len() {
                self.current = None;
                break;
            }
            self.current
                .replace(SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?);
            self.next_sst_idx += 1;
        }

        Ok(())
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        assert!(
            self.current.as_ref().is_none()
                || self.current.as_ref().is_some_and(|iter| iter.is_valid())
        );

        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        self.to_next_sst_iter_until_valid()?;
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
