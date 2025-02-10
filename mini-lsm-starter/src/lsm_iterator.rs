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

use std::ops::Bound;

use anyhow::{bail, Result};
use bytes::Bytes;

use crate::{
    iterators::{
        merge_iterator::MergeIterator, two_merge_iterator::TwoMergeIterator, StorageIterator,
    },
    mem_table::MemTableIterator,
    table::SsTableIterator,
};

/// Represents the internal type for an LSM iterator. This type will be changed across the course for multiple times.
// type LsmIteratorInner = MergeIterator<MemTableIterator>;
type LsmIteratorInner =
    TwoMergeIterator<MergeIterator<MemTableIterator>, MergeIterator<SsTableIterator>>;

pub struct LsmIterator {
    inner: LsmIteratorInner,
    upper: Bound<Bytes>,
}

impl LsmIterator {
    pub(crate) fn new(iter: LsmIteratorInner, upper: Bound<Bytes>) -> Result<Self> {
        let mut this = Self { inner: iter, upper };
        this.move_to_next_non_delete()?;
        Ok(this)
    }
}

impl LsmIterator {
    fn move_to_next_non_delete(&mut self) -> Result<()> {
        while self.inner.is_valid() && self.inner.value().is_empty() {
            self.inner.next()?;
        }
        return Ok(());
    }
}

impl StorageIterator for LsmIterator {
    type KeyType<'a> = &'a [u8];

    fn is_valid(&self) -> bool {
        if !self.inner.is_valid() {
            return false;
        }
        match &self.upper {
            Bound::Included(key) => self.inner.key().raw_ref() <= key.as_ref(),
            Bound::Excluded(key) => self.inner.key().raw_ref() < key.as_ref(),
            Bound::Unbounded => true,
        }
    }

    fn key(&self) -> &[u8] {
        // assert!(self.is_valid());
        return self.inner.key().raw_ref();
    }

    fn value(&self) -> &[u8] {
        // assert!(self.is_valid());
        return self.inner.value();
    }

    fn next(&mut self) -> Result<()> {
        // assert!(self.is_valid());
        self.inner.next()?;
        self.move_to_next_non_delete()?;
        return Ok(());
    }

    fn num_active_iterators(&self) -> usize {
        self.inner.num_active_iterators()
    }
}

/// A wrapper around existing iterator, will prevent users from calling `next` when the iterator is
/// invalid. If an iterator is already invalid, `next` does not do anything. If `next` returns an error,
/// `is_valid` should return false, and `next` should always return an error.
pub struct FusedIterator<I: StorageIterator> {
    iter: I,
    has_errored: bool,
}

impl<I: StorageIterator> FusedIterator<I> {
    pub fn new(iter: I) -> Self {
        Self {
            iter,
            has_errored: false,
        }
    }
}

impl<I: StorageIterator> StorageIterator for FusedIterator<I> {
    type KeyType<'a>
        = I::KeyType<'a>
    where
        Self: 'a;

    fn is_valid(&self) -> bool {
        !self.has_errored && self.iter.is_valid()
    }

    fn key(&self) -> Self::KeyType<'_> {
        assert!(self.is_valid());
        return self.iter.key();
    }

    fn value(&self) -> &[u8] {
        assert!(self.is_valid());
        return self.iter.value();
    }

    fn next(&mut self) -> Result<()> {
        // ensure!(self.is_valid());
        if self.has_errored {
            bail!("the iterator is tainted");
        }

        if self.iter.is_valid() {
            if let Err(e) = self.iter.next() {
                self.has_errored = true;
                return Err(e);
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        self.iter.num_active_iterators()
    }
}
