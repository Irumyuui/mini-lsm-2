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

use std::cmp::{self};
use std::collections::binary_heap::PeekMut;
use std::collections::BinaryHeap;

use anyhow::Result;

use crate::key::KeySlice;

use super::StorageIterator;

struct HeapWrapper<I: StorageIterator>(pub usize, pub Box<I>);

impl<I: StorageIterator> PartialEq for HeapWrapper<I> {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == cmp::Ordering::Equal
    }
}

impl<I: StorageIterator> Eq for HeapWrapper<I> {}

impl<I: StorageIterator> PartialOrd for HeapWrapper<I> {
    fn partial_cmp(&self, other: &Self) -> Option<cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl<I: StorageIterator> Ord for HeapWrapper<I> {
    fn cmp(&self, other: &Self) -> cmp::Ordering {
        self.1
            .key()
            .cmp(&other.1.key())
            .then(self.0.cmp(&other.0))
            .reverse()
    }
}

/// Merge multiple iterators of the same type. If the same key occurs multiple times in some
/// iterators, prefer the one with smaller index.
pub struct MergeIterator<I: StorageIterator> {
    iters: BinaryHeap<HeapWrapper<I>>,
    current: Option<HeapWrapper<I>>,
}

impl<I: StorageIterator> MergeIterator<I> {
    pub fn create(iters: Vec<Box<I>>) -> Self {
        let mut iters = BinaryHeap::from_iter(
            iters
                .into_iter()
                .enumerate()
                .filter(|(_, iter)| iter.is_valid())
                .map(|(i, iter)| HeapWrapper(i, iter)),
        );
        let current = iters.pop();
        return Self { iters, current };
    }
}

impl<I> StorageIterator for MergeIterator<I>
where
    I: 'static + for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
{
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        return self.current.as_ref().unwrap().1.key();
    }

    fn value(&self) -> &[u8] {
        return self.current.as_ref().unwrap().1.value();
    }

    fn is_valid(&self) -> bool {
        return self.current.as_ref().is_some_and(|iter| iter.1.is_valid());
    }

    fn next(&mut self) -> Result<()> {
        let mut current = self.current.take().unwrap();
        while let Some(mut peek_iter) = self.iters.peek_mut() {
            if peek_iter.1.key() != current.1.key() {
                break;
            }

            if let Err(e) = peek_iter.1.next() {
                PeekMut::pop(peek_iter);
                return Err(e);
            }
            if !peek_iter.1.is_valid() {
                PeekMut::pop(peek_iter);
            }
        }

        current.1.next()?;

        if !current.1.is_valid() {
            self.current = self.iters.pop();
            return Ok(());
        }

        let new_current = if let Some(next_iter) = self.iters.peek_mut() {
            if current < *next_iter {
                Some(PeekMut::pop(next_iter))
            } else {
                None
            }
        } else {
            None
        };
        if let Some(new_current) = new_current {
            self.iters.push(current);
            self.current = Some(new_current);
        } else {
            self.current = Some(current);
        }

        Ok(())
    }
}
