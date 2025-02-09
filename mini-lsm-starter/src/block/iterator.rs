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

use bytes::Buf;

use crate::key::{self, KeySlice, KeyVec};

use super::Block;

/// Iterates on a block.
pub struct BlockIterator {
    /// The internal `Block`, wrapped by an `Arc`
    block: Arc<Block>,
    /// The current key, empty represents the iterator is invalid
    key: KeyVec,
    /// the current value range in the block.data, corresponds to the current key
    value_range: (usize, usize),
    /// Current index of the key-value pair, should be in range of [0, num_of_elements)
    idx: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockIterator {
    fn new(block: Arc<Block>) -> Self {
        Self {
            block,
            key: KeyVec::new(),
            value_range: (0, 0),
            idx: 0,
            first_key: KeyVec::new(),
        }
    }

    /// Creates a block iterator and seek to the first entry.
    pub fn create_and_seek_to_first(block: Arc<Block>) -> Self {
        let mut this = Self::new(block);
        this.seek_to_first();
        this
    }

    /// Creates a block iterator and seek to the first key that >= `key`.
    pub fn create_and_seek_to_key(block: Arc<Block>, key: KeySlice) -> Self {
        let mut this = Self::new(block);
        this.seek_to_key(key);
        this
    }

    /// Returns the key of the current entry.
    pub fn key(&self) -> KeySlice {
        self.key.as_key_slice()
    }

    /// Returns the value of the current entry.
    pub fn value(&self) -> &[u8] {
        self.block.data[self.value_range.0..self.value_range.1].as_ref()
    }

    /// Returns true if the iterator is valid.
    /// Note: You may want to make use of `key`
    pub fn is_valid(&self) -> bool {
        !self.key.is_empty()
    }

    /// Seeks to the first key in the block.
    pub fn seek_to_first(&mut self) {
        self.seek_by_index(0);
    }

    /// Move to the next key in the block.
    pub fn next(&mut self) {
        self.seek_by_index(self.idx + 1);
    }

    /// Seek to the first key that >= `key`.
    /// Note: You should assume the key-value pairs in the block are sorted when being added by
    /// callers.
    pub fn seek_to_key(&mut self, key: KeySlice) {
        // self.seek_to_first();
        // while self.is_valid() && self.key().cmp(&key) == std::cmp::Ordering::Less {
        //     self.next();
        // }

        let mut left = 0;
        let mut right = self.block.offsets.len();
        while left < right {
            let mid = left + (right - left) / 2;
            self.seek_by_index(mid);
            assert!(self.is_valid());
            if self.key().cmp(&key) == std::cmp::Ordering::Less {
                left = mid + 1;
            } else {
                right = mid;
            }
        }
        self.seek_by_index(left);
    }

    fn seek_by_index(&mut self, index: usize) -> bool {
        if index >= self.block.offsets.len() {
            self.key.clear();
            self.value_range = (0, 0);
            return false;
        }

        let start = self.block.offsets[index] as usize;
        let mut entry = self.block.data[start..].as_ref();
        let key_len = entry.get_u16() as usize;
        self.key
            .set_from_slice(KeySlice::from_slice(entry[..key_len].as_ref()));
        entry.advance(key_len);
        let value_len = entry.get_u16() as usize;
        self.value_range = (start + 2 + key_len + 2, start + 2 + key_len + 2 + value_len);
        self.idx = index;

        return true;
    }
}
