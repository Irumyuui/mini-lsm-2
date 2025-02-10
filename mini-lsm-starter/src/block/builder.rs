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

use bytes::BufMut;

use crate::key::{KeySlice, KeyVec};

use super::Block;

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let prefix_overlap_len = calc_prefix_overlap(self.first_key.as_key_slice(), key);
        let rest_key_len = key.len() - prefix_overlap_len;

        if !self.is_empty()
            && self.current_size() + 2 + 2 + rest_key_len + 2 + value.len() + 2 > self.block_size
        {
            return false;
        }

        let offset = self.data.len();
        self.data.put_u16(prefix_overlap_len as u16);
        self.data.put_u16(rest_key_len as u16);
        self.data
            .extend_from_slice(key.raw_ref()[prefix_overlap_len..].as_ref());

        self.data.put_u16(value.len() as _);
        self.data.extend_from_slice(value);
        self.offsets.push(offset as u16);

        // 每一个块的第一个 key 作为 first_key，完全存储
        if self.first_key.is_empty() {
            self.first_key.set_from_slice(key);
        }

        return true;
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn current_size(&self) -> usize {
        self.data.len()
            + self.offsets.len() * std::mem::size_of::<u16>()
            + std::mem::size_of::<u16>()
    }
}

fn calc_prefix_overlap(first_key: KeySlice, key: KeySlice) -> usize {
    let mut i = 0;
    let max_len = first_key.len().min(key.len());
    while i < max_len {
        if first_key.raw_ref()[i] != key.raw_ref()[i] {
            break;
        }
        i += 1;
    }
    i
}
