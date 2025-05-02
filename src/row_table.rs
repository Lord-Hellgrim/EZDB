use std::collections::{BTreeMap, BTreeSet};

use crate::{db_structure::{DbValue, HeaderItem}, utilities::*};


pub const ZEROES: [u8;4096] = [0u8;4096];

#[derive(Clone, Copy)]
pub struct Pointer {
    inner: usize,
}

pub struct BlockAllocator {
    pub buffer: Vec<u8>,
    pub block_size: usize,
    pub tail: usize,
    pub free_list: Vec<Pointer>,
}

impl BlockAllocator {
    pub fn allocate(&mut self) -> Pointer {

        match self.free_list.pop() {
            Some(index) => return index,
            None => {
                let res = self.tail;
                self.tail += self.block_size;
                return Pointer{inner: res}
            },
        }
    }

    pub fn get_row(&self, pointer: Pointer) -> &[u8] {
        &self.buffer[pointer.inner..pointer.inner + self.block_size]
    }

    pub fn get_row_mut(&mut self, pointer: Pointer) -> &mut [u8] {
        &mut self.buffer[pointer.inner..pointer.inner + self.block_size]
    }

    pub fn free(&mut self, pointer: Pointer) -> Result<(), EzError> {

        if pointer.inner > self.tail {
            return Err(EzError { tag: ErrorTag::Deserialization, text: "Tried to free a pointer beyond a buffer".to_owned() })
        }

        self.free_list.push(pointer);
        let block_size = self.block_size;
        self.get_row_mut(pointer).copy_from_slice(&ZEROES[0..block_size]);

        Ok(())
    }
}

pub struct DbRow<'a> {
    pub header: BTreeSet<HeaderItem>,
    pub data: &'a mut [u8]
}

pub struct RowTable {
    pub name: KeyString,
    pub header: BTreeSet<HeaderItem>,
    pub tree: BTreeMap<DbValue, Pointer>,
    allocator: BlockAllocator,
}

impl RowTable {
    pub fn insert(&mut self, row: &[u8]) -> Result<(), EzError> {

        

        Ok(())
    }
}