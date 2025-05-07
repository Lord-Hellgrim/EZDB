use std::alloc::{alloc, dealloc, Layout};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::hash::Hash;
use std::io::Write;
use std::ops::Index;

use fnv::{FnvBuildHasher, FnvHashSet, FnvHasher};

use crate::db_structure::{DbKey, DbType};
use crate::{db_structure::{DbValue, HeaderItem}, utilities::*};


pub const ZEROES: [u8;4096] = [0u8;4096];
pub const CHUNK_SIZE: usize = 4096;



pub fn pop_from_hashset<T: Eq + Hash + Clone>(set: &mut FnvHashSet<T>) -> Option<T> {
    let result = match set.iter().next() {
        Some(item) => item,
        None => return None,
    };
    let key = result.clone();

    set.take(&key)
}

pub fn pointer_add(pointer: *mut u8, offset: usize) -> *mut u8 {
    let result = pointer.clone();
    unsafe { result.add(offset) }
}

pub fn check_pointer_safety(pointer: *mut u8) {
    if pointer.is_null() {
        panic!("Got a NULL pointer from the OS. Either out of memory or some other unrecoverable error");
    } else if usize::MAX - (pointer as usize) < 4096 {
        panic!("Pointer from OS is only a page away from overflowing");
    } else {
        ()
    }
}

pub struct Slice {
    pub pointer: *mut u8,
    pub len: usize,
}

impl Slice {
    pub fn offset(&self, offset: usize) -> Result<*mut u8, EzError> {
        if offset >= self.len {
            return Err(EzError { tag: ErrorTag::Structure, text: format!("Attempting out of bounds access. Base pointer - offest: {} - {}", self.pointer as usize, offset) })
        }

        return unsafe { Ok(self.pointer.add(offset)) }
    }
}

pub struct BlockAllocator {
    pub chunks: Vec<*mut u8>,
    pub current_chunk: usize,
    pub current_offset: usize,
    pub block_size: usize,
    pub free_list: FnvHashSet<*mut u8>,
    alloc_count: usize,
}

impl BlockAllocator {
    pub fn new(block_size: usize) -> Result<BlockAllocator, EzError> {

        if block_size % 64 != 0 {
            return Err(EzError { tag: ErrorTag::Structure, text: format!("Improper block size. Must be multiple of 64. Received: {}", block_size) })
        }

        let layout = Layout::from_size_align(block_size * 64, 64)
            .expect(&format!("Must have passed a monstrous block_size.\nBlock_size passed: {}", block_size));

        let start = unsafe { alloc(layout) };
        check_pointer_safety(start);

        Ok(
            BlockAllocator {
                chunks: vec!(start),
                current_chunk: 0,
                current_offset: 0,
                block_size,
                free_list: FnvHashSet::with_hasher(FnvBuildHasher::new()),
                alloc_count: 0,
            }
        )
    }

    pub fn alloc(&mut self) -> Slice {

        self.alloc_count += 1;
        let result: Slice;
        match pop_from_hashset(&mut self.free_list) {
            Some(pointer) => return Slice{pointer, len: self.block_size},
            None => {
                if self.current_chunk == self.chunks.len()-1 && self.block_size + self.current_offset == 64*self.block_size {
                    let l = self.chunks.len();
                    for _ in 0..l {
                        let layout = Layout::from_size_align(self.block_size * 64, 64)
                        .expect(&format!("Must have passed a monstrous block_size.\nBlock_size passed: {}", self.block_size));
    
                        let new_chunk = unsafe { alloc(layout) };
                        check_pointer_safety(new_chunk);
                        self.chunks.push(new_chunk);
                    }
                    let tail = pointer_add(self.chunks[self.current_chunk], self.current_offset);
                    self.current_offset = 0;
                    self.current_chunk += 1;
                    result = Slice{pointer: tail, len: self.block_size};
                } else if self.current_offset + self.block_size == 64*self.block_size {
                    let tail = pointer_add(self.chunks[self.current_chunk], self.current_offset);
                    self.current_chunk += 1;
                    self.current_offset = 0;
                    result = Slice{pointer: tail, len: self.block_size};
                } else {
                    let tail = pointer_add(self.chunks[self.current_chunk], self.current_offset);
                    self.current_offset += self.block_size;
                    result = Slice{pointer: tail, len: self.block_size}
                }
                result
            },
        }
    }

    pub fn free(&mut self, slice: Slice) -> Result<(), EzError> {

        match self.free_list.insert(slice.pointer) {
            true => (),
            false => return Err(EzError { tag: ErrorTag::Structure, text: format!("Attempting to double free a pointer. Pointer address: {}", slice.pointer as usize) }),
        }
        unsafe { slice.pointer.write_bytes(0, self.block_size) };

        Ok(())
    }

}

impl Drop for BlockAllocator {
    fn drop(&mut self) {
        let layout = Layout::from_size_align(self.block_size * 64, 64).unwrap();
        for pointer in &self.chunks {
            unsafe { dealloc(*pointer, layout) };
        }
    }
}

pub struct Hallocator {
    buffer: Vec<u8>,
    block_size: usize,
    tail: usize,
    free_list: FnvHashSet<usize>,
}

impl Hallocator {
    pub fn new(block_size: usize) -> Hallocator {
        Hallocator {
            buffer: Vec::with_capacity(block_size * 64),
            block_size,
            tail: 0,
            free_list: FnvHashSet::default(),
        }
    }

    pub fn alloc(&mut self) -> usize {
        
        match pop_from_hashset(&mut self.free_list) {
            Some(pointer) => {
                pointer
            },
            None => {
                let result = self.tail;
                self.buffer.extend_from_slice(&ZEROES[0..self.block_size]);
                self.tail += self.block_size;
                result
            },
        }
    }

    pub fn free(&mut self, pointer: usize) -> Result<(), EzError> {
        match self.free_list.insert(pointer) {
            true => (),
            false => return Err(EzError { tag: ErrorTag::Structure, text: format!("Attempting to double free a pointer. Pointer address: {}", pointer as usize) }),
        }
        let row_pointer = &self.buffer[pointer..pointer + self.block_size].as_mut_ptr();
        unsafe { row_pointer.write_bytes(0, self.block_size) };

        Ok(())
    }

    #[inline]
    pub fn get_block(&self, pointer: usize) -> &[u8] {
        &self.buffer[pointer..pointer+self.block_size]
    }

    #[inline]
    pub fn get_block_mut(&mut self, pointer: usize) -> &mut [u8] {
        &mut self.buffer[pointer..pointer+self.block_size]
    }

    #[inline]
    pub fn read_i32(&self, pointer: usize, offset: usize) -> i32 {
        if offset > self.block_size - 4 {
            panic!("Trying to read out of bounds memory")
        }
        unsafe { *(self.get_block(pointer+offset).as_ptr() as *const i32) }
    }

    #[inline]
    pub fn read_u64(&self, pointer: usize, offset: usize) -> u64 {
        // if offset > self.block_size - 8 {
        //     panic!("Trying to read out of bounds memory")
        // }
        unsafe { *(self.get_block(pointer+offset).as_ptr() as *const u64) }
    }

    #[inline]
    pub fn read_f32(&self, pointer: usize, offset: usize) -> f32 {
        if offset > self.block_size - 4 {
            panic!("Trying to read out of bounds memory")
        }
        unsafe { *(self.get_block(pointer+offset).as_ptr() as *const f32) }
    }

    #[inline]
    pub fn read_keystring(&self, pointer: usize, offset: usize) -> KeyString {
        if offset > self.block_size - 64 {
            panic!("Trying to read out of bounds memory")
        }
        unsafe { *(self.get_block(pointer+offset).as_ptr() as *const KeyString) }
    }

    #[inline]
    pub fn write_i32(&mut self, pointer: usize, offset: usize, value: i32) {
        if offset > self.block_size - 4 {
            panic!("Trying to write out of bounds memory")
        }
        unsafe { (self.get_block_mut(pointer+offset).as_mut_ptr() as *mut i32).write(value) }
    }

    #[inline]
    pub fn write_u64(&mut self, pointer: usize, offset: usize, value: u64) {
        if offset > self.block_size - 8 {
            panic!("Trying to write out of bounds memory")
        }
        unsafe { (self.get_block_mut(pointer+offset).as_mut_ptr() as *mut u64).write(value) }
    }

    #[inline]
    pub fn write_f32(&mut self, pointer: usize, offset: usize, value: f32) {
        if offset > self.block_size - 4 {
            panic!("Trying to write out of bounds memory")
        }
        unsafe { (self.get_block_mut(pointer+offset).as_mut_ptr() as *mut f32).write(value) }
    }

    #[inline]
    pub fn write_keystring(&mut self, pointer: usize, offset: usize, value: KeyString) {
        if offset > self.block_size - 64 {
            panic!("Trying to write out of bounds memory")
        }
        unsafe { (self.get_block_mut(pointer+offset).as_mut_ptr() as *mut KeyString).write(value) }

    }
    
}




pub struct RowTable {
    pub name: KeyString,
    pub header: BTreeSet<HeaderItem>,
    pub primary_tree: BTreeMap<DbKey, usize>,
    pub indexes: HashMap<KeyString, BTreeMap<DbKey, usize>>,
    pub row_size: usize,
    pub allocator: Hallocator,
}

impl RowTable {
    pub fn new(row_size: usize, name: KeyString, header: BTreeSet<HeaderItem>) -> RowTable {
        RowTable {
            name,
            header,
            primary_tree: BTreeMap::new(),
            indexes: HashMap::new(),
            row_size, 
            allocator: Hallocator::new(row_size) 
        }
    }

    pub fn insert_row(&mut self, key: impl Into<DbKey>, row: &[u8]) -> Result<(), EzError> {

        let key: DbKey = key.into();

        let pointer = self.allocator.alloc();
        match self.allocator.get_block_mut(pointer).write(row) {
            Ok(_) => (),
            Err(e) => return Err(EzError { tag: ErrorTag::Structure, text: e.to_string() }),
        };
        self.primary_tree.insert(key, pointer);


        let mut offset: usize = match key {
            DbKey::Int(_) => 4,
            DbKey::Text(_) => 64,
        };
        for item in &self.header {
            if self.indexes.contains_key(&item.name) {
                match item.kind {
                    crate::db_structure::DbType::Int => {
                        let num = i32_from_le_slice(&row[offset..offset+4]);
                        let index_tree = self.indexes.get_mut(&item.name).expect("Will never panic because of previous check");
                        index_tree.insert(num.into(), pointer);
                        offset += 4;
                    },
                    crate::db_structure::DbType::Float => {
                        unreachable!("There cannot be a float index on a table. If we got here, there has been a consistency error in the code. Alert the maintainers asap.")
                    },
                    crate::db_structure::DbType::Text => {
                        let num = KeyString::try_from(&row[offset..offset+64]).unwrap();
                        let index_tree = self.indexes.get_mut(&item.name).expect("Will never panic because of previous check");
                        index_tree.insert(num.into(), pointer);
                        offset += 64;
                    },
                }
            }
        }

        Ok(())
    }

    pub fn add_index(&mut self, index: KeyString) -> Result<(), EzError> {

        let mut index_is_in_header = false;
        let mut index_offset = 0;
        let mut index_type: DbType = DbType::Int;
        for item in &self.header {

            if index == item.name {
                index_is_in_header = true;
                index_type = item.kind;
                match index_type {
                    DbType::Int => (),
                    DbType::Text => (),
                    DbType::Float => return Err(EzError { tag: ErrorTag::Query, text: format!("Cannot have indexes on floats") }),
                };
                break
            }
            index_offset += item.offset();
        }
        
        if !index_is_in_header {
            return Err(EzError { tag: ErrorTag::Query, text: format!("There is no column: {} in table: {}", index, self.name) })
        }

        let mut new_index_tree: BTreeMap<DbKey, usize> = BTreeMap::new();
        for (_primary_key, pointer) in &self.primary_tree {
            match index_type {
                DbType::Int => {
                    let row = self.allocator.get_block(*pointer);
                    let num = i32_from_le_slice(&row[index_offset..index_offset+4]);
                    new_index_tree.insert(num.into(), *pointer);
                },
                DbType::Text => {
                    let row = self.allocator.get_block(*pointer);
                    let ks = KeyString::try_from(&row[index_offset..index_offset+4]).unwrap();
                    new_index_tree.insert(ks.into(), *pointer);
                },
                DbType::Float => unreachable!(),
            };
        }

        Ok(())
    }
}



#[cfg(test)]
mod tests {

    use super::*;

    #[test]
    fn test_block_allocator() {
        let mut buffer = BlockAllocator::new(64).unwrap();
        let mut pointers = Vec::new();
        let mut nums = Vec::new();
        for i in 0..1000 {
            let slice = buffer.alloc();
            unsafe { (slice.pointer as *mut usize).write(i) };
            pointers.push(slice.pointer);
            nums.push(i);
        }

        println!("num sum: {}", nums.iter().sum::<usize>());

        let mut sum = 0;
        for pointer in pointers {
            let num = unsafe { *(pointer as *mut usize) };
            sum += num;
        }

        println!("pointer_sum: {}", sum);
    }

    #[test]
    fn test_hallocator() {
        let mut buffer = Hallocator::new(64);
        let mut pointers = Vec::new();
        let mut nums = Vec::new();
        for i in 0..10 as i32 {
            let pointer = buffer.alloc();
            pointers.push(pointer);
            let pointer = buffer.get_block_mut(pointer);
            unsafe { (std::mem::transmute::<*mut u8, *mut KeyString>(pointer.as_mut_ptr())).write(ksf(&format!("Hello_world: {}!", i))) };
            nums.push(i);
        }

        println!("num sum: {}", nums.iter().sum::<i32>());

        // let mut sum = 0;
        for pointer in pointers {
            let ks = buffer.read_keystring(pointer, 0);
            println!("{}", ks);
        }

        // println!("pointer_sum: {}", sum);
    }
}