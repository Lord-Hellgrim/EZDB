use std::collections::{BTreeMap, BTreeSet, HashSet};

use crate::db_structure::{DbType, DbValue, HeaderItem};


pub struct RowTable {
    header: BTreeSet<HeaderItem>,
    rows: BTreeMap<DbValue, usize>,
}


pub struct RowArena {
    buffer: Vec<usize>,
    row_size: usize,
    free_list: Vec<u32>,
    pointer: usize,
}

impl RowArena {

    pub fn new(row_size: usize) -> Self {
        println!("calling: RowArena::new()");

        RowArena {
            buffer: Vec::new(),
            row_size: row_size,
            free_list: Vec::new(),
            pointer: 0,
        }
    }

    pub fn allocate_rows_at_end(&mut self, number_of_rows: usize) -> usize {
        println!("calling: RowArena::allocate_rows_at_end()");

        let returned_pointer = self.pointer;

        self.pointer += number_of_rows*self.row_size;

        returned_pointer
    }

    pub fn free_all(&mut self) {
        println!("calling: RowArena::free_all()");

        self.pointer = 0;
        self.buffer.shrink_to_fit();
    }

}