use std::collections::{BTreeMap, HashSet};
use std::fs::{read_dir, File};
use std::io::{Read, Write};
use std::os::unix::fs::MetadataExt;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use crate::db_structure::{write_column_table_binary_header, DbColumn, Metadata, Value};
use crate::utilities::{ksf, KeyString, ErrorTag, EzError};
use crate::db_structure::ColumnTable;
use crate::PATH_SEP;

pub const BIN_TABLE_DIR: &str = "Binary_tables";
pub const MAX_BUFFERPOOL_SIZE: u64 = 4_000_000_000;   // 4gb
pub const CHUNK_SIZE: usize = 1_000_000;                // 1mb


pub struct BufferPool {
    max_size: AtomicU64,
    pub tables: Arc<RwLock<BTreeMap<KeyString, RwLock<ColumnTable>>>>,
    pub values: Arc<RwLock<BTreeMap<KeyString, Value>>>,
    pub table_naughty_list: Arc<RwLock<HashSet<KeyString>>>,
    pub value_naughty_list: Arc<RwLock<HashSet<KeyString>>>,
    pub table_delete_list: Arc<RwLock<HashSet<KeyString>>>,
    pub value_delete_list: Arc<RwLock<HashSet<KeyString>>>,
    
}

impl BufferPool {
    pub fn init_tables(&self, path: &str) -> Result<(), EzError> {
        println!("calling: BufferPool::init_tables()");


        let data_dir = read_dir(path)?;

        for file in data_dir{
            let file = file?;
            let file_size = file.metadata()?.size();
            if file_size + self.occupied_buffer() > self.max_size() {
                break;
            }

            let name = file.file_name().into_string().unwrap();
            let mut table_file = File::open(file.path())?;

            let mut binary = Vec::with_capacity(file_size as usize);
            table_file.read_to_end(&mut binary)?;

            let table = ColumnTable::from_binary(Some(&name), &binary)?;
            
            self.add_table(table)?;
        }

        let good_table = std::fs::read_to_string(&format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let good_table = ColumnTable::from_csv_string(&good_table, "good_table", "server").unwrap();
        println!("good_table.len() = {}", good_table.to_binary().len());
        match self.add_table(good_table) {
            Ok(_) => (),
            Err(_) => (),
        };

        Ok(())
    }

    pub fn init_values(&self, path: &str) -> Result<(), EzError> {
        
        println!("calling: BufferPool::init_values()");

        let data_dir = read_dir(path)?;

        for file in data_dir{
            let file = file?;
            let file_size = file.metadata()?.size();
            if file_size + self.occupied_buffer() > self.max_size() {
                break;
            }

            let name = file.file_name().into_string().unwrap();
            let mut value_file = File::open(file.path())?;

            let mut binary = Vec::with_capacity(file_size as usize);
            value_file.read_to_end(&mut binary)?;

            let value = Value::from_binary(&name, &binary)?;

            self.add_value(value)?;
        }

        let core_value_1 = Value{name: ksf("core1"), body: vec![1,2,3,4,5,6,7,8]};
        let core_value_2 = Value{name: ksf("core2"), body: vec![8,7,6,5,4,3,2,1]};
        let core_value_3 = Value{name: ksf("core3"), body: vec![0,0,0,0,0,0,0,0]};

        self.add_value(core_value_1);
        self.add_value(core_value_2);
        self.add_value(core_value_3);
        
        Ok(())
    }

    pub fn empty(max_size: AtomicU64) -> BufferPool {
        println!("calling: BufferPool::empty()");

        let tables = Arc::new(RwLock::new(BTreeMap::new()));
        let values = Arc::new(RwLock::new(BTreeMap::new()));
        let table_naughty_list = Arc::new(RwLock::new(HashSet::new()));
        let value_naughty_list = Arc::new(RwLock::new(HashSet::new()));
        let table_delete_list = Arc::new(RwLock::new(HashSet::new()));
        let value_delete_list = Arc::new(RwLock::new(HashSet::new()));

        BufferPool {
            max_size,
            tables,
            values,
            table_naughty_list,
            value_naughty_list,
            table_delete_list,
            value_delete_list,
            
        }
    }

    pub fn occupied_buffer(&self) -> u64 {
        println!("calling: BufferPool::occupied_buffer()");

        let mut output: u64 = 0;
        for table in self.tables.read().unwrap().values() {
            output += table.read().unwrap().byte_size() as u64;
        }

        output
    }

    pub fn max_size(&self) -> u64 {
        self.max_size.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn add_table(&self, table: ColumnTable) -> Result<(), EzError> {
        println!("calling: BufferPool::add_table()");


        if self.occupied_buffer() + table.size_of_table() as u64 > self.max_size() {
            return Err(EzError{tag: ErrorTag::NoMoreBufferSpace, text: format!("Table sized: {} is too big. Remaining space is: {}",table.size_of_table(), self.max_size()-self.occupied_buffer())})
        }

        if self.tables.read().unwrap().contains_key(&table.name) {
            return Err(EzError{tag: ErrorTag::Structure, text: format!("Table named '{}' already exists", table.name)});
        } else {
            self.table_naughty_list.write().unwrap().insert(table.name);
            self.tables.write().unwrap().insert(table.name, RwLock::new(table));
        }

        Ok(())
    }

    pub fn remove_table(&self, table_name: KeyString) -> Result<(), EzError> {
        println!("calling: BufferPool::add_table()");


        match self.tables.write().unwrap().remove(&table_name) {
            Some(_) => Ok(()),
            None => Err(EzError { tag: ErrorTag::Structure, text: format!("No table named: '{}'", table_name) }),
        }
    }

    pub fn add_value(&self, value: Value) -> Result<(), EzError> {
        println!("calling: BufferPool::add_value()");

        if self.occupied_buffer() + value.body.len() as u64 > self.max_size() {
            return Err(EzError{tag: ErrorTag::NoMoreBufferSpace, text: format!("Table sized: {} is too big. Remaining space is: {}",value.body.len(), self.max_size()-self.occupied_buffer())})

        }

        if self.values.read().unwrap().contains_key(&value.name) {
            return Err(EzError{tag: ErrorTag::Structure, text: format!("value named '{}' already exists", value.name)});
        } else {
            self.value_naughty_list.write().unwrap().insert(value.name);
            self.values.write().unwrap().insert(value.name, value);
        }
        Ok(())
    }
    
    pub fn write_table_to_disk(&self) -> Result<(), EzError> {
        println!("calling: BufferPool::write_table_to_disk()");

        

        Ok(())
    }

}


#[cfg(test)]
mod tests {
    #![allow(unused)]

    use super::*;


}