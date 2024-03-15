use std::collections::{BTreeMap, HashMap};
use std::fs::{create_dir, File};
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, RwLock};

use crate::db_structure::{write_subtable_to_raw_binary, DbVec, HeaderItem, KeyString, Metadata, Value};
use crate::{db_structure::ColumnTable, server_networking::CONFIG_FOLDER};
use crate::PATH_SEP;

pub const BIN_TABLE_DIR: &'static str = "Binary_tables";
pub const MAX_BUFFERPOOL_SIZE: usize = 4_000_000_000;   // 4gb
pub const CHUNK_SIZE: usize = 1_000_000;                // 1mb


pub struct BufferPool {
    max_size: usize,
    pub tables: Arc<RwLock<HashMap<KeyString, RwLock<ColumnTable>>>>,
    pub values: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>,
    naughty_list: Vec<KeyString>,
}

impl BufferPool {
    pub fn with_max_size(max_size: usize) -> BufferPool {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        let values = Arc::new(RwLock::new(HashMap::new()));
        let naughty_list = Vec::new();

        BufferPool {
            max_size,
            tables,
            values,
            naughty_list,
        }

    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }
}


#[derive(Debug)]
pub struct DiskTable {
    pub name: KeyString,
    pub header: Vec<HeaderItem>,
    pub metadata: Metadata,
    pub chunks: Vec<RwLock<File>>,
}


pub fn write_table_to_binary_directory(table: &ColumnTable) -> Result<(), std::io::Error> {

    let path_str = format!("{CONFIG_FOLDER}{PATH_SEP}{BIN_TABLE_DIR}{PATH_SEP}{}", table.name.as_str());

    let bin_dir_path = Path::new(&path_str);

    if bin_dir_path.is_dir() {
        return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "There is already a table on disk with this name"))
    }

    create_dir(&path_str)?;

    if table.len() == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "The table that was attempted to write to disk was empty"))
    }
    

    let mut start = 0;
    let rows_per_chunk = CHUNK_SIZE / table.metadata.size_of_row();
    while start + rows_per_chunk < table.len() {

        
        let subtable = table.create_subtable(start, start + rows_per_chunk);
        let stop = start + rows_per_chunk;
        // println!("{}..={}", start, stop);
        
        let mut chunk_path = path_str.clone();
        match &table.table[table.get_primary_key_col_index()] {
            DbVec::Ints(v) => {
                println!("{}..={}", v[start], v[stop]);
                chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[stop]))
            },
            DbVec::Floats(v) => {
                println!("{}..={}", v[start], v[stop]);
                chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[stop]))
                
            },
            DbVec::Texts(v) => {
                println!("{}..={}", v[start], v[stop]);
                chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[stop]))
            },
        };
        let mut chunk_file = File::create(Path::new(&chunk_path))?;
        let subtable_binary = write_subtable_to_raw_binary(subtable);
        chunk_file.write_all(&subtable_binary)?;
        
        start += rows_per_chunk;

    }

    let subtable = table.create_subtable(start, start + rows_per_chunk);
    
    let mut chunk_path = path_str.clone();
    match &table.table[table.get_primary_key_col_index()] {
        DbVec::Ints(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[v.len()]))
        },
        DbVec::Floats(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[v.len()]))
            
        },
        DbVec::Texts(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[v.len()]))
        },
    };
    let mut chunk_file = File::create(Path::new(&chunk_path))?;
    let subtable_binary = write_subtable_to_raw_binary(subtable);
    chunk_file.write_all(&subtable_binary)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    // #![allow(unused)]

    use super::*;

    #[test]
    fn basic_test() {
        let table_string = std::fs::read_to_string(&format!("testlarge.csv")).unwrap();
        let table = ColumnTable::from_csv_string(&table_string, "basic_test", "test").unwrap();
        write_table_to_binary_directory(&table).unwrap();
    }

}