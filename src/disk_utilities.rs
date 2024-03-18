use std::collections::{BTreeMap, HashMap};
use std::fs::{create_dir, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::{self, Path, PathBuf};
use std::sync::{Arc, RwLock};

use serde::Serialize;

use crate::db_structure::{write_subtable_to_raw_binary, DbType, DbVec, HeaderItem, KeyString, Metadata, StrictError, Value};
use crate::networking_utilities::{f32_from_le_slice, i32_from_le_slice, ServerError};
use crate::{db_structure::EZTable, server_networking::CONFIG_FOLDER};
use crate::PATH_SEP;

pub const BIN_TABLE_DIR: &'static str = "Binary_tables";
pub const MAX_BUFFERPOOL_SIZE: usize = 4_000_000_000;   // 4gb
pub const CHUNK_SIZE: usize = 1_000_000;                // 1mb


pub struct BufferPool {
    max_size: usize,
    current_size: usize,
    pub tables: Arc<RwLock<HashMap<KeyString, RwLock<EZTable>>>>,
    pub values: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>,
}

impl BufferPool {
    pub fn with_max_size(max_size: usize) -> BufferPool {
        let tables = Arc::new(RwLock::new(HashMap::new()));
        let values = Arc::new(RwLock::new(HashMap::new()));

        BufferPool {
            max_size,
            current_size: 0,
            tables,
            values,
        }

    }

    pub fn max_size(&self) -> usize {
        self.max_size
    }

    pub fn add_table(&mut self, table: EZTable) -> Result<(), ServerError> {

        if self.current_size + table.metadata.size_of_table() > self.max_size {
            return Err(ServerError::NoMoreBufferSpace(table.metadata.size_of_table()))
        }

        self.tables.write().unwrap().insert(table.name, RwLock::new(table));

        Ok(())
    }

    pub fn clear_space(&mut self) -> Result<(), ServerError> {
        
        let mut lru = u64::MAX;
        let mut lru_key = KeyString::new();
        {
            let tables = self.tables.read().unwrap();
            for key in tables.keys() {
                let temp = tables[key].read().unwrap().metadata.last_access;
                if temp < lru {
                    lru_key = key.clone();
                    lru = temp;
                }
            }
        }
        let mut key_is_value = false;
        {
            let values = self.values.read().unwrap();
            for key in values.keys() {
                let temp = values[key].read().unwrap().metadata.last_access;
                if temp < lru {
                    key_is_value = true;
                    lru_key = key.clone();
                    lru = temp;
                }
            }
        }

        if key_is_value {
            let values = self.values.write().unwrap();
            let disk_data = values[&lru_key].write().unwrap().write_to_raw_binary();

            values[&lru_key].write().unwrap().body = Vec::new();
        }
        

        Ok(())
        
    }
}


#[derive(Debug)]
pub struct DiskTable {
    pub name: KeyString,
    pub header: Vec<HeaderItem>,
    pub metadata: Metadata,
    pub file: RwLock<File>,
    pub pages: Vec<Page>,
}

#[derive(Debug)]
pub struct Page {
    is_dirty: bool,
    offset: u64,
    size: u64,
}

pub fn alternate_write(table: &EZTable) -> Result<(), std::io::Error> {



    Ok(())
}

pub fn write_table_to_binary_directory(table: &EZTable) -> Result<(), std::io::Error> {

    let path_str = format!("{CONFIG_FOLDER}{PATH_SEP}{BIN_TABLE_DIR}{PATH_SEP}{}", table.name.as_str());

    let bin_dir_path = Path::new(&path_str);

    if bin_dir_path.is_dir() {
        return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "There is already a table on disk with this name"))
    }

    create_dir(&path_str)?;

    if table.len() == 0 {
        return Err(std::io::Error::new(std::io::ErrorKind::WriteZero, "The table that was attempted to write to disk was empty"))
    }
    
    let mut header_file_path = path_str.clone();
    header_file_path.push_str(&format!("{PATH_SEP}header"));
    let mut header_file = File::create(header_file_path)?;
    let mut full_header = String::new();
    for head in &table.header {
        full_header.push_str(&head.to_string());
        full_header.push(';');
    }
    full_header.pop();

    let mut start = 0;
    let rows_per_chunk = CHUNK_SIZE / table.metadata.size_of_row();
    while start + rows_per_chunk < table.len() {

        let subtable = table.create_subtable(start, start + rows_per_chunk);
        // println!("subtable:\n{}\n\n", subtable);
        let stop = start + rows_per_chunk;
        
        // println!("{}..={}", start, stop);
        
        let mut chunk_path = path_str.clone();
        match &table.columns[table.get_primary_key_col_index()] {
            DbVec::Ints(v) => {
                // println!("{}..={}", v[start], v[stop]);
                chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[stop]))
            },
            DbVec::Floats(v) => {
                // println!("{}..={}", v[start], v[stop]);
                chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[stop]))
                
            },
            DbVec::Texts(v) => {
                // println!("{}..={}", v[start], v[stop]);
                chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[stop]))
            },
        };
        let mut chunk_file = File::create(Path::new(&chunk_path))?;
        let subtable_binary = write_subtable_to_raw_binary(subtable);
        chunk_file.write_all(&subtable_binary)?;
        
        start += rows_per_chunk;
        
    }
    
    let subtable = table.create_subtable(start, table.len());
    
    let mut chunk_path = path_str.clone();
    match &table.columns[table.get_primary_key_col_index()] {
        DbVec::Ints(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v.last().unwrap()))
        },
        DbVec::Floats(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v.last().unwrap()))
            
        },
        DbVec::Texts(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v.last().unwrap()))
        },
    };
    let mut chunk_file = File::create(Path::new(&chunk_path))?;
    let subtable_binary = write_subtable_to_raw_binary(subtable);
    chunk_file.write_all(&subtable_binary)?;

    Ok(())
}

pub fn read_binary_table_chunk_into_memory(table_file: &str, header: &Vec<HeaderItem>, metadata: &Metadata) -> Result<EZTable, StrictError> {

    let mut file = File::open(table_file)?;

    let file_size = file.metadata().unwrap().size();
    let length = file_size / metadata.size_of_row() as u64;

    let mut table = Vec::with_capacity(header.len());
    let mut buf = [0u8;1_000_000];
    let mut index = 0;
    let mut total_bytes: usize = 0;
    while total_bytes < file_size as usize {
        
        match header[index].kind {
            DbType::Int => {
                let amount_to_read = (length * 4) as usize;
                file.read_exact(&mut buf[..amount_to_read])?;
                let v: Vec<i32> = buf[..(length * 4) as usize]
                    .chunks(4)
                    .map(|chunk| i32_from_le_slice(chunk))
                    .collect();
                table.push(DbVec::Ints(v));
                total_bytes += amount_to_read;
            },
            DbType::Float => {
                let amount_to_read = (length * 4) as usize;
                file.read_exact(&mut buf[..amount_to_read])?;
                let v: Vec<f32> = buf[..(length * 4) as usize]
                    .chunks(4)
                    .map(|chunk| f32_from_le_slice(chunk))
                    .collect();
                table.push(DbVec::Floats(v));
                total_bytes += amount_to_read;
            },
            DbType::Text => {
                let amount_to_read = (length * 64) as usize;
                file.read_exact(&mut buf[..amount_to_read])?;
                let v: Vec<KeyString> = buf[..(length * 64) as usize]
                    .chunks(64)
                    .map(|chunk| KeyString::from(chunk))
                    .collect();
                table.push(DbVec::Texts(v));
                total_bytes += amount_to_read;
            },
        }
        index += 1;
        file.seek(SeekFrom::Start(total_bytes as u64))?;
    }

    Ok(
        EZTable {
            name: KeyString::from("test"),
            metadata: metadata.clone(),
            header: header.clone(),
            columns: table,
        }
    )
}

#[cfg(test)]
mod tests {
    // #![allow(unused)]

    use super::*;

    #[test]
    fn bin_dir_basic_test() {
        let table_string = std::fs::read_to_string(&format!("testlarge.csv")).unwrap();
        let table = EZTable::from_csv_string(&table_string, "basic_test", "test").unwrap();
        // write_table_to_binary_directory(&table).unwrap();
        let chunks = "/home/hellgrim/code/rust/EZDB/EZconfig/Binary_tables/basic_test";
        let mut chunks = std::fs::read_dir(chunks).unwrap();
        let first = chunks.next().unwrap().unwrap().path();
        // println!("first: {}", first.display());
        let mut read_table = read_binary_table_chunk_into_memory(&first.as_path().to_str().unwrap(), &table.header, &table.metadata).unwrap();

        for chunk in chunks {
            let chunk = chunk.unwrap().path();
            // println!("chunk_path: {}", chunk.display());
            let temp_table = read_binary_table_chunk_into_memory(chunk.as_path().to_str().unwrap(), &table.header, &table.metadata).unwrap();
            read_table.update(&temp_table).unwrap();
        }

        for (index, column) in table.columns.iter().enumerate() {
            match column {
                DbVec::Ints(col) => {
                    match &read_table.columns[index] {
                        DbVec::Ints(read_col) => {
                            for i in 0.. col.len() {
                                if col[i] != read_col[i] {
                                    println!("wrong index: {}", i);
                                }
                            }
                        },
                        _ => todo!(),
                    }
                },
                DbVec::Floats(col) => {
                    match &read_table.columns[index] {
                        DbVec::Floats(read_col) => {
                            for i in 0.. col.len() {
                                if col[i] != read_col[i] {
                                    println!("wrong index: {}", i);
                                }
                            }
                        },
                        _ => todo!(),
                    }
                },
                DbVec::Texts(col) => {
                    match &read_table.columns[index] {
                        DbVec::Texts(read_col) => {
                            for i in 0.. col.len() {
                                if col[i] != read_col[i] {
                                    println!("wrong index: {}", i);
                                }
                            }
                        },
                        _ => todo!(),
                    }
                },
            }
        }

        // let mut table_file = File::create("table.txt").unwrap();
        // table_file.write(table.to_string().as_bytes());

        // let mut table_file = File::create("read_table.txt").unwrap();
        // table_file.write(read_table.to_string().as_bytes());


        assert_eq!(read_table.columns, table.columns);
    }
}