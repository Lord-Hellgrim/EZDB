use std::collections::{BTreeMap, HashSet};
use std::fs::{create_dir, read_dir, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::os::unix::fs::MetadataExt;
use std::path::Path;
use std::sync::atomic::AtomicU64;
use std::sync::{Arc, RwLock};

use crate::db_structure::{write_subtable_to_raw_binary, DbType, DbColumn, HeaderItem, KeyString, Metadata, StrictError, Value};
use crate::networking_utilities::{f32_from_le_slice, i32_from_le_slice, ServerError};
use crate::{db_structure::EZTable, server_networking::CONFIG_FOLDER};
use crate::PATH_SEP;

pub const BIN_TABLE_DIR: &str = "Binary_tables";
pub const MAX_BUFFERPOOL_SIZE: u64 = 4_000_000_000;   // 4gb
pub const CHUNK_SIZE: usize = 1_000_000;                // 1mb


pub struct BufferPool {
    max_size: AtomicU64,
    pub tables: Arc<RwLock<BTreeMap<KeyString, RwLock<EZTable>>>>,
    pub values: Arc<RwLock<BTreeMap<KeyString, RwLock<Value>>>>,
    pub files: Arc<RwLock<BTreeMap<KeyString, RwLock<File>>>>,
    pub naughty_list: Arc<RwLock<HashSet<KeyString>>>,
}

impl BufferPool {
    pub fn init_tables(&self, path: &str) -> Result<(), ServerError> {

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

            let table = EZTable::read_raw_binary(&name, &binary)?;
            self.add_table(table, table_file)?;
        }

        Ok(())
    }

    pub fn init_values(&self, path: &str) -> Result<(), ServerError> {
        
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

            let value = Value::read_raw_binary(&name, &binary);

            self.add_value(value, value_file)?;
        }
        
        Ok(())
    }

    pub fn empty(max_size: AtomicU64) -> BufferPool {
        let tables = Arc::new(RwLock::new(BTreeMap::new()));
        let values = Arc::new(RwLock::new(BTreeMap::new()));
        let files = Arc::new(RwLock::new(BTreeMap::new()));
        let naughty_list = Arc::new(RwLock::new(HashSet::new()));

        BufferPool {
            max_size,
            tables,
            values,
            files,
            naughty_list,
        }

    }

    pub fn occupied_buffer(&self) -> u64 {
        let mut output: u64 = 0;
        for table in self.tables.read().unwrap().values() {
            output += table.read().unwrap().byte_size() as u64;
        }

        output
    }

    pub fn max_size(&self) -> u64 {
        self.max_size.load(std::sync::atomic::Ordering::Relaxed)
    }

    pub fn add_table(&self, table: EZTable, table_file: File) -> Result<(), ServerError> {

        if self.occupied_buffer() + table.metadata.size_of_table() as u64 > self.max_size() {
            return Err(ServerError::NoMoreBufferSpace(table.metadata.size_of_table()))
        }

        self.files.write().unwrap().insert(table.name, RwLock::new(table_file));
        self.tables.write().unwrap().insert(table.name, RwLock::new(table));


        Ok(())
    }

    pub fn add_value(&self, value: Value, value_file: File) -> Result<(), ServerError> {
        if self.occupied_buffer() + value.body.len() as u64 > self.max_size() {
            return Err(ServerError::NoMoreBufferSpace(value.body.len()))
        }

        self.files.write().unwrap().insert(value.name, RwLock::new(value_file));
        self.values.write().unwrap().insert(value.name, RwLock::new(value));

        Ok(())
    }
    
    pub fn write_table_to_file(&self, table_name: &KeyString) -> Result<(), ServerError> {

        let disk_data = self.tables.read().unwrap()[table_name].read().unwrap().write_to_raw_binary();
        self.files.write().unwrap().get_mut(table_name).unwrap().write().unwrap().write_all(&disk_data)?;
        Ok(())

    }

    pub fn clear_space(&mut self, space_to_clear: u64) -> Result<(), ServerError> {
        
        let lru_table = self.tables
            .read()
            .unwrap()
            .values()
            .map(|n| {
                let x = n.read().unwrap();
                (x.metadata.last_access, x.name)
            })
            .min_by(|x, y| x.0.cmp(&y.0))
            .unwrap();

        let lru_value = self.values
            .read()
            .unwrap()
            .values()
            .map(|n| {
                let x = n.read().unwrap();
                (x.metadata.last_access, x.name)
            })
            .min_by(|x, y| x.0.cmp(&y.0))
            .unwrap();

        if lru_table.0 < lru_value.0 {
            self.write_table_to_file(&lru_table.1)?;
            self.tables.write().unwrap()[&lru_table.1].write().unwrap().clear();
        }

        if self.occupied_buffer() + space_to_clear < self.max_size() {   
            Ok(())
        } else {
            self.clear_space(space_to_clear)
        }
            
    }
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
        match &table.columns[&table.get_primary_key_col_index()] {
            DbColumn::Ints(v) => {
                // println!("{}..={}", v[start], v[stop]);
                chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[stop]))
            },
            DbColumn::Floats(v) => {
                // println!("{}..={}", v[start], v[stop]);
                chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v[stop]))
                
            },
            DbColumn::Texts(v) => {
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
    match &table.columns[&table.get_primary_key_col_index()] {
        DbColumn::Ints(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v.last().unwrap()))
        },
        DbColumn::Floats(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v.last().unwrap()))
            
        },
        DbColumn::Texts(v) => {
            chunk_path.push_str(&format!("{PATH_SEP}{}..={}", v[start], v.last().unwrap()))
        },
    };
    let mut chunk_file = File::create(Path::new(&chunk_path))?;
    let subtable_binary = write_subtable_to_raw_binary(subtable);
    chunk_file.write_all(&subtable_binary)?;

    Ok(())
}

pub fn read_binary_table_chunk_into_memory(table_file: &str, header: &[HeaderItem], metadata: &Metadata) -> Result<EZTable, StrictError> {

    let mut file = File::open(table_file)?;

    let file_size = file.metadata().unwrap().size();
    let length = file_size / metadata.size_of_row() as u64;

    let mut table = BTreeMap::new();
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
                    .map(i32_from_le_slice)
                    .collect();
                table.insert(header[index].name, DbColumn::Ints(v));
                total_bytes += amount_to_read;
            },
            DbType::Float => {
                let amount_to_read = (length * 4) as usize;
                file.read_exact(&mut buf[..amount_to_read])?;
                let v: Vec<f32> = buf[..(length * 4) as usize]
                    .chunks(4)
                    .map(f32_from_le_slice)
                    .collect();
                table.insert(header[index].name, DbColumn::Floats(v));
                total_bytes += amount_to_read;
            },
            DbType::Text => {
                let amount_to_read = (length * 64) as usize;
                file.read_exact(&mut buf[..amount_to_read])?;
                let v: Result<Vec<KeyString>, StrictError> = buf[..(length * 64) as usize]
                    .chunks(64)
                    .map(KeyString::try_from)
                    .collect();
                let v = v?;
                table.insert(header[index].name, DbColumn::Texts(v));
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
            header: header.to_owned(),
            columns: table,
        }
    )
}

#[cfg(test)]
mod tests {
    // #![allow(unused)]

    use super::*;

    // #[test]
    // fn bin_dir_basic_test() {
    //     let table_string = std::fs::read_to_string(&format!("testlarge.csv")).unwrap();
    //     let table = EZTable::from_csv_string(&table_string, "basic_test", "test").unwrap();
    //     // write_table_to_binary_directory(&table).unwrap();
    //     let chunks = "/home/hellgrim/code/rust/EZDB/EZconfig/Binary_tables/basic_test";
    //     let mut chunks = std::fs::read_dir(chunks).unwrap();
    //     let first = chunks.next().unwrap().unwrap().path();
    //     // println!("first: {}", first.display());
    //     let mut read_table = read_binary_table_chunk_into_memory(&first.as_path().to_str().unwrap(), &table.header, &table.metadata).unwrap();

    //     for chunk in chunks {
    //         let chunk = chunk.unwrap().path();
    //         // println!("chunk_path: {}", chunk.display());
    //         let temp_table = read_binary_table_chunk_into_memory(chunk.as_path().to_str().unwrap(), &table.header, &table.metadata).unwrap();
    //         read_table.update(&temp_table).unwrap();
    //     }

    //     for (index, column) in table.columns.iter().enumerate() {
    //         match column {
    //             DbVec::Ints(col) => {
    //                 match &read_table.columns[index] {
    //                     DbVec::Ints(read_col) => {
    //                         for i in 0.. col.len() {
    //                             if col[i] != read_col[i] {
    //                                 println!("wrong index: {}", i);
    //                             }
    //                         }
    //                     },
    //                     _ => todo!(),
    //                 }
    //             },
    //             DbVec::Floats(col) => {
    //                 match &read_table.columns[index] {
    //                     DbVec::Floats(read_col) => {
    //                         for i in 0.. col.len() {
    //                             if col[i] != read_col[i] {
    //                                 println!("wrong index: {}", i);
    //                             }
    //                         }
    //                     },
    //                     _ => todo!(),
    //                 }
    //             },
    //             DbVec::Texts(col) => {
    //                 match &read_table.columns[index] {
    //                     DbVec::Texts(read_col) => {
    //                         for i in 0.. col.len() {
    //                             if col[i] != read_col[i] {
    //                                 println!("wrong index: {}", i);
    //                             }
    //                         }
    //                     },
    //                     _ => todo!(),
    //                 }
    //             },
    //         }
    //     }

    //     // let mut table_file = File::create("table.txt").unwrap();
    //     // table_file.write(table.to_string().as_bytes());

    //     // let mut table_file = File::create("read_table.txt").unwrap();
    //     // table_file.write(read_table.to_string().as_bytes());


    //     assert_eq!(read_table.columns, table.columns);
    // }
}