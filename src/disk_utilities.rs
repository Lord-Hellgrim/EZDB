use std::collections::{BTreeMap, HashMap};
use std::fs::{create_dir, File};
use std::sync::RwLock;

use crate::db_structure::{HeaderItem, KeyString, Metadata, Value};
use crate::{db_structure::ColumnTable, server_networking::CONFIG_FOLDER};
use crate::PATH_SEP;

pub const BIN_TABLE_DIR: &'static str = "Binary_tables";
pub const MAX_BUFFERPOOL_SIZE: usize = 4_294_967_296;   // 4gb


pub struct BufferPool {
    max_size: usize,
    pub tables: HashMap<KeyString, RwLock<ColumnTable>>,
    pub values: HashMap<KeyString, RwLock<Value>>,
    naughty_list: Vec<KeyString>,
}

impl BufferPool {
    pub fn with_max_size(max_size: usize) -> BufferPool {
        let tables = HashMap::new();
        let values = HashMap::new();
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


pub fn write_table_to_binary_directory(table: &ColumnTable) -> Result<DiskTable, std::io::Error> {

    let path_str = format!("{CONFIG_FOLDER}{PATH_SEP}{BIN_TABLE_DIR}{PATH_SEP}{}", table.name.as_str());

    let bin_dir_path = std::path::Path::new(&path_str);

    if bin_dir_path.is_dir() {
        return Err(std::io::Error::new(std::io::ErrorKind::AlreadyExists, "There is already a table on disk with this name"))
    }

    create_dir(format!("{CONFIG_FOLDER}{PATH_SEP}{}", table.name.as_str()))?;


    

    todo!()
}