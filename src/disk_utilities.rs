use std::fs::{create_dir, File};
use std::sync::RwLock;

use crate::db_structure::{HeaderItem, KeyString, Metadata};
use crate::{db_structure::ColumnTable, server_networking::CONFIG_FOLDER};
use crate::PATH_SEP;

pub const BIN_TABLE_DIR: &'static str = "Binary_tables";

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