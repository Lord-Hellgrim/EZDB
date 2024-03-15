use std::fs::{self, create_dir};

use crate::{db_structure::ColumnTable, server_networking::CONFIG_FOLDER};




pub fn write_table_to_binary_directory(table: &ColumnTable) -> std::io::Result<()> {

    create_dir(format!("{CONFIG_FOLDER}{PATH_SEP}{}", table.name.as_str()))?;

    Ok(())
}