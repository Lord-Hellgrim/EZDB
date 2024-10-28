use std::str::{self};

use eznoise::{initiate_connection, Connection};

use crate::auth::User;
use crate::db_structure::{ColumnTable, KeyString};
use crate::ezql::Query;
use crate::utilities::{bytes_to_str, parse_response, EzError, Instruction};
use crate::PATH_SEP;


pub enum Response {
    Message(String),
    Table(ColumnTable),
}

pub fn make_connection(address: &str, username: &str, password: &str) -> Result<Connection, EzError> {
    let mut connection = initiate_connection(address)?;
    let mut auth_buffer = [0u8;1024];
    if username.len() > 512 || password.len() > 512 {
        return Err(EzError::Authentication("Username and password must each be less than 512 bytes".to_owned()))
}
    auth_buffer[0..username.len()].copy_from_slice(username.as_bytes());
    auth_buffer[512..password.len()].copy_from_slice(username.as_bytes());

    connection.send_c1(&auth_buffer)?;

    Ok(connection)
}

/// Send an EZQL query to the database server
pub fn send_query(
    address: &str,
    username: &str,
    password: &str,
    query: Query,
) -> Result<ColumnTable, EzError> {
    println!("calling: send_query()");

    let mut connection = make_connection(address, username, password)?;

    let query = query.to_string();
    let mut packet = Vec::new();
    packet.extend_from_slice(KeyString::from("QUERY").raw());
    packet.extend_from_slice(query.as_bytes());
    connection.send_c1(&packet)?;
    
    let response = connection.receive_c2()?;

    match ColumnTable::from_binary(Some("RESULT"), &response) {
        Ok(table) => Ok(table),
        Err(e) => Err(e),
    }

}

/// Returns a list of table_names in the database.
pub fn meta_list_tables(
    address: &str,
    username: &str,
    password: &str,
) -> Result<String, EzError> {
    println!("calling: meta_list_tables()");

    let mut connection = make_connection(address, username, password)?;

    let mut packet = Vec::new();
    packet.extend_from_slice(KeyString::from("META_LIST_TABLES").raw());
    connection.send_c1(&packet)?;   

    let value = connection.receive_c2()?;
    let table_list = bytes_to_str(&value)?;

    Ok(table_list.to_owned())
}

/// Returns a list of keys with associated binary blobs.
pub fn meta_list_key_values(
    address: &str,
    username: &str,
    password: &str,
) -> Result<String, EzError> {
    println!("calling: meta_list_key_values()");

    let mut connection = make_connection(address, username, password)?;

    // let instruction = Instruction::MetaListKeyValues.to_bytes(username);
    // connection.send_c1(&instruction)?;   

    let value = connection.receive_c2()?;
    let table_list = bytes_to_str(&value)?;

    Ok(table_list.to_owned())
}

pub fn meta_create_new_user(
    user: User,
    address: &str,
    username: &str,
    password: &str,
) -> Result<(), EzError> {
    println!("calling: meta_create_new_user()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::NewUser;
    // send_instruction_with_associated_data(instruction, username, &user.to_cbor_bytes(), &mut connection)?;

    let response = connection.receive_c2()?;
    let response = String::from_utf8(response)?;

    parse_response(&response, username, &user.username)
}


#[cfg(test)]
mod tests {
    #![allow(unused)]
    use std::{fs::remove_file, path::Path};

    use crate::db_structure::ColumnTable;

    use super::*;

    #[test]
    fn test_list_tables() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        // test_send_good_csv();
        // test_send_large_csv();
        // std::thread::sleep(Duration::from_secs(3));
        let tables = meta_list_tables(address, username, password).unwrap();
        println!("tables: \n{}", tables);
    }


}
