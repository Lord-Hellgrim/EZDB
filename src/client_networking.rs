use std::io::Write;
use std::str::{self};

use ezcbor::cbor::Cbor;
use eznoise::{initiate_connection, Connection};

use crate::aes_temp_crypto::encrypt_aes256_nonce_prefixed;
use crate::auth::{AuthenticationError, User};
use crate::compression::miniz_compress;
use crate::db_structure::{ColumnTable, KeyString};
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
        return Err(EzError::Authentication(AuthenticationError::TooLong))
}
    auth_buffer[0..username.len()].copy_from_slice(username.as_bytes());
    auth_buffer[512..password.len()].copy_from_slice(username.as_bytes());

    connection.send(&auth_buffer)?;

    Ok(connection)
}

/// downloads a table as a csv String from the EZDB server at the given address.
pub fn download_table(
    address: &str,
    username: &str,
    password: &str,
    table_name: &str,
) -> Result<ColumnTable, EzError> {
    println!("calling: download_table()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::Download(KeyString::from(table_name)).to_bytes(username);
    connection.send(&instruction)?;

    let response = connection.receive()?;

    let output = match ColumnTable::from_binary(table_name, &response) {
        Ok(table) => Ok(table),
        Err(e) => Err(EzError::from(e)),
    };

    output
}

/// Uploads a given csv string to the EZDB server at the given address.
/// Will return an error if the string is not strictly formatted
pub fn upload_csv(
    address: &str,
    username: &str,
    password: &str,
    table_name: &str,
    csv: &String,
) -> Result<(), EzError> {
    println!("calling: upload_csv()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::Upload(KeyString::from(table_name));
    send_instruction_with_associated_data(instruction, username, csv.as_bytes(), &mut connection)?;

    let response = connection.receive()?;
    let response = String::from_utf8(response)?;

    parse_response(&response, username, table_name)

}

/// Updates a given table with a given csv string. If there is an existing record in the database with
/// primary key matching a primary key in the csv passed here, it will be overwritten.
/// If there is no record with the primary key in the passed in csv, a new row will be added
/// preserving the sorted order of the table.
pub fn update_table(
    address: &str,
    username: &str,
    password: &str,
    table_name: &str,
    csv: &str,
) -> Result<(), EzError> {
    println!("calling: upload_csv()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::Update(KeyString::from(table_name));
    
    send_instruction_with_associated_data(instruction, username, csv.as_bytes(), &mut connection)?;
    
    let response = connection.receive()?;
    let response = String::from_utf8(response)?;

    parse_response(&response, username, table_name)
}

/// Send an EZQL query to the database server
pub fn send_query(
    address: &str,
    username: &str,
    password: &str,
    query: &str,
) -> Result<ColumnTable, EzError> {
    println!("calling: send_query()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::Query;
    send_instruction_with_associated_data(instruction, username, query.as_bytes(), &mut connection)?;

    let response = connection.receive()?;

    match ColumnTable::from_binary("RESULT", &response) {
        Ok(table) => Ok(table),
        Err(e) => Err(EzError::Strict(e)),
    }

}

pub fn delete_table(
    address: &str,
    username: &str,
    password: &str,
    table_name: &str,
) -> Result<(), EzError> {
    println!("calling: delete_table()");


    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::Delete(KeyString::from(table_name));

    connection.send(&instruction.to_bytes(username))?;

    let response = connection.receive()?;
    let response = String::from_utf8(response)?;

    parse_response(&response, username, table_name)
}

/// Uploads an arbitrary binary blob to the EZDB server at the given address and associates it with the given key
pub fn kv_upload(
    address: &str,
    username: &str,
    password: &str,
    key: &str,
    value: &[u8],
) -> Result<(), EzError> {
    println!("calling: kv_upload()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::KvUpload(KeyString::from(key));
    send_instruction_with_associated_data(instruction, username, value, &mut connection)?;

    let response = connection.receive()?;
    let response = String::from_utf8(response)?;

    parse_response(&response, username, key)
}

/// Downloads the binary blob associated with the passed key from the EZDB server running at address.
pub fn kv_download(
    address: &str,
    username: &str,
    password: &str,
    key: &str,
) -> Result<Vec<u8>, EzError> {
    println!("calling: kv_download()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::KvDownload(KeyString::from(key));
    connection.send(&instruction.to_bytes(username))?;    

    let response = connection.receive()?;

    Ok(response)
}

/// Overwrites the binary blob associated with the passed in key at the given address
pub fn kv_update(
    address: &str,
    username: &str,
    password: &str,
    key: &str,
    value: &[u8],
) -> Result<(), EzError> {
    println!("calling: kv_update()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::KvUpdate(KeyString::from(key));
    send_instruction_with_associated_data(instruction, username, value, &mut connection)?;

    let response = connection.receive()?;
    let response = String::from_utf8(response)?;

    parse_response(&response, username, key)
}

/// Overwrites the binary blob associated with the passed in key at the given address
pub fn kv_delete(
    address: &str,
    username: &str,
    password: &str,
    key: &str,
) -> Result<(), EzError> {
    println!("calling: kv_delete()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::KvDelete(KeyString::from(key));
    connection.send(&instruction.to_bytes(username))?;   

    let response = connection.receive()?;
    let response = String::from_utf8(response)?;

    parse_response(&response, username, key)
}

/// Returns a list of table_names in the database.
pub fn meta_list_tables(
    address: &str,
    username: &str,
    password: &str,
) -> Result<String, EzError> {
    println!("calling: meta_list_tables()");

    let mut connection = make_connection(address, username, password)?;

    let instruction = Instruction::MetaListTables.to_bytes(username);
    connection.send(&instruction)?;   

    let value = connection.receive()?;
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

    let instruction = Instruction::MetaListKeyValues.to_bytes(username);
    connection.send(&instruction)?;   

    let value = connection.receive()?;
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
    send_instruction_with_associated_data(instruction, username, &user.to_cbor_bytes(), &mut connection)?;

    let response = connection.receive()?;
    let response = String::from_utf8(response)?;

    parse_response(&response, username, &user.username)
}

fn send_instruction_with_associated_data(instruction: Instruction, username: &str, associated_data: &[u8], connection: &mut Connection) -> Result<(), EzError> {
    let instruction = instruction.to_bytes(username);
    println!("instruction lnght: {} bytes", instruction.len());
    connection.send(&instruction)?;
    connection.send(&associated_data)?;

    Ok(())
}

#[cfg(test)]
mod tests {
    #![allow(unused)]
    use std::{fs::remove_file, path::Path};

    use crate::db_structure::ColumnTable;

    use super::*;

    #[test]
    fn test_no_such_table() {
        let name = "nope";
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let table = download_table(address, username, password, name);
        assert!(table.is_err());
    }

    #[test]
    fn test_send_good_csv() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_csv(address, username, password, "good_csv", &csv);
        e.unwrap();
        // assert!(e.is_ok());
    }

    #[test]
    fn test_send_good_csv_twice() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_csv(address, username, password, "good_csv", &csv);
        assert!(e.is_ok());
        println!("About to check second table");
        std::thread::sleep(std::time::Duration::from_secs(2));
        let d = upload_csv(address, username, password, "good_csv", &csv);
        assert!(d.is_ok());
    }

    // #[test]
    // fn test_concurrent_connections() {
    //     let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
    //     let address = "127.0.0.1:3004";
    //     let username = "admin";
    //     let password = "admin";
    //     let a = upload_table(address, username, password, "good_csv", &csv);
    //     assert!(a.is_ok());
    //     println!("About to check second table");
    //     std::thread::sleep(std::time::Duration::from_secs(2));
    //     for _ in 0..100 {
    //         download_table(address, username, password, "good_csv").unwrap();
    //     }
    // }

    #[test]
    fn test_send_bad_csv() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}bad_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_csv(address, username, password, "bad_csv", &csv);
        assert!(e.is_err());
    }

    #[test]
    fn test_receive_csv() {
        println!("Sending...\n##########################");
        // test_send_good_csv();
        let name = "good_csv";
        let address = "127.0.0.1:3004";
        println!("Receiving\n############################");
        let username = "admin";
        let password = "admin";
        let table = download_table(address, username, password, name).unwrap();
        println!("{:?}", table);
        let good_table = ColumnTable::from_csv_string(
            &std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap(),
            "good_table",
            "test",
        )
        .unwrap();
        assert_eq!(table, good_table);
    }

    #[test]
    fn test_send_large_csv() {
        // create the large_csv
        let mut i = 0;
        let mut printer = String::from("vnr,t-P;heiti,t-N;magn,i-N\n");
        loop {
            if i > 1_000_000 {
                break;
            }
            printer.push_str(&format!("i{};product name;569\n", i));
            i += 1;
        }
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_csv(address, username, password, "large_csv", &printer).unwrap();
    }

    #[test]
    fn test_update_table() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_csv(address, username, password, "good_csv", &csv).unwrap();
        assert_eq!(e, ());
        let update_csv = "vnr,i-P;heiti,t-N;magn,i-N\n0113000;undirlegg2;200\n0113035;undirlegg;200\n18572054;flísalím;42";
        update_table(address, username, password, "good_csv", update_csv).unwrap();

    }

    #[test]
    fn test_query() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_csv(address, username, password, "good_csv", &csv).unwrap();
        assert_eq!(e, ());
        println!("NEXT REQUEST");

        let query = "SELECT(table_name: good_csv, primary_keys: (*), columns: (*), conditions: ())";
        let username = "admin";
        let password = "admin";
        let response = send_query(address, username, password, query).unwrap();
        let full_table = download_table(address, username, password, "good_csv").unwrap();
        println!("{}", response);
        assert_eq!(response, full_table);
    }

    #[test]
    fn test_delete_table() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        test_send_good_csv();
        let tables = meta_list_tables(address, username, password).unwrap();
        println!("tables:\n{}", tables);
        let e = delete_table(address, username, password, "good_csv").unwrap();
        let tables = meta_list_tables(address, username, password).unwrap();
        println!("tables:\n{}", tables);
    }

    #[test]
    fn test_kv_upload() {
        let value: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9];
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = kv_upload(address, username, password, "test_upload", value).unwrap();
    }

    #[test]
    fn test_kv_download() {
        let value: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9];
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        println!("About to upload");
        kv_upload(address, username, password, "test_download", value);
        println!("About to download");
        let e = kv_download(address, username, password, "test_download").unwrap();
        println!("value: {:x?}", e);
    }

    #[test]
    fn test_kv_delete() {
        let value: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9];
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        println!("About to upload");
        kv_upload(address, username, password, "test_delete", value);
        println!("About to delete");
        let e = kv_delete(address, username, password, "test_delete").unwrap();
        println!("value: {:x?}", e);
    }

    #[test]
    fn test_kv_update() {
        let value: &[u8] = &[1, 2, 3, 4, 5, 6, 7, 8, 9];
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        kv_upload(address, username, password, "test_update", value);
        let value: &[u8] = &[9, 8, 7, 6, 5, 4, 3, 2, 1];
        kv_update(address, username, password, "test_update", value);
        let e = kv_download(address, username, password, "test_update").unwrap();
        println!("value: {:x?}", e);
    }

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

    #[test]
    fn test_list_key_values() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        test_kv_upload();
        // std::thread::sleep(Duration::from_secs(3));
        let tables = meta_list_key_values(address, username, password).unwrap();
        println!("Key - Value pairs: \n{}", tables);
    }
}
