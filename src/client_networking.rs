use std::io::Write;
use std::str::{self};

use crate::auth::AuthenticationError;
use crate::networking_utilities::*;
use crate::PATH_SEP;


// TODO
// delete query
// WHERE
// bounds on queries
// skila sorteruðum niðurstöðum
// relational mapping (BCNF)



pub fn download_table(address: &str, username: &str, password: &str, table_name: &str) -> Result<String, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::Download(table_name.to_owned()), &mut connection)?;
    println!("Instruction successfully sent");
    println!("response: {}", response);
    
    let csv: Vec<u8>;
    match parse_response(&response, &connection.user, table_name) {
        Ok(_) => csv = receive_data(&mut connection)?,
        Err(e) => return Err(e),
    }
    println!("received: {}", bytes_to_str(&csv)?);

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));}
    };
    connection.stream.flush()?;

    Ok(bytes_to_str(&csv)?.to_owned())
}


pub fn upload_table(address: &str, username: &str, password: &str, table_name: &str, csv: &String) -> Result<String, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::Upload(table_name.to_owned()), &mut connection)?;

    println!("upload_table - parsing response");
    let confirmation: String = match parse_response(&response, &connection.user, table_name) {
        Ok(_) => data_send_and_confirm(&mut connection, csv.as_bytes())?,
        Err(e) => return Err(e),
    };
    println!("confirmation: {}", confirmation);

    if confirmation == "OK" {
        return Ok("OK".to_owned());
    } else {
        return Err(ServerError::Confirmation(confirmation));
    }

}


pub fn update_table(address: &str, username: &str, password: &str, table_name: &str, csv: &str) -> Result<String, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::Update(table_name.to_owned()), &mut connection)?;

    let confirmation: String = match parse_response(&response, &connection.user, table_name) {
        Ok(_) => data_send_and_confirm(&mut connection, csv.as_bytes())?,
        Err(e) => return Err(e),
    };

    // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
    // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
    let data_len = (csv.len() + 28).to_string();
    if confirmation == data_len {
        println!("Confirmation from server: {}", confirmation);
        return Ok("OK".to_owned());
    } else {
        println!("Confirmation from server: {}", confirmation);
        return Err(ServerError::Confirmation(confirmation));
    }

}


pub fn query_table(address: &str, username: &str, password: &str, table_name: &str, query: &str) -> Result<String, ServerError> {
    
    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::Query(table_name.to_owned(), query.to_owned()), &mut connection)?;

    let csv: Vec<u8>;
    match response.as_str() {

        // THIS IS WHERE YOU SEND THE BULK OF THE DATA
        //########## SUCCESS BRANCH #################################
        "OK" => csv = receive_data(&mut connection)?,
        //###########################################################
        "Username is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongUser(connection.user))),
        "Password is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.as_bytes().to_owned()))),
        e => panic!("Need to handle error: {}", e),
    };

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));}
    };

    Ok(bytes_to_str(&csv)?.to_owned())
}


pub fn kv_upload(address: &str, username: &str, password: &str, key: &str, value: &[u8]) -> Result<(), ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::KvUpload(key.to_owned()), &mut connection)?;

    println!("upload_value - parsing response");
    let confirmation: String = match parse_response(&response, &connection.user, key) {
        Ok(_) => data_send_and_confirm(&mut connection, value)?,
        Err(e) => return Err(e),
    };
    println!("value uploaded successfully");

    // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
    // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
    let data_len = (value.len() + 28).to_string();
    if confirmation == data_len {
        return Ok(());
    } else {
        return Err(ServerError::Confirmation(confirmation));
    }
}

pub fn kv_download(address: &str, username: &str, password: &str, key: &str) -> Result<Vec<u8>, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::KvDownload(key.to_owned()), &mut connection)?;

    let value: Vec<u8>;
    
    match parse_response(&response, &connection.user, key) {
        Ok(_) => value = receive_data(&mut connection)?,
        Err(e) => return Err(e),
    }


    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));}
    };

    Ok(value)
}

pub fn kv_update(address: &str, username: &str, password: &str, key: &str, value: &[u8]) -> Result<(), ServerError> {
    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::KvUpdate(key.to_owned()), &mut connection)?;

    let confirmation: String;

    println!("upload_value - parsing response");
    match parse_response(&response, &connection.user, key) {
        Ok(_) => confirmation = data_send_and_confirm(&mut connection, value)?,
        Err(e) => return Err(e),
    }
    println!("value uploaded successfully");

    // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
    // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
    let data_len = (value.len() + 28).to_string();
    if confirmation == data_len {
        return Ok(());
    } else {
        return Err(ServerError::Confirmation(confirmation));
    }
}

pub fn meta_list_tables(address: &str, username: &str, password: &str) -> Result<String, ServerError> {
    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::MetaListTables, &mut connection)?;

    let value: Vec<u8>;
    
    match parse_response(&response, &connection.user, "") {
        Ok(_) => value = receive_data(&mut connection)?,
        Err(e) => return Err(e),
    }
    println!("value downloaded successfully");

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));}
    };

    let table_list = bytes_to_str(&value)?;

    Ok(table_list.to_owned())
}

pub fn meta_list_key_values(address: &str, username: &str, password: &str) -> Result<String, ServerError> {
    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::MetaListKeyValues, &mut connection)?;

    let value: Vec<u8>;
    
    match parse_response(&response, &connection.user, "") {
        Ok(_) => value = receive_data(&mut connection)?,
        Err(e) => return Err(e),
    }
    println!("value downloaded successfully");

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));}
    };

    let table_list = bytes_to_str(&value)?;

    Ok(table_list.to_owned())
}




#[cfg(test)]
mod tests {
    #![allow(unused)]
    use std::{path::Path, fs::remove_file};

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
        let e = upload_table(address, username, password, "good_csv", &csv);
        e.unwrap();
        // assert!(e.is_ok());
    }

    #[test]
    fn test_send_good_csv_twice() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_table(address, username, password, "good_csv", &csv);
        assert!(e.is_ok());
        println!("About to check second table");
        std::thread::sleep(std::time::Duration::from_secs(2));
        let d = upload_table(address, username, password, "good_csv", &csv);
        assert!(d.is_ok());
    }

    #[test]
    fn test_concurrent_connections() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let a = upload_table(address, username, password, "good_csv", &csv);
        assert!(a.is_ok());
        println!("About to check second table");
        std::thread::sleep(std::time::Duration::from_secs(2));
        for _ in 0..100 {
            download_table(address, username, password, "good_csv").unwrap();
        }
    }


    #[test]
    fn test_send_bad_csv() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}bad_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_table(address, username, password, "bad_csv", &csv);
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
        let good_table = ColumnTable::from_csv_string(&std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap(), "good_table", "test").unwrap();
        assert_eq!(table, good_table.to_string());
    }

    #[test]
    fn test_send_large_csv() {

        // create the large_csv
        let mut i = 0;
        let mut printer = String::from("vnr;heiti;magn\n");
        loop {
            if i > 1_000_000 {
                break;
            }
            printer.push_str(&format!("i{};product name;569\n", i));
            i+= 1;
        }
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_table(address, username, password, "large_csv", &printer).unwrap();
        
    }


    #[test]
    fn test_query_list() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_table(address, username, password, "good_csv", &csv).unwrap();
        assert_eq!(e, "OK");

        let query = "0113000,0113035";
        let username = "admin";
        let password = "admin";
        let response = query_table(address, username, password, "good_csv", query).unwrap();
        println!("{}", response);
    }

    #[test]
    fn test_kv_upload() {
        let value: &[u8] = &[1,2,3,4,5,6,7,8,9];
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = kv_upload(address, username, password, "test_upload", value).unwrap();   
    
    }

    #[test]
    fn test_kv_download() {
        let value: &[u8] = &[1,2,3,4,5,6,7,8,9];
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
    fn test_kv_update() {
        let value: &[u8] = &[1,2,3,4,5,6,7,8,9];
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        kv_upload(address, username, password, "test_update", value);
        let value: &[u8] = &[9,8,7,6,5,4,3,2,1];
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
        // test_send_good_csv();
        // test_send_large_csv();
        // std::thread::sleep(Duration::from_secs(3));
        let tables = meta_list_key_values(address, username, password).unwrap();
        println!("tables: \n{}", tables);
    }

}