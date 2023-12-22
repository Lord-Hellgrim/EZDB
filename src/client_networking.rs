use std::io::Write;
use std::str::{self};

use crate::auth::AuthenticationError;
use crate::networking_utilities::*;


pub fn download_table(address: &str, username: &str, password: &str, table_name: &str) -> Result<String, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

        
    let response = instruction_send_and_confirm(Instruction::Download(table_name.to_owned()), &mut connection)?;

    let csv: Vec<u8>;
    
    match parse_response(&response, &connection.user, password.as_bytes(), table_name) {
        Ok(_) => (csv, _) = receive_data(&mut connection)?,
        Err(e) => return Err(e),
    }


    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));}
    };

    Ok(bytes_to_str(&csv)?.to_owned())


}


pub fn upload_table(address: &str, username: &str, password: &str, table_name: &str, csv: &String) -> Result<String, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::Upload(table_name.to_owned()), &mut connection)?;

    let confirmation: String;

    println!("upload_table - parsing response");
    match parse_response(&response, &connection.user, password.as_bytes(), table_name) {
        Ok(_) => confirmation = data_send_and_confirm(&mut connection, csv.as_bytes())?,
        Err(e) => return Err(e),
    }

    // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
    // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
    let data_len = (csv.len() + 28).to_string();
    if confirmation == data_len {
        return Ok("OK".to_owned());
    } else {
        return Err(ServerError::Confirmation(confirmation));
    }

}


pub fn update_table(address: &str, username: &str, password: &str, table_name: &str, csv: &str) -> Result<String, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::Update(table_name.to_owned()), &mut connection)?;

    let confirmation: String;

    match parse_response(&response, &connection.user, password.as_bytes(), table_name) {
        Ok(_) => confirmation = data_send_and_confirm(&mut connection, csv.as_bytes())?,
        Err(e) => return Err(e),
    }

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
        "OK" => (csv, _) = receive_data(&mut connection)?,
        //###########################################################
        "Username is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongUser(connection.user))),
        "Password is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.as_bytes().to_owned()))),
        e => panic!("Need to handle error: {}", e),
    };

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));}
    };

    Ok(bytes_to_str(&csv)?.to_owned())
}


pub fn kv_upload(address: &str, username: &str, password: &str, key: &str, value: &[u8]) -> Result<(), ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::KvUpload(key.to_owned()), &mut connection)?;

    let confirmation: String;

    println!("upload_value - parsing response");
    match parse_response(&response, &connection.user, password.as_bytes(), key) {
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

pub fn kv_download(address: &str, username: &str, password: &str, key: &str) -> Result<Vec<u8>, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

        
    let response = instruction_send_and_confirm(Instruction::KvDownload(key.to_owned()), &mut connection)?;

    let value: Vec<u8>;
    
    match parse_response(&response, &connection.user, password.as_bytes(), key) {
        Ok(_) => (value, _) = receive_data(&mut connection)?,
        Err(e) => return Err(e),
    }


    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));}
    };

    Ok(value)

}




#[cfg(test)]
mod tests {
    #[allow(unused)]
    use std::{path::Path, fs::remove_file};

    use crate::db_structure::ColumnTable;

    use super::*;


    #[test]
    fn test_send_good_csv() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_table(address, username, password, "good_csv", &csv).unwrap();
    }

    #[test]
    fn test_send_good_csv_twice() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = upload_table(address, username, password, "good_csv", &csv).unwrap();
        println!("About to check second table");
        std::thread::sleep(std::time::Duration::from_secs(2));
        let d = upload_table(address, username, password, "good_csv", &csv).unwrap();
    }

    #[test]
    fn test_concurrent_connections() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let a = upload_table(address, username, password, "good_csv", &csv).unwrap();
        println!("About to check second table");
        std::thread::sleep(std::time::Duration::from_secs(2));
        for i in 0..100 {
            download_table(address, username, password, "good_csv");
        }
        
    }


    #[test]
    fn test_send_bad_csv() {
        let csv = std::fs::read_to_string("bad_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let mut connection = Connection::connect(address, username, password).unwrap();        
        let e = upload_table(address, username, password, "bad_csv", &csv);
        assert!(e.is_err());
        
    }

    #[test]
    fn test_receive_csv() {
        println!("Sending...\n##########################");
        test_send_good_csv();
        let name = "good_csv";
        let address = "127.0.0.1:3004";
        println!("Receiving\n############################");
        let username = "admin";
        let password = "admin";
        let mut connection = Connection::connect(address, username, password).unwrap();        
        let table = download_table(address, username, password, name).unwrap();
        println!("{:?}", table);
        let good_table = ColumnTable::from_csv_string(&std::fs::read_to_string("good_csv.txt").unwrap(), "good_table", "test").unwrap();
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
        let mut file = std::fs::File::create("testlarge.csv").unwrap();
        file.write_all(printer.as_bytes()).unwrap();


        let csv = std::fs::read_to_string("testlarge.csv").unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let mut connection = Connection::connect(address, username, password).unwrap();        
        let e = upload_table(address, username, password, "large_csv", &csv);
        
        //delete the large_csv
        // remove_file("large.csv").unwrap();
        assert!(e.is_ok());
    }


    #[test]
    fn test_query_list() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
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
        let e = kv_upload(address, username, password, "test_key", value).unwrap();   
    
    }

    #[test]
    fn test_kv_download() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let e = kv_download(address, username, password, "test_key").unwrap();
        println!("value: {:x?}", e);
    }

}