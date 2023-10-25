use std::net::TcpStream;
use std::io::{Read, Write};
use std::process::Command;
use std::sync::TryLockError;
use std::time::Duration;
use std::str::{self};

use crate::auth::AuthenticationError;
use crate::db_structure::StrictTable;
use crate::{diffie_hellman::*, aes};
use crate::networking_utilities::*;


pub fn download_table(address: &str, username: &str, password: &str, table_name: &str) -> Result<String, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

        
    let response = instruction_send_and_confirm(Instruction::Download(table_name.to_owned()), &mut connection)?;

    let csv: String;
    
    match parse_response(&response, &connection.peer.Username, &connection.peer.Password, table_name) {
        Ok(f) => (csv, _) = receive_data(&mut connection)?,
        Err(e) => return Err(e),
    }


    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));}
    };

    Ok(csv)


}


pub fn upload_table(address: &str, username: &str, password: &str, table_name: &str, csv: &String) -> Result<String, ServerError> {

    let mut connection = Connection::connect(address, username, password)?;

    let response = instruction_send_and_confirm(Instruction::Upload(table_name.to_owned()), &mut connection)?;

    let confirmation: String;

    println!("upload_table - parsing response");
    match parse_response(&response, &connection.peer.Username, &connection.peer.Password, table_name) {
        Ok(_) => confirmation = data_send_and_confirm(&mut connection, &csv)?,
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

    match parse_response(&response, &connection.peer.Username, &connection.peer.Password, table_name) {
        Ok(_) => confirmation = data_send_and_confirm(&mut connection, &csv)?,
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

    let csv: String;
    match response.as_str() {

        // THIS IS WHERE YOU SEND THE BULK OF THE DATA
        //########## SUCCESS BRANCH #################################
        "OK" => (csv, _) = receive_data(&mut connection)?,
        //###########################################################
        "Username is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongUser(connection.peer.Username.to_owned()))),
        "Password is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongPassword(connection.peer.Password.to_owned()))),
        e => panic!("Need to handle error: {}", e),
    };

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));}
    };

    Ok(csv)
}

#[cfg(test)]
mod tests {
    use std::arch::asm;
    #[allow(unused)]
    use std::{path::Path, fs::remove_file};

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
        std::thread::sleep(Duration::from_secs(2));
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
        std::thread::sleep(Duration::from_secs(2));
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
        let good_table = StrictTable::from_csv_string(&std::fs::read_to_string("good_csv.txt").unwrap(), "good_table").unwrap();
        assert_eq!(table, good_table.to_csv_string());

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
        let mut file = std::fs::File::create("large.csv").unwrap();
        file.write_all(printer.as_bytes()).unwrap();


        let csv = std::fs::read_to_string("large.csv").unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let mut connection = Connection::connect(address, username, password).unwrap();        
        let e = upload_table(address, username, password, "large_csv", &csv);
        
        //delete the large_csv
        remove_file("large.csv").unwrap();
        assert!(e.is_ok());
    }


    #[test]
    fn test_query_list() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let mut connection = Connection::connect(address, username, password).unwrap();        
        let e = upload_table(address, username, password, "good_csv", &csv).unwrap();
        assert_eq!(e, "OK");

        let query = "0113000,0113035";
        let username = "admin";
        let password = "admin";
        let mut connection = Connection::connect(address, username, password).unwrap();        
        let response = query_table(address, username, password, "good_csv", query).unwrap();
        println!("{}", response);
    }

}