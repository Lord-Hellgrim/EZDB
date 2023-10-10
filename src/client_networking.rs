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


// I'd change the declaration to: request_table(table_name: &str, server_address: &str)
// Agree with name => table_name but this gets a csv. Should be called download_csv, though, to be consistent with server()
pub fn download_table(mut connection: &mut Connection, table_name: &str, username: &str, password: &str) -> Result<String, ServerError> {
        
    let response = instruction_send_and_confirm(username, password, Instruction::Download(table_name.to_owned()), &mut connection)?;

    let csv: String;

    if response.as_str() == "OK" {
        (csv, _) = receive_data(&mut connection)?;
    } else if response.as_str() == "Username is incorrect" {
        return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned())));
    } else if response.as_str() == "Password is incorrect" {
        return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.to_owned())));
    } else if response.as_str().starts_with("No such table as:") {
        return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("No such table as {}", table_name))));
    } else {
        panic!("Need to handle error: {}", response.as_str());
    }

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));}
    };

    Ok(csv)
    


}


pub fn upload_table(mut connection: &mut Connection, table_name: &str, csv: &String, username: &str, password: &str) -> Result<String, ServerError> {

    let response = instruction_send_and_confirm(username, password, Instruction::Upload(table_name.to_owned()), &mut connection)?;

    let confirmation: String;

    if response.as_str() == "OK" {
        confirmation = data_send_and_confirm(&mut connection, &csv)?;
    } else if response.as_str() == "Username is incorrect" {
        return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned())));
    } else if response.as_str() == "Password is incorrect" {
        return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.to_owned())));
    } else if response.as_str().starts_with("No such table as:") {
        return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Table {} does not exist", table_name))));
    } else {
        panic!("Need to handle error: {}", response.as_str());
    }

    let data_len = (csv.len() + 28).to_string();
    if confirmation == data_len {
        return Ok("OK".to_owned());
    } else {
        return Err(ServerError::Confirmation(confirmation));
    }

}


pub fn update_table(mut connection: &mut Connection, table_name: &str, csv: &String, username: &str, password: &str) -> Result<String, ServerError> {

    let response = instruction_send_and_confirm(username, password, Instruction::Update(table_name.to_owned()), &mut connection)?;

    let confirmation: String;
    match response.as_str() {

        // THIS IS WHERE YOU SEND THE BULK OF THE DATA
        //########## SUCCESS BRANCH #################################
        "OK" => confirmation = data_send_and_confirm(&mut connection, &csv)?,
        //###########################################################
        "Username is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned()))),
        "Password is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.to_owned()))),
        e => panic!("Need to handle error: {}", e),
    };

    let data_len = (csv.len() + 28).to_string();
    if confirmation == data_len {
        println!("Confirmation from server: {}", confirmation);
        return Ok("OK".to_owned());
    } else {
        println!("Confirmation from server: {}", confirmation);
        return Err(ServerError::Confirmation(confirmation));
    }

}


pub fn query_table(mut connection: &mut Connection, table_name: &str, query: &str, username: &str, password: &str) -> Result<String, ServerError> {
    
    let response = instruction_send_and_confirm(username, password, Instruction::Query(table_name.to_owned(), query.to_owned()), &mut connection)?;

    let csv: String;
    match response.as_str() {

        // THIS IS WHERE YOU SEND THE BULK OF THE DATA
        //########## SUCCESS BRANCH #################################
        "OK" => (csv, _) = receive_data(&mut connection)?,
        //###########################################################
        "Username is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned()))),
        "Password is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.to_owned()))),
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
        let start = rdtsc();
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let mut connection = Connection::connect(address).unwrap();
        let e = upload_table(&mut connection, "good_csv", &csv, "admin", "admin").unwrap();
    }


    #[test]
    fn test_send_bad_csv() {
        let csv = std::fs::read_to_string("bad_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let mut connection = Connection::connect(address).unwrap();
        let e = upload_table(&mut connection, "bad_csv", &csv, "admin", "admin");
        assert!(e.is_err());
        
    }

    #[test]
    fn test_receive_csv() {
        println!("Sending...\n##########################");
        test_send_good_csv();
        let name = "good_csv";
        let address = "127.0.0.1:3004";
        println!("Receiving\n############################");
        let mut connection = Connection::connect(address).unwrap();
        let table = download_table(&mut connection, name, "admin", "admin").unwrap();
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
        let mut connection = Connection::connect(address).unwrap();
        let e = upload_table(&mut connection, "large_csv", &csv, "admin", "admin");
        
        //delete the large_csv
        remove_file("large.csv").unwrap();
        assert!(e.is_ok());
    }


    #[test]
    fn test_query_list() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let mut connection = Connection::connect(address).unwrap();
        let e = upload_table(&mut connection, "good_csv", &csv, "admin", "admin").unwrap();
        assert_eq!(e, "OK");

        let query = "0113000,0113035";
        let mut connection = Connection::connect(address).unwrap();
        let response = query_table(&mut connection, "good_csv", query, "admin", "admin").unwrap();
        println!("{}", response);
    }

}