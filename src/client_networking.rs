use std::net::TcpStream;
use std::io::{Read, Write};
use std::time::Duration;
use std::str::{self};

use crate::auth::AuthenticationError;
use crate::db_structure::StrictTable;
use crate::networking_utilities::*;


// I'd change the declaration to: request_table(table_name: &str, server_address: &str)
// Agree with name => table_name but this gets a csv. Should be called download_csv, though, to be consistent with server()
pub fn download_table(table_name: &str, address: &str, username: &str, password: &str) -> Result<StrictTable, ServerError> {
    
    let mut stream: TcpStream = TcpStream::connect(address)?;
    
    let response = instruction_send_and_confirm(username, password, Instruction::Download(table_name.to_owned()), &mut stream)?;

    let csv: String;
    match response.as_str() {

        // THIS IS WHERE YOU SEND THE BULK OF THE DATA
        //########## SUCCESS BRANCH #################################
        "OK" => (csv, _) = receive_data(&mut stream)?,
        //###########################################################
        "Username is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned()))),
        "Password is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.to_owned()))),
        "Missing username or password or both" => return Err(ServerError::Authentication(AuthenticationError::MissingField)),
        e => panic!("Need to handle error: {}", e),
    };

    let table = StrictTable::from_csv_string(&csv, table_name)?;

    match stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));}
    };

    Ok(table)
    


}


pub fn upload_table(table_name: &str, csv: &String, address: &str, username: &str, password: &str) -> Result<String, ServerError> {

    let mut stream = TcpStream::connect(address)?;

    let response = instruction_send_and_confirm(username, password, Instruction::Upload(table_name.to_owned()), &mut stream)?;

    let confirmation: String;
    match response.as_str() {

        // THIS IS WHERE YOU SEND THE BULK OF THE DATA
        //########## SUCCESS BRANCH #################################
        "OK" => confirmation = data_send_and_confirm(&mut stream, &csv)?,
        //###########################################################
        "Username is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned()))),
        "Password is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.to_owned()))),
        "Missing username or password or both" => return Err(ServerError::Authentication(AuthenticationError::MissingField)),
        e => panic!("Need to handle error: {}", e),
    };

    let data_len = csv.len().to_string();
    if confirmation == data_len {
        return Ok("OK".to_owned());
    } else {
        return Err(ServerError::Confirmation(vec!()));
    }

}


pub fn update_table(table_name: &str, csv: &String, address: &str, username: &str, password: &str) -> Result<String, ServerError> {

    let mut stream = TcpStream::connect(address)?;

    let response = instruction_send_and_confirm(username, password, Instruction::Update(table_name.to_owned()), &mut stream)?;

    let confirmation: String;
    match response.as_str() {

        // THIS IS WHERE YOU SEND THE BULK OF THE DATA
        //########## SUCCESS BRANCH #################################
        "OK" => confirmation = data_send_and_confirm(&mut stream, &csv)?,
        //###########################################################
        "Username is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned()))),
        "Password is incorrect" => return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.to_owned()))),
        "Missing username or password or both" => return Err(ServerError::Authentication(AuthenticationError::MissingField)),
        e => panic!("Need to handle error: {}", e),
    };

    let data_len = csv.len().to_string();
    if confirmation == data_len {
        println!("Confirmation from server: {}", confirmation);
        return Ok("OK".to_owned());
    } else {
        println!("Confirmation from server: {}", confirmation);
        return Err(ServerError::Confirmation(vec!()));
    }

}


#[cfg(test)]
mod tests {
    #[allow(unused)]
    use std::{path::Path, fs::remove_file};

    use super::*;

    #[test]
    fn test_send_good_csv() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let e = upload_table("good_csv", &csv, address, "admin", "admin");
        match e {
            Ok(_) => println!("OK"),
            Err(e) => println!("{}", e),
        }
    }

    #[test]
    fn test_send_bad_csv() {
        let csv = std::fs::read_to_string("bad_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let e = upload_table("bad_csv", &csv, address, "admin", "admin");
        match e {
            Ok(_) => println!("OK"),
            Err(e) => println!("{}", e),
        }
    }

    #[test]
    fn test_receive_csv() {
        println!("Sending...\n##########################");
        test_send_good_csv();
        let name = "good_csv";
        let address = "127.0.0.1:3004";
        println!("Receiving\n############################");
        let table = download_table(name, address, "admin", "admin").unwrap();
        println!("{:?}", table.table);

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
        let e = upload_table("large_csv", &csv, address, "admin", "admin");
        match e {
            Ok(_) => println!("OK"),
            Err(e) => println!("{}", e),
        }

        //delete the large_csv
        remove_file("large.csv").unwrap();
    }

}