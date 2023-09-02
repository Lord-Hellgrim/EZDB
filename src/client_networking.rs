use std::fmt;
use std::net::TcpStream;
use std::io::{Read, Write};
use std::time::{Duration, SystemTime};
use std::str::{self, Utf8Error};

use crate::db_structure::{StrictTable, StrictError};
use crate::networking_utilities::bytes_to_str;


const BUFFER_SIZE: usize = 1_000_000;


#[derive(Debug)]
pub enum ConnectionError {
    Io(std::io::Error),
    TimeOut,
    InvalidRequest(String),
    UnconfirmedTransaction,
    CorruptTransaction,
    Utf8(Utf8Error),
    Strict(StrictError),
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "There has been an io error: {}", e),
            ConnectionError::TimeOut => write!(f, "Connection timed out\n"),
            ConnectionError::InvalidRequest(s) => write!(f, "Request: '{}' is invalid. For list of valid requests, see documentation", s),
            ConnectionError::UnconfirmedTransaction => write!(f, "Transaction was not confirmed by server and may not have been received"),
            ConnectionError::CorruptTransaction => write!(f, "Transaction may be corrupted"),
            ConnectionError::Utf8(e) => write!(f, "There has been a utf8 error: {}", e),
            ConnectionError::Strict(e) => write!(f, "The requested table is not strict:\n{}", e),
        }
    }
}

impl From<std::io::Error> for ConnectionError {
    fn from(e: std::io::Error) -> Self {
        ConnectionError::Io(e)
    }
}

impl From<Utf8Error> for ConnectionError {
    fn from(e: Utf8Error) -> Self {
        ConnectionError::Utf8(e)
    }
}

impl From<StrictError> for ConnectionError {
    fn from(e: StrictError) -> Self {
        ConnectionError::Strict(e)
    }
}


// I'd change the declaration to: request_table(table_name: &str, server_address: &str)
pub fn request_csv(name: &str, address: &str) -> Result<StrictTable, ConnectionError> {
    // Validating the server_address somehow and providing a descriptive error would probably be a good idea

    let mut connection: TcpStream = match TcpStream::connect(address) {
        Ok(stream) => stream,
        Err(e) => {return Err(ConnectionError::Io(e));}
    };
    
    // The writing to the connection here is the same code as elsewhere
    // perhaps you want to encapsulate the writing/communications themselves somehow. 
    // E.g. say you change the server to accept json or another format, 
    // you'd only want to make the formatting change in one place.
    // It doesn't have to be a class but some intermideary between this request_table function
    // and the connection that would surface say at least two methods: 
    // send_request(method: requesting|sending|other, params: &str), 
    //      where the method param would be an enum allowing you to 
    //      switch between the appearent methods however that's best done in rust
    //      and the other would be the params (like name) currently going into the formatted string.
    //      This method could then also handle parsing the data from the server  (i.e. the read in the loop below)
    //      and casting the bytes to a string
    // and 
    // send_confirmation(), which would send the "OK"

    match connection.write(format!("admin|admin|Requesting|{}", name).as_bytes()) {
        Ok(n) => println!("Wrote request as {n} bytes"),
        Err(e) => {return Err(ConnectionError::Io(e));},
    };

    let mut buffer: [u8; BUFFER_SIZE] = [0; BUFFER_SIZE];
    // What the hell are you looping over here? Hahah, rust looks weird coming from js 😂 but very cool
    loop {
        match connection.read(&mut buffer) {
            Ok(_) => break,
            Err(e) => {return Err(ConnectionError::Io(e));}        }
    }

    // This would then be the result of the call to send_request, 
    // i'd maybe rename it "response" because its not necissarily csv which you clearly know ofc
    let csv = bytes_to_str(&buffer)?;

    if csv == "No such table" {
        return Err(ConnectionError::InvalidRequest("No such table".to_owned()));
    }

    let table = StrictTable::from_csv_string(csv, name)?;

    match connection.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote 'OK' as {n} bytes"),
        Err(e) => {return Err(ConnectionError::Io(e));}
    };

    Ok(table)
    


}


pub fn send_csv(name: &str, csv: &String, address: &str) -> Result<String, ConnectionError> {

    let mut stream: TcpStream;
    match TcpStream::connect(address) {
        Ok(s) => stream = s,
        Err(e) => {return Err(ConnectionError::Io(e));},
    };

    match stream.write(format!("admin|admin|Sending|{}", name).as_bytes()) {
        Ok(n) => println!("Wrote request as {n} bytes"),
        Err(e) => {return Err(ConnectionError::Io(e));},
    };
    
    let mut buffer: [u8;BUFFER_SIZE] = [0;BUFFER_SIZE];
    let timer = SystemTime::now();
    println!("Waiting for response from server");
    loop {
        if timer.elapsed().unwrap() > Duration::from_secs(5) {
            return Err(ConnectionError::TimeOut);         
        }
        match stream.read(&mut buffer) {
            Ok(_) => break,
            Err(e) => {return Err(ConnectionError::Io(e));},
        }
    }

    let buffer = match bytes_to_str(&buffer) {
        Ok(value) => {
            value
        },
        Err(e) => {return Err(ConnectionError::Utf8(e));}
    };
    println!("Response: '{}' - received", buffer);
    if buffer.trim() == "OK" {
        println!("Sending data size");
        stream.write(&csv.len().to_be_bytes())?;
        println!("Sending data...");
        let temp_buffer = String::from(csv);
        let mut index = 0;
        while index+4096 < csv.len() {
            stream.write(temp_buffer[index..index+4096].as_bytes())?;
            index += 4096;
        }
        stream.write(temp_buffer[index..temp_buffer.len()-1].as_bytes())?;
        stream.write(&[0])?;

    } else {
        return Err(ConnectionError::InvalidRequest(buffer.to_owned()));
    }

    println!("Data sent.\nWaiting for confirmation...");

    let timer = SystemTime::now();
    let mut buffer: [u8; BUFFER_SIZE] = [0;BUFFER_SIZE];
    loop {
        if timer.elapsed().unwrap() > Duration::from_secs(5) {
            return Err(ConnectionError::UnconfirmedTransaction);         
        }
        match stream.read(&mut buffer) {
            Ok(_) => break,
            Err(_) => {return Err(ConnectionError::UnconfirmedTransaction);},
        }
    }

    let final_answer = match bytes_to_str(&buffer) {
        Ok(value) => value.to_owned(),
        Err(_) => { return Err(ConnectionError::UnconfirmedTransaction);}
    };

    let mut bytes_received = "".to_owned();
    let mut num_switch = 0;
    for c in final_answer.chars() {
        if c == 'X' {
            num_switch = num_switch ^ 1;
            continue;
        }
        if num_switch == 1 {
            bytes_received.push(c);
        }
    }
    let bytes_received = bytes_received.parse::<usize>().unwrap_or(0xBAD);

    println!("Bytes received: {:X}", bytes_received);
    
    Ok("Transaction successful".to_owned())

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
        let e = send_csv("good_csv", &csv, address);
        match e {
            Ok(_) => println!("OK"),
            Err(e) => println!("{}", e),
        }
    }

    #[test]
    fn test_send_bad_csv() {
        let csv = std::fs::read_to_string("bad_csv.txt").unwrap();
        let address = "127.0.0.1:3004";
        let e = send_csv("bad_csv", &csv, address);
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
        let table = request_csv(name, address).unwrap();
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
        let e = send_csv("large_csv", &csv, address);
        match e {
            Ok(_) => println!("OK"),
            Err(e) => println!("{}", e),
        }

        //delete the large_csv
        remove_file("large.csv").unwrap();
    }

}