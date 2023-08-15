use core::num;
use std::fmt;
use std::net::TcpStream;
use std::io::{Read, Write};
use std::error::Error;
use std::time::{Duration, self, SystemTime};
use std::str::{self, Utf8Error};

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
}

impl fmt::Display for ConnectionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ConnectionError::Io(e) => write!(f, "There has been an io error: {}", e),
            ConnectionError::TimeOut => write!(f, "Connection timed out\n"),
            ConnectionError::InvalidRequest(s) => write!(f, "Request: '{}' is invalid. For list of valid requests, see documentation", s),
            ConnectionError::UnconfirmedTransaction => write!(f, "Transaction was not confirmed by server and may not have been received"),
            ConnectionError::CorruptTransaction => write!(f, "Transaction may be corrupted"),
            ConnectionError::Utf8(e) => write!(f, "There has been a utf8 error: {}", e)
        }
    }
}


pub fn send_csv(request: &str, csv: &String, address: &str) -> Result<String, ConnectionError> {

    let mut connection: TcpStream;
    match TcpStream::connect("127.0.0.1:3004") {
        Ok(stream) => connection = stream,
        Err(e) => {return Err(ConnectionError::Io(e));},
    };
    
    match connection.write(request.as_bytes()) {
        Ok(n) => println!("Wrote request: {request}\nas {n} bytes"),
        Err(e) => {return Err(ConnectionError::Io(e));},
    };
    
    let mut buffer: [u8;1024] = [0;1024];
    let timer = SystemTime::now();
    println!("Waiting for response from server");
    loop {
        if timer.elapsed().unwrap() > Duration::from_secs(5) {
            return Err(ConnectionError::TimeOut);         
        }
        match connection.read(&mut buffer) {
            Ok(_) => break,
            Err(e) => {return Err(ConnectionError::Io(e));},
        }
    }

    let sent_bytes: usize;
    let buffer = match bytes_to_str(&buffer) {
        Ok(value) => {
            value
        },
        Err(e) => {return Err(ConnectionError::Utf8(e));}
    };
    println!("Response: '{}' - received", buffer);
    if buffer.trim() == "OK" {
        println!("Sending data...");
        match connection.write(csv.as_bytes()) {
            Ok(_) => sent_bytes = csv.as_bytes().len(),
            Err(e) => {return Err(ConnectionError::Io(e));},
        }
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
        match connection.read(&mut buffer) {
            Ok(_) => break,
            Err(_) => {return Err(ConnectionError::UnconfirmedTransaction);},
        }
    }

    let final_answer = match bytes_to_str(&buffer) {
        Ok(value) => value.to_owned(),
        Err(e) => { return Err(ConnectionError::UnconfirmedTransaction);}
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
    use std::path::Path;

    use super::*;

    #[test]
    fn test_send_csv() {
        let csv = std::fs::read_to_string("sample_data.txt").unwrap();
        let address = "127.0.0.1:3004";
        let e = send_csv("Sending CSV|test", &csv, address);
        match e {
            Ok(_) => println!("OK"),
            Err(e) => println!("{}", e),
        }
    }
}