use std::fmt;
use std::net::TcpStream;
use std::io::{Read, Write};
use std::error::Error;
use std::time::{Duration, self, SystemTime};
use std::str::{self, Utf8Error};
// pub fn client() {
//     let mut x = TcpStream::connect("127.0.0.1:3004").unwrap();
//     let mut s = String::from("");
//     match x.read_to_string(&mut s) {
//         Ok(n) => {
//             println!("Read {} bytes", n);
//             println!("spacer\n\n");    
//         },
//         Err(_) => panic!(),
//     };
//     println!("{}", s);
// }


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
            Self::CorruptTransaction => write!(f, "Transaction may be corrupted"),
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
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ConnectionError::Io(e));},
    };
    
    let mut buffer: [u8;1024] = [0;1024];
    let timer = SystemTime::now();
    println!("Waiting for response");
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
    let buffer = match str::from_utf8(&buffer) {
        Ok(value) => {
            println!("{}", value);
            value
        },
        Err(e) => {return Err(ConnectionError::Utf8(e));}
    };
    println!("Response: '{buffer}' - received");
    if buffer.trim() == "OK" {
        match connection.write_all(csv.as_bytes()) {
            Ok(_) => sent_bytes = csv.as_bytes().len(),
            Err(e) => {return Err(ConnectionError::Io(e));},
        }
    } else {
        return Err(ConnectionError::InvalidRequest(buffer.to_owned()));
    }

    let timer = SystemTime::now();
    let mut buffer = String::new();
    loop {
        if timer.elapsed().unwrap() > Duration::from_secs(5) {
            return Err(ConnectionError::UnconfirmedTransaction);         
        }
        match connection.read_to_string(&mut buffer) {
            Ok(_) => break,
            Err(_) => {return Err(ConnectionError::UnconfirmedTransaction);},
        }
    }
    if buffer.parse::<usize>().unwrap() == sent_bytes {
        Ok("Transaction cofirmed by server".to_owned())
    } else {
        Err(ConnectionError::CorruptTransaction)
    }
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