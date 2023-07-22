use std::fmt;
use std::net::TcpStream;
use std::io::{Read, Write};
use std::error::Error;
use std::time::{Duration, self, SystemTime};

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
}


pub fn send_csv(request: &str, csv: &String, address: &str) -> Result<String, ConnectionError> {

    let mut connection: TcpStream;
    match TcpStream::connect("127.0.0.1:3004") {
        Ok(stream) => connection = stream,
        Err(e) => {return Err(ConnectionError::Io(e));},
    };
    let mut buffer = String::new();

    match connection.write(request.as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes\n"),
        Err(e) => {return Err(ConnectionError::Io(e));},
    };

    let timer = SystemTime::now();
    loop {
        if timer.elapsed().unwrap() > Duration::from_secs(5) {
            return Err(ConnectionError::TimeOut);         
        }
        match connection.read_to_string(&mut buffer) {
            Ok(_) => break,
            Err(e) => {return Err(ConnectionError::Io(e));},
        }
    }

    let sent_bytes: usize;
    if buffer == "OK" {
        match connection.write(csv.as_bytes()) {
            Ok(n) => sent_bytes = n,
            Err(e) => {return Err(ConnectionError::Io(e));},
        }
    } else {
        return Err(ConnectionError::InvalidRequest(buffer));
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



pub fn client() {
    let mut stream = TcpStream::connect("127.0.0.1:3004").unwrap();
    let mut s: [u8;1000] = [0;1000];

    match stream.write("give me five!".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => panic!("{e}"),
    };
    // stream.flush().unwrap();
    // std::thread::sleep(std::time::Duration::from_secs(1));
    loop {
        match stream.read(&mut s) {
            Ok(n) => {
                if n == 0 {
                    println!("end of file");
                    break;
                }
                println!("Read {} bytes", n);
                let mut output = String::from("");
                for byte in s {
                    if byte == 0 {
                        break;
                    }
                    output.push(char::from(byte));
                }
                println!("{}", output);
            },
            Err(_) => break,
        };
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_client() {
        client();
    }


    #[test]
    fn test_send_csv() {
        
    }
}