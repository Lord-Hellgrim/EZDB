use std::fmt;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::error::Error;

use crate::db_structure::{self, StrictTable};


const MAX_INSTRUCTION_LENGTH: usize = 1024;

pub enum Request {
    Upload,
    Download,
}

#[derive(Debug, PartialEq)]
pub enum InstructionError {
    Invalid(String),
    TooLong,
}

impl fmt::Display for InstructionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InstructionError::Invalid(instruction) => write!(f, "The instruction:\n\n{instruction}\n\nis invalid. See documentation for valid instructions\n\n"),
            InstructionError::TooLong => write!(f, "Your instructions are too long. Maximum instruction length is: {MAX_INSTRUCTION_LENGTH}\n\n"),
        }
    }
}




pub fn server(address: &str, TableVec: &mut Vec<StrictTable>) -> Result<(), Box<dyn Error>> {
    let l = TcpListener::bind(address)?;

    for stream in l.incoming() {
        println!("Accepted connection");
        std::thread::spawn(|| {
            println!("Spawned thread");
            let mut stream = match stream {
                Ok(value) => {println!("Unwrapped Result"); value},
                Err(e) => panic!("{}", e),
            };

            let mut instructions = String::new();
            println!("Initialized string buffer");
            loop {
                match stream.read_to_string(&mut instructions) {
                    Ok(n) => {
                        println!("Read {n} bytes");
                        break;
                    },
                    Err(e) => panic!("{e}"),
                };
            }

            if instructions == "Sending CSV" {
                match stream.write("OK".as_bytes()) {
                    Ok(n) => println!("Wrote {n} bytes"),
                    Err(e) => panic!("{e}"),
                };
            }

            let mut csv = String::new();
            loop {
                match stream.read_to_string(&mut csv) {
                    Ok(n) => {
                        match stream.write(&n.to_be_bytes()) {
                            Ok(_) => println!("Confirmed reception"),
                            Err(e) => {return Err(e);},                        
                        };
                        break;
                    },
                    Err(e) => {return Err(e)},
                };
            }

            // TODO Need to parse the CSV for correctness before saving
            TableVec.push(db_structure::create_StrictTable_from_csv(&csv));
            Ok(())

        });
        continue;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listener() {
        server("127.0.0.1:3004");
    }
}