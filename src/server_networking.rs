use std::fmt;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::error::Error;

use crate::db_structure;


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


pub fn parse_instructions(instructions: &String) -> Result<Request, InstructionError> {
    match instructions {
        
        _ => return Err(InstructionError::Invalid(instructions.clone())),
    }

    Ok(Request::Download)
}


pub fn server() -> Result<(), Box<dyn Error>> {
    let l = TcpListener::bind("127.0.0.1:3004")?;

    for stream in l.incoming() {
        println!("Accepted connection");
        std::thread::spawn(|| {
            println!("Spawned thread");
            let mut stream = match stream {
                Ok(value) => {println!("Unwrapped Result"); value},
                Err(e) => panic!("{}", e),
            };

            let mut instructions: [u8;15] = [0;15];
            println!("Initialized string buffer");
            loop {
                match stream.read(&mut instructions) {
                    Ok(n) => {
                        println!("Read {n} bytes");
                        break;
                    },
                    Err(e) => panic!("{e}"),
                };
            }
            
            let mut instruction_string = "".to_owned();
            for byte in instructions {
                if byte == 0 {
                    break;
                }
                instruction_string.push(char::from(byte));
            }
            dbg!(instruction_string.as_bytes());
            println!("{}", &instruction_string);

            /*
            
            parse_instructions(&instruction_string);

             */

            if &instruction_string == "give me five!" {
                println!("matching...");
                match stream.write("FIVE!".as_bytes()) {
                    Ok(n) => println!("Wrote {n} bytes"),
                    Err(e) => panic!("{e}"),
                };
            }

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
        server();
    }
}