use std::collections::HashMap;
use std::fmt;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::error::Error;
use std::sync::{Arc, Mutex};

use crate::db_structure::{self, StrictTable, create_StrictTable_from_csv};


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




pub fn server(address: &str, global: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), Box<dyn Error>> {
    let l = TcpListener::bind(address)?;

    for stream in l.incoming() {
        println!("Accepted connection");
        let thread_global = global.clone();
        std::thread::spawn(move || {
            println!("Spawned thread");
            let mut stream = match stream {
                Ok(value) => {println!("Unwrapped Result"); value},
                Err(e) => panic!("{}", e),
            };

            let mut instructions: [u8; 1024] = [0; 1024];
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
            
            let mut instruction_string = String::new();
            for byte in instructions {
                if byte == 0 {
                    break
                } else {
                    instruction_string.push(char::from(byte));
                }
            }

            if instruction_string == "Sending CSV" {
                match stream.write("OK".as_bytes()) {
                    Ok(n) => println!("Wrote {n} bytes"),
                    Err(e) => panic!("{e}"),
                };
            }
            stream.flush();
            println!("Flushed stream");

            let mut csv = String::new();
            let b: usize;
            loop {
                match stream.read_to_string(&mut csv) {
                    Ok(n) => {
                        b = n;
                        break;
                    },
                    Err(e) => {return Err(e)},
                };
            }

            match create_StrictTable_from_csv(&csv) {
                Ok(table) => {
                    match stream.write(&b.to_be_bytes()) {
                        Ok(_) => println!("Confirmed correctness with client"),
                        Err(e) => {return Err(e)},
                    };
                    //need to append the new table to global data here
                    thread_global.lock().unwrap().insert(table.metadata.name.clone(), table);
                },
                Err(e) => match stream.write(e.to_string().as_bytes()){
                    Ok(_) => println!("Informed client of corruption"),
                    Err(e) => {return Err(e)},
                },
            };


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
        let mut global: HashMap<String, StrictTable> = HashMap::new();
        let arc_global = Arc::new(Mutex::new(global));
        server("127.0.0.1:3004", arc_global.clone());
    }
}