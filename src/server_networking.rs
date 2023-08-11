use std::collections::HashMap;
use std::fmt;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::str::{self, Utf8Error};

use crate::client_networking::ConnectionError;
use crate::db_structure::{self, StrictTable, create_StrictTable_from_csv, StrictError};


const MAX_INSTRUCTION_LENGTH: usize = 1024;

pub enum Request {
    Upload,
    Download(String)
}

pub enum ServerError {
    Utf8(Utf8Error),
    Io(std::io::Error),
    Instruction(InstructionError),
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


pub fn parse_instruction(s: &str) -> Result<Request, InstructionError> {
    match s {
        "Sending CSV" => Ok(Request::Upload),
        _ => Err(InstructionError::Invalid(s.to_owned())),
    }
}




pub fn server(address: &str, global: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), Box<dyn Error>> {
    let l = TcpListener::bind(address)?;

    
    /* This is the main loop of the function. Here we accept incoming connections and process them */
    for stream in l.incoming() {
        println!("Accepted connection");

        // Spawn a new thread for each connection for some semblence of scalability
        let thread_global = global.clone();
        std::thread::spawn(move || {

            /* The first thing the thread does is match on the accepted connection and return a ServerError if there is a problem */
            println!("Spawned thread");
            let mut stream = match stream {
                Ok(value) => {println!("Unwrapped Result"); value},
                Err(e) => {return Err(ServerError::Io(e));},
            };

            /* Then we allocate an instruction buffer. It's size should vary according to the incoming transmission. It doesn't currently */
            let mut instructions: [u8; 1024] = [0; 1024];
            println!("Initialized string buffer");
            loop {
                /* This loop should handle differentl sized transmissions and break when there is no more data */
                match stream.read(&mut instructions) {
                    Ok(n) => {
                        println!("Read {n} bytes");
                        break;
                    },
                    Err(e) => {return Err(ServerError::Io(e));},
                };
            }
            
            /* Depending on the instructions received, a different action should be taken */
            let instruction = match str::from_utf8(&instructions) {
                Ok(value) => {
                    println!("{}", value);
                    value
                },
                Err(e) => {return Err(ServerError::Utf8(e));},
            };

            let instruction: Vec<&str> = instruction.split('|').collect();
            if instruction.len() != 2 {
                return Err(ServerError::Instruction(InstructionError::Invalid(instruction[0].to_owned())));
            }
            let (instruction, name) = (instruction[0], instruction[1]);

            // Here we parse the instructions. I would like to figure out how to make this a function that propagates an InstructionError
            if instruction == "Sending CSV" {
                match stream.write("OK".as_bytes()) {
                    Ok(n) => println!("Wrote {n} bytes"),
                    Err(e) => {return Err(ServerError::Io(e));},
                };
            } else {
                stream.write("Invalid request".as_bytes()).expect("Panicked while informing client of invalid request");
                return Err(ServerError::Instruction(InstructionError::Invalid(instruction.to_owned())));
            }

            // Here we read the transmitted CSV from the stream into a rust String (aka a Vec)
            let mut csv = String::from("");
            let b: usize;
            loop {
                match stream.read_to_string(&mut csv) {
                    Ok(n) => {
                        b = n;
                        println!("{}", &csv);
                        break;
                    },
                    Err(e) => {return Err(ServerError::Io(e));},
                };
            }

            // Here we create a StrictTable from the csv and supplied name
            match create_StrictTable_from_csv(&csv, name) {
                Ok(table) => {
                    match stream.write(&b.to_be_bytes()) {
                        Ok(_) => println!("Confirmed correctness with client"),
                        Err(e) => {return Err(ServerError::Io(e));},
                    };
                    //need to append the new table to global data here
                    thread_global.lock().unwrap().insert(table.metadata.name.clone(), table);
                },
                Err(e) => match stream.write_all(e.to_string().as_bytes()){
                    Ok(_) => println!("Informed client of corruption"),
                    Err(e) => {return Err(ServerError::Io(e));},
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