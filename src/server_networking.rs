use std::collections::HashMap;
use std::{fmt, error};
use std::io::{Write, Read};
use std::net::{TcpListener, TcpStream};
use std::error::Error;
use std::sync::{Arc, Mutex};
use std::str::{self, Utf8Error};

use crate::networking_utilities::bytes_to_str;
use crate::client_networking::ConnectionError;
use crate::db_structure::{self, StrictTable, StrictError};


const INSTRUCTION_BUFFER: usize = 1024;
const CSV_BUFFER: usize = 1_000_000;

pub enum Request {
    Upload,
    Download(String)
}

#[derive(Debug)]
pub enum ServerError {
    Utf8(Utf8Error),
    Io(std::io::Error),
    Instruction(InstructionError),
    Confirmation(Vec<u8>),
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServerError::Utf8(e) => write!(f, "Encontered invalid utf-8: {}", e),
            ServerError::Io(e) => write!(f, "Encountered an IO error: {}", e),
            ServerError::Instruction(e) => write!(f, "{}", e),
            ServerError::Confirmation(e) => write!(f, "Received corrupt confirmation {:?}", e),
        }
    }
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        ServerError::Io(e)
    }
}

impl From<Utf8Error> for ServerError {
    fn from(e: Utf8Error) -> Self {
        ServerError::Utf8(e)
    }
}


#[derive(Debug, PartialEq, Clone)]
pub enum InstructionError {
    Invalid(String),
    TooLong,
}

impl fmt::Display for InstructionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InstructionError::Invalid(instruction) => write!(f, "The instruction:\n\n\t{instruction}\n\nis invalid. See documentation for valid buffer\n\n"),
            InstructionError::TooLong => write!(f, "Your buffer are too long. Maximum instruction length is: {INSTRUCTION_BUFFER}\n\n"),
        }
    }
}


fn handle_sending_csv(mut stream: TcpStream, name: &str, thread_global: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {

    match stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    // Here we read the transmitted CSV from the stream into a rust String (aka a Vec)
    println!("Allocating csv buffer");
    let mut buffer = [0;CSV_BUFFER];
    let b: usize;
    loop {
        match stream.read(&mut buffer) {
            Ok(n) => {
                b = n;
                println!("Read {n} bytes");
                break;},
            Err(e) => {return Err(ServerError::Io(e));},
        };
    }

    let csv = bytes_to_str(&buffer)?;

    // Here we create a StrictTable from the csv and supplied name
    match StrictTable::from_csv_string(csv, name) {
        Ok(table) => {
            match stream.write(format!("X{}X", b).as_bytes()) {
                Ok(_) => println!("Confirmed correctness with client"),
                Err(e) => {return Err(ServerError::Io(e));},
            };

            //need to append the new table to global data here
            println!("Appending to global");
            println!("{:?}", &table.table);
            thread_global.lock().unwrap().insert(table.metadata.name.clone(), table);
            // This is just to check whether it worked
            // let check = &*thread_global;
            // let check_guard = check.lock().unwrap();
            // let map = &*check_guard;
            // println!("Printing global data:\n\n{:?}", map["test"]);
        },
        Err(e) => match stream.write(e.to_string().as_bytes()){
            Ok(_) => println!("Informed client of corruption"),
            Err(e) => {return Err(ServerError::Io(e));},
        },
    };

    Ok(())
}


fn handle_requesting_csv(mut stream: TcpStream, name: &str, thread_global: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    
    let requested_table = thread_global.lock().unwrap();
    let requested_table = match requested_table.get(name) {
        Some(table) => table,
        None => {stream.write("No such table".as_bytes());
            return Err(ServerError::Instruction(InstructionError::Invalid(format!("No table named {}", name).to_owned())));
        },
    };

    match stream.write(requested_table.to_csv_string().as_bytes()) {
        Ok(n) => println!("Wrote requested csv as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    }

    // Waiting for confirmation from client
    let mut buffer: [u8;INSTRUCTION_BUFFER] = [0;INSTRUCTION_BUFFER];
    loop {
        match stream.read(&mut buffer) {
            Ok(n) => {
                println!("Confirmation '{}' received", bytes_to_str(&buffer)?);
                break;
            },
            Err(_) => println!("Did not confirm transmission with client"),
        }
    }

    let confirmation = bytes_to_str(&buffer)?;

    if confirmation == "OK" {
        Ok(())
    } else {
        Err(ServerError::Confirmation(Vec::from(confirmation)))
    }
}


pub fn server(address: &str, global: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    let l = match TcpListener::bind(address) {
        Ok(value) => value,
        Err(e) => {return Err(ServerError::Io(e));},
    };

    
    /* This is the main loop of the function. Here we accept incoming connections and process them */
    // for stream in l.incoming() {
    loop {
        let (mut stream, client_address) = match l.accept() {
            Ok((n,m)) => (n, m),
            Err(e) => {return Err(ServerError::Io(e));},
        };
        println!("Accepted connection from: {}", client_address);

        // Spawn a new thread for each connection for some semblence of scalability
        let thread_global = global.clone();
        std::thread::spawn(move || {

            let mut buffer: [u8; INSTRUCTION_BUFFER] = [0; INSTRUCTION_BUFFER];
            println!("Initialized string buffer");
            loop {
                match stream.read(&mut buffer) {
                    Ok(n) => {
                        println!("Read {n} bytes");
                        break;
                    },
                    Err(e) => {return Err(ServerError::Io(e));},
                };
            }
            
            let instruction = match bytes_to_str(&buffer) {
                Ok(value) => {
                    println!("{}", value);
                    value
                },
                Err(e) => {return Err(ServerError::Utf8(e));},
            };

            let instruction: Vec<&str> = instruction.split('|').collect();
            println!("{}", instruction.len());
            if instruction.len() != 2 {
                return Err(ServerError::Instruction(InstructionError::Invalid(instruction[0].to_owned())));
            }
            let (instruction, name) = (instruction[0], instruction[1]);

            // Here we parse the instructions. I would like to figure out how to make this a function that propagates an InstructionError
            if instruction == "Sending csv" {
                match handle_sending_csv(stream, name, thread_global.clone()) {
                    Ok(_) => {
                        println!("Thread finished!");
                        return Ok(());
                    },
                    Err(e) => {return Err(e);}
                }
            } else if instruction == "Requesting csv" {
                match handle_requesting_csv(stream, name, thread_global.clone()) {
                    Ok(_) => {
                        println!("Thread finished!");
                        return Ok(());
                    },
                    Err(e) => {return Err(e);}
                }
            } else {
                stream.write("Invalid request".as_bytes()).expect("Panicked while informing client of invalid request");
                return Err(ServerError::Instruction(InstructionError::Invalid(instruction.to_owned())));
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
        let mut global: HashMap<String, StrictTable> = HashMap::new();
        let arc_global = Arc::new(Mutex::new(global));
        server("127.0.0.1:3004", arc_global.clone());
    }
}