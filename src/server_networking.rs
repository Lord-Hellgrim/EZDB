use std::collections::HashMap;
use std::fmt;
use std::io::{Write, Read};
use std::net::{TcpListener, TcpStream, IpAddr};
use std::sync::{Arc, Mutex};
use std::str::{self, Utf8Error};

use crate::auth::{self, authenticate_client, User, AuthenticationError};
use crate::networking_utilities::bytes_to_str;
use crate::db_structure::StrictTable;


const INSTRUCTION_BUFFER: usize = 1024;
const CSV_BUFFER: usize = 1_000_000;
const MIN_INSTRUCTION_LENGTH: usize = 4;
const MAX_INSTRUCTION_LENGTH: usize = 4;


pub enum Instruction {
    Upload(String),
    Download(String),
    Update(String),
}

#[derive(Debug)]
pub enum ServerError {
    Utf8(Utf8Error),
    Io(std::io::Error),
    Instruction(InstructionError),
    Confirmation(Vec<u8>),
    Authentication(AuthenticationError)
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServerError::Utf8(e) => write!(f, "Encontered invalid utf-8: {}", e),
            ServerError::Io(e) => write!(f, "Encountered an IO error: {}", e),
            ServerError::Instruction(e) => write!(f, "{}", e),
            ServerError::Confirmation(e) => write!(f, "Received corrupt confirmation {:?}", e),
            ServerError::Authentication(e) => write!(f, "{}", e),
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

impl From<InstructionError> for ServerError {
    fn from(e: InstructionError) -> Self {
        ServerError::Instruction(e)
    }
}

impl From<AuthenticationError> for ServerError {
    fn from(e: AuthenticationError) -> Self {
        ServerError::Authentication(e)
    }
}


#[derive(Debug, PartialEq, Clone)]
pub enum InstructionError {
    Invalid(String),
    TooLong,
    Utf8(Utf8Error),
    InvalidTable(String),
}

impl fmt::Display for InstructionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            InstructionError::Invalid(instruction) => write!(f, "The instruction:\n\n\t{instruction}\n\nis invalid. See documentation for valid buffer\n\n"),
            InstructionError::TooLong => write!(f, "Your instruction is too long. Maximum instruction length is: {INSTRUCTION_BUFFER}\n\n"),
            InstructionError::Utf8(e) => write!(f, "Invalid utf-8: {e}"),
            InstructionError::InvalidTable(s) => write!(f, "Table: {} does not exist.", s),
        }
    }
}

impl From<Utf8Error> for InstructionError {
    fn from(e: Utf8Error) -> Self {
        InstructionError::Utf8(e)
    }
}

pub fn parse_instruction(buffer: &[u8], users: &HashMap<String, User>, global: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<Instruction, ServerError> {

    println!("parsing 1...");
    let instruction = bytes_to_str(&buffer)?;
    let instruction_block: Vec<&str> = instruction.split('|').collect();

    println!("parsing 2...");
    if instruction_block.len() < MIN_INSTRUCTION_LENGTH {
        return Err(ServerError::Authentication(AuthenticationError::MissingField));
    } else if instruction_block.len() > MAX_INSTRUCTION_LENGTH {
        return Err(ServerError::Instruction(InstructionError::Invalid(instruction.to_owned())));
    }
    
    println!("parsing 3...");
    let (
        username, 
        passHash, 
        action, 
        tableName
    ) = (
        instruction_block[0], 
        instruction_block[1], 
        instruction_block[2], 
        instruction_block[3],
    );

    println!("parsing 4...");
    if !users.contains_key(username) {
        return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned())));
    } else if users[username].PasswordHash != passHash {
        return Err(ServerError::Authentication(AuthenticationError::WrongPassword(passHash.to_owned())));
    } else {
        match action {
            "Sending" => Ok(Instruction::Upload(tableName.to_owned())),
            "Requesting" => {
                if !global.lock().unwrap().contains_key(tableName) {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(tableName.to_owned())));
                } else {
                    Ok(Instruction::Download(tableName.to_owned()))
                }
            },
            "Updating" => {
                if !global.lock().unwrap().contains_key(tableName) {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(tableName.to_owned())));
                } else {
                    Ok(Instruction::Update(tableName.to_owned()))
                }
            },
            _ => {return Err(ServerError::Instruction(InstructionError::Invalid(action.to_owned())));},
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
            thread_global.lock().unwrap().insert(table.name.clone(), table);
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
    println!("Starting server...\n###########################");
    println!("Binding to address: {address}");
    let l = match TcpListener::bind(address) {
        Ok(value) => value,
        Err(e) => {return Err(ServerError::Io(e));},
    };

    println!("Reading users config into memory");
    let temp = std::fs::read_to_string("users.txt").unwrap();
    let mut users = HashMap::new();
    for line in temp.lines() {
        if line.as_bytes()[0] == '#' as u8 {
            continue
        }
        let t: Vec<&str> = line.split(';').collect();
        users.insert(t[0].to_owned(), User::from_str(line));
    }
    let users = Arc::new(users);

    dbg!(&users);
    
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
        let thread_users = users.clone();
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

            println!("Parsing instructions...");
            match parse_instruction(&buffer, &thread_users, thread_global.clone()) {
                Ok(I) => match I {

                    Instruction::Upload(name) => {
                        match handle_sending_csv(stream, &name, thread_global.clone()) {
                            Ok(_) => {
                                println!("Thread finished!");
                                return Ok(());
                            },
                            Err(e) => {return Err(e);}
                        }
                    },
                    Instruction::Download(name) => {
                        match handle_requesting_csv(stream, &name, thread_global.clone()) {
                            Ok(_) => {
                                println!("Thread finished!");
                                return Ok(());
                            },
                            Err(e) => {return Err(e);}
                        }
                    }
                    Instruction::Update(name) => todo!(),
                }
                
                Err(e) => {
                    println!("Thread finished on error: {e}");
                    return Err(e);
                },
                    
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