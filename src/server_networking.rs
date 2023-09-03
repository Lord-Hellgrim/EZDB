use std::collections::HashMap;
use std::io::{Write, Read};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::str::{self};

use crate::auth::{User, AuthenticationError};
use crate::networking_utilities::*;
use crate::db_structure::StrictTable;


pub fn parse_instruction(buffer: &[u8], users: &HashMap<String, User>, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<Instruction, ServerError> {

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
        pass_hash, 
        action, 
        table_name
    ) = (
        instruction_block[0], 
        hash_function(instruction_block[1]), 
        instruction_block[2], 
        instruction_block[3],
    );

    println!("parsing 4...");
    if !users.contains_key(username) {
        return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned())));
    } else if users[username].PasswordHash != pass_hash {
        return Err(ServerError::Authentication(AuthenticationError::WrongPassword(pass_hash.to_owned())));
    } else {
        match action {
            "Sending" => Ok(Instruction::Upload(table_name.to_owned())),
            "Requesting" => {
                if !global_tables.lock().unwrap().contains_key(table_name) {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                } else {
                    Ok(Instruction::Download(table_name.to_owned()))
                }
            },
            "Updating" => {
                if !global_tables.lock().unwrap().contains_key(table_name) {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                } else {
                    Ok(Instruction::Update(table_name.to_owned()))
                }
            },
            _ => {return Err(ServerError::Instruction(InstructionError::Invalid(action.to_owned())));},
        }
    }
}


fn handle_upload_request(mut stream: TcpStream, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {

    match stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let (csv, total_read) = receive_data(&mut stream)?;

    // Here we create a StrictTable from the csv and supplied name
    match StrictTable::from_csv_string(&csv, name) {
        Ok(table) => {
            match stream.write(format!("{}", total_read).as_bytes()) {
                Ok(_) => println!("Confirmed correctness with client"),
                Err(e) => {return Err(ServerError::Io(e));},
            };

            println!("Appending to global");
            println!("{:?}", &table.header);
            global_tables.lock().unwrap().insert(table.name.clone(), table);

        },
        Err(e) => match stream.write(e.to_string().as_bytes()){
            Ok(_) => println!("Informed client of corruption"),
            Err(e) => {return Err(ServerError::Io(e));},
        },
    };

    Ok(())
}


fn handle_download_request(mut stream: TcpStream, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    
    let requested_table = global_tables.lock().unwrap();
    let requested_table = match requested_table.get(name) {
        Some(table) => table,
        None => {stream.write(format!("No table named: {}", name).as_bytes())?;
            return Err(ServerError::Instruction(InstructionError::Invalid(format!("No table named {}", name).to_owned())));
        },
    };
    let requested_csv = requested_table.to_csv_string();



    let response = send_data(&mut stream, &requested_csv)?;

    if response == "OK" {
        return Ok(())
    } else {
        return Err(ServerError::Confirmation(Vec::from(response)))
    }

}
    
    
pub fn handle_update_request(mut stream: TcpStream, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    
    let requested_table = global_tables.lock().unwrap();
    if !requested_table.contains_key(name) {
        stream.write(format!("No table named: {}", name).as_bytes())?;
        return Err(ServerError::Instruction(InstructionError::Invalid(format!("No table named {}", name).to_owned())));   
    }
    todo!();

    //requested_table.get_mut(name).unwrap().update(updates)?;

}


pub fn server(address: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    println!("Starting server...\n###########################");
    println!("Binding to address: {address}");
    let l = match TcpListener::bind(address) {
        Ok(value) => value,
        Err(e) => {return Err(ServerError::Io(e));},
    };

    println!("Reading users config into memory");
    let temp = std::fs::read_to_string("users.txt")?;
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
        let thread_global = global_tables.clone();
        let thread_users = users.clone();
        std::thread::spawn(move || {

            let mut buffer: [u8; INSTRUCTION_BUFFER] = [0; INSTRUCTION_BUFFER];
            println!("Initialized string buffer");
            match stream.read(&mut buffer) {
                Ok(n) => {
                    println!("Read {n} bytes");
                },
                Err(e) => {return Err(ServerError::Io(e));},
            };

            println!("Parsing instructions...");
            match parse_instruction(&buffer, &thread_users, thread_global.clone()) {
                Ok(i) => match i {

                    Instruction::Upload(name) => {
                        match handle_upload_request(stream, &name, thread_global.clone()) {
                            Ok(_) => {
                                println!("Thread finished!");
                                return Ok(());
                            },
                            Err(e) => {return Err(e);}
                        }
                    },
                    Instruction::Download(name) => {
                        match handle_download_request(stream, &name, thread_global.clone()) {
                            Ok(_) => {
                                println!("Thread finished!");
                                return Ok(());
                            },
                            Err(e) => {return Err(e);}
                        }
                    }
                    Instruction::Update(name) => {
                        match handle_update_request(stream, &name, thread_global.clone()) {
                            Ok(_) => todo!(),
                            Err(_) => todo!(),
                        }
                    }
                }
                
                Err(e) => {
                    stream.write(&e.to_string().as_bytes())?;
                    println!("Thread finished on error: {e}");
                    return Err(e);
                },
                    
            }
            
        });
        continue;
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listener() {
        let global: HashMap<String, StrictTable> = HashMap::new();
        let arc_global = Arc::new(Mutex::new(global));
        server("127.0.0.1:3004", arc_global.clone()).unwrap();
    }
}