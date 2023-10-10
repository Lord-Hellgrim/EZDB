use std::collections::HashMap;
use std::io::{Write, Read};
use std::net::{TcpListener, IpAddr};
use std::sync::{Arc, Mutex};
use std::str::{self};

use rug::{Integer, Complete};
use rug::integer::Order;

use crate::aes_temp_crypto::decrypt_aes256;
use crate::auth::{User, AuthenticationError};
use crate::diffie_hellman::{DiffieHellman, aes256key, shared_secret};
use crate::logger::{get_current_time, LogTimeStamp};
use crate::networking_utilities::*;
use crate::db_structure::{StrictTable, Actions, StrictError};

pub const CONFIG_FOLDER: &str = "EZconfig/";


pub fn parse_instruction(buffer: &[u8], users: &HashMap<String, User>, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>, aes_key: &[u8]) -> Result<Instruction, ServerError> {

    println!("parsing 1...");
    let instruction = bytes_to_str(&buffer)?;
    println!("Encrypted instructions: {}", instruction);
    let instruction_block: Vec<&str> = instruction.split('|').collect();

    let (ciphertext, nonce) = (decode_hex(instruction_block[0]).unwrap(), decode_hex(instruction_block[1]).unwrap());
    let plaintext = decrypt_aes256(&ciphertext, aes_key, &nonce)?;
    println!("decrypted_instructions: {:x?}", plaintext);
    let instruction = bytes_to_str(&plaintext)?;
    println!("instruction: {}", instruction);
    let instruction_block: Vec<&str> = instruction.split('|').collect();

    println!("parsing 2...");
    if instruction_block.len() != INSTRUCTION_LENGTH {
        return Err(ServerError::Instruction(InstructionError::Invalid("Wrong number of query fields. Query should be usernme, password, request, table_name, query(or blank)".to_owned())));
    }
    
    println!("parsing 3...");
    let (
        username, 
        pass_hash, 
        action, 
        table_name,
        query,
    ) = (
        instruction_block[0], 
        hash_function(instruction_block[1]), 
        instruction_block[2], 
        instruction_block[3],
        instruction_block[4],
    );

    println!("parsing 4...");
    if !users.contains_key(username) {
        return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned())));
    } else if users[username].PasswordHash != pass_hash {
        println!("users.passwordhash:\n{:x?}", users[username].PasswordHash);
        println!("pass_hash:\n{:x?}", pass_hash);
        return Err(ServerError::Authentication(AuthenticationError::WrongPassword(format!("{:x?}", pass_hash))));
    } else {
        match action {
            "Querying" => {
                if !global_tables.lock().unwrap().contains_key(table_name) {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                } else {
                    Ok(Instruction::Query(table_name.to_owned(), query.to_owned()))
                }
            }
            "Uploading" => Ok(Instruction::Upload(table_name.to_owned())),
            "Downloading" => {
                if !global_tables.lock().unwrap().contains_key(table_name) {
                    let raw_table_exists = std::path::Path::new(&format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name)).exists();
                    if raw_table_exists {
                        println!("Loading table from disk");
                        let mut temp = global_tables.lock().unwrap();
                        let disk_table = std::fs::read_to_string(&format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name))?;
                        temp.insert(table_name.to_owned(), StrictTable::from_csv_string(&disk_table, table_name)?);
                        Ok(Instruction::Download(table_name.to_owned()))
                    } else {
                        return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                    }
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


fn handle_download_request(mut connection: Connection, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let mut mutex_binding = global_tables.lock().unwrap();
    let mut requested_table = mutex_binding.get_mut(name).expect("Instruction parser should have verified table");
    let requested_csv = requested_table.to_csv_string();
    println!("Requested_csv: {}", requested_csv);

    let response = data_send_and_confirm(&mut connection, &requested_csv)?;

    if response == "OK" {
        requested_table.metadata.last_access = get_current_time();

        requested_table.metadata.accessed_by
        .entry(connection.peer.to_string())
        .and_modify(|curr| curr.downloaded += 1)
        .or_insert(Actions::first_download());

        requested_table.metadata.times_accessed += 1;
        println!("metadata: {}", requested_table.metadata.to_string());

        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}


fn handle_upload_request(mut connection: Connection, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let (csv, total_read) = receive_data(&mut connection)?;

    // Here we create a StrictTable from the csv and supplied name
    println!("About to check for strictness");
    let instant = std::time::Instant::now();
    match StrictTable::from_csv_string(&csv, name) {
        Ok(mut table) => {
            match connection.stream.write(format!("{}", total_read).as_bytes()) {
                Ok(_) => {
                    println!("Time to check strictness: {}", instant.elapsed().as_millis());
                    println!("Confirmed correctness with client");
                }
                    ,

                Err(e) => {return Err(ServerError::Io(e));},
            };

            println!("Appending to global");
            println!("{:?}", &table.header);
            table.metadata.last_access = get_current_time();
            table.metadata.created_by = connection.peer.to_string();
            table.metadata.accessed_by.insert(connection.peer.to_string(), Actions::new());
        
            table.metadata.times_accessed += 1;
            
            global_tables.lock().unwrap().insert(table.name.clone(), table);

        },
        Err(e) => match connection.stream.write(e.to_string().as_bytes()){
            Ok(_) => println!("Informed client of unstrictness"),
            Err(e) => {return Err(ServerError::Io(e));},
        },
    };
    

    Ok("OK".to_owned())
}
    
    
pub fn handle_update_request(mut connection: Connection, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let (csv, total_read) = receive_data(&mut connection)?;

    let mut mutex_binding = global_tables.lock().unwrap();

    let requested_table = mutex_binding.get_mut(name).expect("Instruction parser should have verified existence of table");
    
    match requested_table.update(&csv) {
        Ok(_) => {
            connection.stream.write(total_read.to_string().as_bytes())?;
        },
        Err(e) => {
            connection.stream.write(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e));
        },
    };

    Ok("OK".to_owned())
}


fn handle_query_request(mut connection: Connection, name: &str, query: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    
    let mutex_binding = global_tables.lock().unwrap();
    let requested_table = mutex_binding.get(name).expect("Instruction parser should have verified table");
    let requested_csv: String;
    // PARSE INSTRUCTION
    let query_type;
    match query.find("..") {
        Some(i) => query_type = "range",
        None => query_type = "list"
    };

    if query_type == "range" {
        let parsed_query: Vec<&str> = query.split("..").collect();
        requested_csv = requested_table.query_range((parsed_query[0], parsed_query[1]))?;
    } else {
        let parsed_query = query.split(',').collect();
        requested_csv = requested_table.query_list(parsed_query)?;
    }

    let response = data_send_and_confirm(&mut connection, &requested_csv)?;
    
    if response == "OK" {
        return Ok("OK".to_owned())
    } else {
        return Err(ServerError::Confirmation(response))
    }
}


pub fn server(address: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    println!("Starting server...\n###########################");
    println!("Binding to address: {address}");
    let server_dh = DiffieHellman::new();
    let server_public_key = Arc::new(server_dh.public_key().to_digits::<u8>(Order::Lsf));
    let server_private_key = Arc::new(server_dh.private_key);
    
    let l = match TcpListener::bind(address) {
        Ok(value) => value,
        Err(e) => {return Err(ServerError::Io(e));},
    };

    // #################################### STARTUP SEQUENCE #############################################
    
    println!("Reading users config into memory");
    
    let mut users: HashMap<String, User> = HashMap::new();
    
    if std::path::Path::new("EZconfig").is_dir() {
        println!("config exists");
        let temp = std::fs::read_to_string(&format!("{CONFIG_FOLDER}.users"))?;
        for line in temp.lines() {
            if line.as_bytes()[0] == '#' as u8 {
                continue
            }
            let t: Vec<&str> = line.split(';').collect();
            users.insert(t[0].to_owned(), User::from_str(line)?);
        }
    } else {
        println!("config does not exist");
        let temp = String::from("admin;d289b2da9b7051f36b4e396e0af3e069e78cf119a7fdcb6437b685c4875e9f9e;127.0.0.1;false;ALL;ALL;true");
        println!("We are not supposed to get here");
        std::fs::create_dir("EZconfig").unwrap();
        std::fs::create_dir("EZconfig/raw_tables").unwrap();
        let mut user_file = match std::fs::File::create(format!("{CONFIG_FOLDER}.users")) {
            Ok(f) => f,
            Err(e) => return Err(ServerError::Strict(StrictError::Io(e.kind()))),
        };
        user_file.write_all(&temp.as_bytes());
        for line in temp.lines() {
            if line.as_bytes()[0] == '#' as u8 {
                continue
            }
            let t: Vec<&str> = line.split(';').collect();
            users.insert(t[0].to_owned(), User::from_str(line)?);
        }
    } 
    
    let mut users = Arc::new(users);

    // #################################### END STARTUP SEQUENCE ###############################################


    // #################################### DATA SAVING AND LOADING LOOP ###################################################

    let data_saving_global_data = global_tables.clone();
    let data_saving_users = users.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(10));
            println!("Background thread running good!...");
            {
                let data = data_saving_global_data.lock().unwrap();
                for (name, table) in data.iter() {
                    match table.save_to_disk_raw(CONFIG_FOLDER) {
                        Ok(_) => (),
                        Err(e) => println!("Unable to save because: {}", e),
                    };
                }
            }
        }

    });

    // #################################### END DATA SAVING AND LOADING LOOP ###############################################


    /* This is the main loop of the function. Here we accept incoming connections and process them */
    loop {
        // Reading instructions
        let (mut stream, client_address) = match l.accept() {
            Ok((n,m)) => (n, m),
            Err(e) => {return Err(ServerError::Io(e));},
        };
        println!("Accepted connection from: {}", client_address);        

        // Spawn a new thread for each connection for some semblence of scalability
        let thread_global = global_tables.clone();
        let thread_users = users.clone();
        let thread_public_key = server_public_key.clone();
        let thread_private_key = server_private_key.clone();
        
        std::thread::spawn(move || {
            
            // loop while connection is still open
            'connection: loop {

                
                stream.write(&thread_public_key)?;
                let mut buffer: [u8; 256] = [0; 256];
                
                stream.read(&mut buffer)?;
                
                let client_public_key = Integer::from_digits(&buffer, Order::Lsf);
                
                let shared_secret = shared_secret(&client_public_key, &thread_private_key);
                let aes_key = aes256key(&shared_secret.to_digits::<u8>(Order::Lsf));
                let mut connection = Connection {
                    stream: stream, 
                    peer: client_address.to_string(), 
                    aes_key: aes_key};

                let mut buffer: [u8; INSTRUCTION_BUFFER] = [0; INSTRUCTION_BUFFER];
                println!("Initialized string buffer");
                
                match connection.stream.read(&mut buffer) {
                    Ok(n) => {
                        println!("Read {n} bytes");
                    },
                    Err(e) => {
                        return Err(ServerError::Io(e));
                    },
                };
                
                
                println!("Parsing instructions...");
                match parse_instruction(&buffer, &thread_users, thread_global.clone(), &connection.aes_key) {
                    Ok(i) => match i {
                        
                        Instruction::Upload(name) => {
                            match handle_upload_request(connection, &name, thread_global.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                    return Ok(());
                                },
                                Err(e) => {return Err(e);}
                            }
                        },
                        Instruction::Download(name) => {
                        match handle_download_request(connection, &name, thread_global.clone()) {
                            Ok(_) => {
                                    println!("Operation finished!");
                                    return Ok(());
                                },
                                Err(e) => {return Err(e);}
                            }
                        }
                        Instruction::Update(name) => {
                            match handle_update_request(connection, &name, thread_global.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                    return Ok(());
                                },
                                Err(e) => {return Err(e);},
                            }
                        }
                        Instruction::Query(table_name, query) => {
                            match handle_query_request(connection, &table_name, &query, thread_global.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                    return Ok(());
                                },
                                Err(e) => {return Err(e);},
                            }
                        }
                    }
                    
                    Err(e) => {
                        connection.stream.write(&e.to_string().as_bytes())?;
                        println!("Thread finished on error: {e}");
                        return Err(e);
                    },
                    
                }
            }
            
        });
        println!("Thread finished!");
        continue;
    }

}


#[cfg(test)]
mod tests {
    use super::*;

    // #[test]
    // fn test_listener() {
    //     let global: HashMap<String, StrictTable> = HashMap::new();
    //     let arc_global = Arc::new(Mutex::new(global));
    //     server("127.0.0.1:3004", arc_global.clone()).unwrap();
    // }
}