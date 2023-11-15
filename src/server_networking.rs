use std::collections::HashMap;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::str::{self};

use rug::Integer;
use rug::integer::Order;

use crate::aes_temp_crypto::decrypt_aes256;
use crate::auth::User;
use crate::diffie_hellman::{DiffieHellman, blake3_hash, shared_secret};
use crate::networking_utilities::*;
use crate::db_structure::{StrictTable, StrictError, Value};
use crate::handlers::*;

pub const CONFIG_FOLDER: &str = "EZconfig/";


pub fn parse_instruction(instructions: &[u8], users: Arc<Mutex<HashMap<String, User>>>, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>, global_kv_table: Arc<Mutex<HashMap<String, Value>>>, aes_key: &[u8]) -> Result<Instruction, ServerError> {

    println!("Decrypting instructions");
    let ciphertext = &instructions[0..instructions.len()-12];
    let nonce = &instructions[instructions.len()-12..];

    let plaintext = decrypt_aes256(&ciphertext, aes_key, &nonce)?;

    let instruction = bytes_to_str(&plaintext)?;


    let instruction_block: Vec<&str> = instruction.split('|').collect();

    println!("parsing 2...");
    if instruction_block.len() != INSTRUCTION_LENGTH {
        return Err(ServerError::Instruction(InstructionError::Invalid("Wrong number of query fields. Query should be usernme, password, request, table_name, query(or blank)".to_owned())));
    }
    
    println!("parsing 3...");
    let (
        action, 
        table_name,
        query,
    ) = (
        instruction_block[0], 
        instruction_block[1],
        instruction_block[2],
    );

    if table_name == "All" {
        return Err(ServerError::Instruction(InstructionError::InvalidTable("Table cannot be called 'All'".to_owned())));
    }

    println!("parsing 4...");
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
        "KvUpload" => {
            if global_kv_table.lock().unwrap().contains_key(table_name) {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' already exists. Use 'update' instead", table_name))));
            } else {
                Ok(Instruction::KvUpload(table_name.to_owned()))
            }
        },
        "KvUpdate" => {
            if !global_kv_table.lock().unwrap().contains_key(table_name) {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' does not exist. Use 'upload' instead", table_name))));
            } else {
                Ok(Instruction::KvUpdate(table_name.to_owned()))
            }
        },
        "KvDownload" => {
            if !global_kv_table.lock().unwrap().contains_key(table_name) {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry does not exist. You asked for '{}'", table_name))));
            } else {
                Ok(Instruction::KvDownload(table_name.to_owned()))
            }
        },
        _ => {return Err(ServerError::Instruction(InstructionError::Invalid(action.to_owned())));},
    }
}




pub fn server(address: &str) -> Result<(), ServerError> {
    // #################################### STARTUP SEQUENCE #############################################
    
    println!("Starting server...\n###########################");
    println!("Binding to address: {address}");

    let thread_pool = rayon::ThreadPoolBuilder::new().build().unwrap();
    
    let server_dh = DiffieHellman::new();
    let server_public_key = Arc::new(server_dh.public_key().to_digits::<u8>(Order::Lsf));
    let server_private_key = Arc::new(server_dh.private_key);
    
    let l = match TcpListener::bind(address) {
        Ok(value) => value,
        Err(e) => {return Err(ServerError::Io(e));},
    };
    
    
    println!("Reading users config into memory");
    
    let global_tables: Arc<Mutex<HashMap<String, StrictTable>>> = Arc::new(Mutex::new(HashMap::new()));
    let global_kv_table: Arc<Mutex<HashMap<String, Value>>> = Arc::new(Mutex::new(HashMap::new()));
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
        let temp = String::from("#username;password_hash;permissions\nadmin;6ef5f331ccc2384c9e744dead5cb61b7e1624b9bf2eaf9b2a1aa8baf4cc0692e;All:All\nguest;0d99d15ec31cb06b828ed4de120e2f82a3b3d1ca716b4fd574159d97f13cf6b3;good_csv:Download,Query-All:Download");
        std::fs::create_dir("EZconfig").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/raw_tables").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/raw_tables-metadata").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/key_value").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/key_value-metadata").expect("Need IO access to initialize database");
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

    dbg!(&users);
    
    let mut users = Arc::new(Mutex::new(users));

    // #################################### END STARTUP SEQUENCE ###############################################


    // #################################### DATA SAVING AND LOADING LOOP ###################################################

    let data_saving_global_data = global_tables.clone();
    let data_saving_users = Arc::clone(&users);
    let data_saving_kv = global_kv_table.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(10));
            println!("Background thread running good!...");
            {
                let data = data_saving_global_data.lock().unwrap();
                for (name, table) in data.iter() {
                    match table.save_to_disk_raw(CONFIG_FOLDER) {
                        Ok(_) => (),
                        Err(e) => println!("Unable to save table {} because: {}", name, e),
                    };
                }
                let user_lock = data_saving_users.lock().unwrap();
                let mut printer = String::new();
                for (_, user) in user_lock.iter() {
                    printer.push_str(&user.to_str());
                    printer.push_str("\n");
                }
                printer.pop();
            }

            {
                let data = data_saving_kv.lock().unwrap();
                for (key, value) in data.iter() {
                    match value.save_to_disk_raw(key, CONFIG_FOLDER) {
                        Ok(_) => (),
                        Err(e) => println!("Unable to save value of key '{}' because: {}",key, e),
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
        
        let thread_global_tables = global_tables.clone();
        let thread_global_kv_table = global_kv_table.clone();
        let mut thread_users = Arc::clone(&users);
        let thread_public_key = server_public_key.clone();
        let thread_private_key = server_private_key.clone();
        

        // Spawn a thread to handle establishing connections
        thread_pool.spawn(move || {

            // ################## ESTABLISHING ENCRYPTED CONNECTION ##########################################################################################################
            match stream.write(&thread_public_key) {
                Ok(n) => (),
                Err(e) => {
                    println!("failed to write server public key because: {}", e);
                    return
                }
            }
            println!("About to get crypto");
            let mut buffer: [u8; 256] = [0; 256];
            
            match stream.read_exact(&mut buffer){
                Ok(n) => (),
                Err(e) => {
                    println!("failed to read client public key because: {}", e);
                    return
                }
            }
            
            let client_public_key = Integer::from_digits(&buffer, Order::Lsf);
            
            let shared_secret = shared_secret(&client_public_key, &thread_private_key);
            let aes_key = blake3_hash(&shared_secret.to_digits::<u8>(Order::Lsf));

            let mut auth_buffer = [0u8; 1052];
            println!("About to read auth string");
            match stream.read_exact(&mut auth_buffer) {
                Ok(n) => (),
                Err(e) => {
                    println!("failed to read auth_string because: {}", e);
                    return
                }
            }
            // println!("encrypted auth_buffer: {:x?}", auth_buffer);
            // println!("Encrypted auth_buffer.len(): {}", auth_buffer.len());

            let (ciphertext, nonce) = (&auth_buffer[0..auth_buffer.len()-12], &auth_buffer[auth_buffer.len()-12..auth_buffer.len()]);
            println!("About to decrypt auth string");
            let auth_string = match decrypt_aes256(ciphertext, &aes_key, nonce) {
                Ok(s) => s,
                Err(e) => {
                    println!("failed to decrypt auth string because: {}", e);
                    return
                }
            };
            println!("About to parse auth_string");
            let username = match bytes_to_str(&auth_string[0..512]) {
                Ok(s) => s,
                Err(e) => {
                    println!("failed to read auth_string from bytes because: {}", e);
                    return
                }
            };
            let password = &auth_string[512..];

            // println!("username: {}\npassword: {:x?}", username, password);
            let password = blake3_hash(&password);
            // println!("password_hash: {:x?}", password);
            println!("About to verify username and password");
            
            let mut connection: Connection;
            {
                let thread_users_lock = thread_users.lock().unwrap();
                if !thread_users_lock.contains_key(username) {
                    println!("Username:\n\t{}\n...is wrong", username);
                    return 
                } else if thread_users_lock[username].Password != password {
                    println!("Password hash:\n\t{:?}\n...is wrong", password);
                    return
                }
                
                let peer_addr = match stream.peer_addr() {
                    Ok(addr) => addr,
                    Err(e) => {
                        println!("failed to get peer_addr because: {}", e);
                        return
                    }
                };
                connection = Connection {
                    stream: stream, 
                    peer: thread_users_lock[username].clone(), 
                    aes_key: aes_key
                };
            }

            // ############################ END OF ESTABLISHING ENCRYPTED CONNECTION ###################################################################################


            // ############################ HANDLING REQUESTS ###########################################################################################################
            let mut instruction_size = 0;

            let mut buffer: [u8; INSTRUCTION_BUFFER] = [0; INSTRUCTION_BUFFER];
            println!("Initialized string buffer");
            
            while instruction_size == 0 {
                match connection.stream.read(&mut buffer) {
                    Ok(n) => instruction_size = n,
                    Err(e) => {
                        println!("There was an io error during a large read");
                        return
                    },
                };
            }
            
            // println!("Instruction buffer[0..50]: {:x?}", &buffer[0..50]);
            let instructions = &buffer[0..instruction_size];
            
            println!("Parsing instructions...");
            match parse_instruction(instructions, thread_users.clone(), thread_global_tables.clone(), thread_global_kv_table.clone(), &connection.aes_key) {
                Ok(i) => match i {
                    
                    Instruction::Download(name) => {
                        match handle_download_request(&mut connection, &name, thread_global_tables.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                                return
                            }
                        }
                    },
                    Instruction::Upload(name) => {
                        match handle_upload_request(&mut connection, &name, thread_global_tables.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                                return
                            }
                        }
                    },
                    Instruction::Update(name) => {
                        match handle_update_request(&mut connection, &name, thread_global_tables.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                                return
                            },
                        }
                    },
                    Instruction::Query(table_name, query) => {
                        match handle_query_request(&mut connection, &table_name, &query, thread_global_tables.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                                return
                            },
                        }
                    },
                    Instruction::NewUser(user_string) => {
                        match handle_new_user_request(&user_string, thread_users.clone()) {
                            Ok(_) => {
                                println!("New user added!");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                                return
                            },
                        }
                        
                    },
                    Instruction::KvUpload(table_name) => {
                        let check_global_kv  = thread_global_kv_table.clone(); 
                        match handle_kv_upload(&mut connection, &table_name, thread_global_kv_table.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                                println!("kv result: {:x?}", check_global_kv.lock().unwrap().get("test_key").unwrap().body);
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                                return
                            },
                        }
                    },
                    Instruction::KvUpdate(table_name) => {
                        match handle_kv_update(&mut connection, &table_name, thread_global_kv_table.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                                return
                            },
                        }
                    },
                    Instruction::KvDownload(table_name) => {
                        match handle_kv_download(&mut connection, &table_name, thread_global_kv_table.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                                return
                            },
                        }
                    },
                },
                
                Err(e) => {
                    match connection.stream.write(&e.to_string().as_bytes()){
                        Ok(n) => (),
                        Err(e) => {
                            println!("failed to write error message because: {}", e);
                            return
                        }
                    }
                    println!("Thread finished on error: {e}");
                    return
                },
                
            };

            //####################### END OF HANDLING REQUESTS #############################################################################################################
        });

    ()

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
}}