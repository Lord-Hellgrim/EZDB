use std::collections::HashMap;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::sync::{Arc, Mutex};
use std::str::{self};

use aes_gcm::Key;
use smartstring::{SmartString, LazyCompact};
use x25519_dalek::{StaticSecret, PublicKey};

pub type KeyString = SmartString<LazyCompact>;

use crate::aes_temp_crypto::decrypt_aes256;
use crate::auth::{User, AuthenticationError, user_has_permission};
use crate::networking_utilities::*;
use crate::db_structure::{ColumnTable, StrictError, Value};
use crate::handlers::*;

pub const CONFIG_FOLDER: &str = "EZconfig/";

/// Parses the inctructions sent by the client. Will be rewritten soon to accomodate EZQL
pub fn parse_instruction(instructions: &[u8], users: Arc<Mutex<HashMap<KeyString, User>>>, global_tables: Arc<Mutex<HashMap<KeyString, ColumnTable>>>, global_kv_table: Arc<Mutex<HashMap<KeyString, Value>>>, aes_key: &[u8]) -> Result<Instruction, ServerError> {

    println!("Decrypting instructions");
    let ciphertext = &instructions[0..instructions.len()-12];
    let nonce = &instructions[instructions.len()-12..];

    let plaintext = decrypt_aes256(ciphertext, aes_key, nonce)?;

    let instruction = bytes_to_str(&plaintext)?;
    println!("instruction: {}", instruction);


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
        username,
    ) = (
        instruction_block[0], 
        instruction_block[1],
        instruction_block[2],
        instruction_block[3],
    );

    if table_name == "All" {
        return Err(ServerError::Instruction(InstructionError::InvalidTable("Table cannot be called 'All'".to_owned())));
    }

    println!("parsing 4...");
    match action {
        "Querying" => {
            if !global_tables.lock().unwrap().contains_key(table_name) {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
            } else if user_has_permission(table_name, action, username, users) {
                
                return Ok(Instruction::Query(table_name.to_owned(), query.to_owned()));
                
            } else {
                return Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        }
        "Uploading" => {
            if user_has_permission(table_name, action, username, users) {
                return Ok(Instruction::Upload(table_name.to_owned()));
            } else {
                return Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        } 
        "Downloading" => {
            if !global_tables.lock().unwrap().contains_key(table_name) {
                let raw_table_exists = std::path::Path::new(&format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name)).exists();
                if raw_table_exists {
                    println!("Loading table from disk");
                    let mut temp = global_tables.lock().unwrap();
                    let disk_table = std::fs::read_to_string(format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name))?;
                    let disk_table = ColumnTable::from_csv_string(&disk_table, table_name, "temp")?;
                    temp.insert(KeyString::from(table_name), disk_table);
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
                let raw_table_exists = std::path::Path::new(&format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name)).exists();
                if raw_table_exists {
                    println!("Loading table from disk");
                    let mut temp = global_tables.lock().unwrap();
                    let disk_table = std::fs::read_to_string(format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name))?;
                    let disk_table = ColumnTable::from_csv_string(&disk_table, table_name, "temp")?;
                    temp.insert(KeyString::from(table_name), disk_table);
                    Ok(Instruction::Update(table_name.to_owned()))
                } else {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                }
            } else {
                Ok(Instruction::Update(table_name.to_owned()))
            }
        },
        "Deleting" => {
            if !global_tables.lock().unwrap().contains_key(table_name) {
                let raw_table_exists = std::path::Path::new(&format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name)).exists();
                if raw_table_exists {
                    println!("Loading table from disk");
                    let mut temp = global_tables.lock().unwrap();
                    let disk_table = std::fs::read_to_string(format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name))?;
                    let disk_table = ColumnTable::from_csv_string(&disk_table, table_name, "temp")?;
                    temp.insert(KeyString::from(table_name), disk_table);
                    Ok(Instruction::Download(table_name.to_owned()))
                } else {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                }
            } else {
                Ok(Instruction::Delete(table_name.to_owned(), query.to_owned()))
            }
        }
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
        "MetaListTables" => {
            Ok(Instruction::MetaListTables)
        },
        "MetaListKeyValues" => {
            Ok(Instruction::MetaListKeyValues)
        },
        _ => {return Err(ServerError::Instruction(InstructionError::Invalid(action.to_owned())));},
    }
}

/// The struct that carries data relevant to the running server. 
/// Am trying to think of ways to reduce reliance on Arc<Mutex<T>>
pub struct Server {
    public_key: PublicKey,
    private_key: StaticSecret,
    listener: TcpListener,
    tables: Arc<Mutex<HashMap<KeyString, ColumnTable>>>,
    kv_list: Arc<Mutex<HashMap<KeyString, Value>>>,
    users: HashMap<KeyString, User>,
}

/// The main loop of the server. Checks for incoming connections, parses their instructions, and handles them
/// Also writes tables to disk in a super primitive way. Basically a separate thread writes all the tables to disk
/// every 10 seconds. This will be improved but I would appreciate some advice here.
pub fn run_server(address: &str) -> Result<(), ServerError> {
    
    // #################################### STARTUP SEQUENCE #############################################
    println!("Starting server...\n###########################");
        let server_private_key = StaticSecret::random();
        let server_public_key = PublicKey::from(&server_private_key);
        
        println!("Binding to address: {address}");
        let l = match TcpListener::bind(address) {
            Ok(value) => value,
            Err(e) => {return Err(ServerError::Io(e.kind()));},
        };

        let global_tables: Arc<Mutex<HashMap<KeyString, ColumnTable>>> = Arc::new(Mutex::new(HashMap::new()));
        let global_kv_table: Arc<Mutex<HashMap<KeyString, Value>>> = Arc::new(Mutex::new(HashMap::new()));
        let users: HashMap<KeyString, User> = HashMap::new();

        let mut server = Server {
            public_key: server_public_key,
            private_key: server_private_key,
            listener: l,
            tables: global_tables,
            kv_list: global_kv_table,
            users: users,
        };

    
    println!("Reading users config into memory");
    if std::path::Path::new("EZconfig").is_dir() {
        println!("config exists");
        let temp = std::fs::read_to_string(&format!("{CONFIG_FOLDER}.users"))?;
        for line in temp.lines() {
            if line.as_bytes()[0] == b'#' {
                continue
            }
            let temp_user: User = ron::from_str(line).unwrap();
            server.users.insert(temp_user.username.clone(), temp_user);
        }
    } else {
        println!("config does not exist");
        let temp = ron::to_string(&User::admin("admin", "admin")).unwrap();
        std::fs::create_dir("EZconfig").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/raw_tables").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/raw_tables-metadata").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/key_value").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/key_value-metadata").expect("Need IO access to initialize database");
        let mut user_file = match std::fs::File::create(format!("{CONFIG_FOLDER}.users")) {
            Ok(f) => f,
            Err(e) => return Err(ServerError::Strict(StrictError::Io(e.kind()))),
        };
        match user_file.write_all(temp.as_bytes()) {
            Ok(_) => (),
            Err(e) => panic!("failed to create config file. Server cannot run.\n\nError cause was:\n{e}"),
        };

        server.users.insert(KeyString::from("admin"), User::admin("admin", "admin"));
    } 

    dbg!(&server.users);
    
    let users = Arc::new(Mutex::new(server.users));

    // #################################### END STARTUP SEQUENCE ###############################################


    // #################################### DATA SAVING AND LOADING LOOP ###################################################

    let data_saving_global_data = server.tables.clone();
    let data_saving_users = Arc::clone(&users);
    let data_saving_kv = server.kv_list.clone();
    std::thread::spawn(move || {
        loop {
            std::thread::sleep(std::time::Duration::from_secs(10));
            println!("Background thread running good!...");
            {
                let data = data_saving_global_data.lock().unwrap();
                for (name, table) in data.iter() {
                    match table.save_to_disk_csv(CONFIG_FOLDER) {
                        Ok(_) => (),
                        Err(e) => println!("Unable to save table {} because: {}", name, e),
                    };
                }
                let user_lock = data_saving_users.lock().unwrap();
                let mut printer = String::new();
                for (_, user) in user_lock.iter() {
                    printer.push_str(&ron::to_string(&user).unwrap());
                    printer.push('\n');
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
        let (mut stream, client_address) = match server.listener.accept() {
            Ok((n,m)) => (n, m),
            Err(e) => {return Err(ServerError::Io(e.kind()));},
        };
        println!("Accepted connection from: {}", client_address);        
        
        let thread_global_tables = server.tables.clone();
        let thread_global_kv_table = server.kv_list.clone();
        let thread_users = Arc::clone(&users);
        let thread_public_key = server.public_key;
        let thread_private_key = server.private_key.clone();
        

        // Spawn a thread to handle establishing connections
        std::thread::spawn(move || {

            // ################## ESTABLISHING ENCRYPTED CONNECTION ##########################################################################################################
            match stream.write(thread_public_key.as_bytes()) {
                Ok(_) => (),
                Err(e) => {
                    println!("failed to write server public key because: {}", e);
                    return
                }
            }
            println!("About to get crypto");
            let mut buffer: [u8; 32] = [0; 32];
            
            match stream.read_exact(&mut buffer){
                Ok(_) => (),
                Err(e) => {
                    println!("failed to read client public key because: {}", e);
                    return
                }
            }
            
            let client_public_key = PublicKey::from(buffer);
            
            let shared_secret = thread_private_key.diffie_hellman(&client_public_key);
            let aes_key = blake3_hash(shared_secret.as_bytes());

            let mut auth_buffer = [0u8; 1052];
            println!("About to read auth string");
            match stream.read_exact(&mut auth_buffer) {
                Ok(_) => (),
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
            println!("password: {:?}", password);

            // println!("username: {}\npassword: {:x?}", username, password);
            let password = blake3_hash(bytes_to_str(password).unwrap().as_bytes());
            println!("password: {:?}", password);
            // println!("password_hash: {:x?}", password);
            println!("About to verify username and password");
            
            let mut connection: Connection;
            {
                let thread_users_lock = thread_users.lock().unwrap();
                if !thread_users_lock.contains_key(username) {
                    println!("users: {:?}", thread_users_lock["admin"]);
                    println!("Username:\n\t{}\n...is wrong", username);
                    return 
                } else if thread_users_lock[username].password != password {
                    println!("thread_users_lock[username].password: {:?}", thread_users_lock[username].password);
                    println!("password: {:?}", password);
                    println!("Password hash:\n\t{:?}\n...is wrong", password);
                    return
                }
                
                connection = Connection {
                    stream: stream, 
                    user: username.to_owned(), 
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
                        println!("There was an io error during a large read.\nError:\t{e}");
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
                            },
                        }
                    },
                    Instruction::Delete(table_name, query) => {
                        match handle_delete_request(&mut connection, &table_name, &query, thread_global_tables.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
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
                            },
                        }
                        
                    },
                    Instruction::KvUpload(table_name) => {
                        let check_global_kv  = thread_global_kv_table.clone(); 
                        match handle_kv_upload(&mut connection, &table_name, thread_global_kv_table.clone()) {
                            Ok(_) => {
                                println!("Operation finished!");
                                println!("kv result: {:x?}", check_global_kv.lock().unwrap().get(&KeyString::from(table_name)).unwrap().body);
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
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
                            },
                        }
                    },
                    Instruction::MetaListTables => {
                        match handle_meta_list_tables(&mut connection, thread_global_tables.clone()) {
                            Ok(_) => {
                                println!("Operation finished");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                            }
                        }
                    }
                    Instruction::MetaListKeyValues => {
                        match handle_meta_list_key_values(&mut connection, thread_global_kv_table.clone()) {
                            Ok(_) => {
                                println!("Operation finished");
                            },
                            Err(e) => {
                                println!("Operation failed because: {}", e);
                            }
                        }
                    }
                },
                
                Err(e) => {
                    println!("Failed to serve request because: {e}");
                    match connection.stream.write(e.to_string().as_bytes()){
                        Ok(_) => (),
                        Err(e) => {
                            println!("failed to write error message because: {}", e);
                        }
                    }
                    println!("Thread finished on error: {e}");
                },
                
            };

            //####################### END OF HANDLING REQUESTS #############################################################################################################
        });
        // END OF SERVER FUNCTION

    }
}


