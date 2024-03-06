use std::collections::HashMap;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::sync::{Arc, Mutex, RwLock};
use std::str::{self};

use smartstring::{SmartString, LazyCompact};
use x25519_dalek::{StaticSecret, PublicKey};

pub type KeyString = SmartString<LazyCompact>;

use crate::aes_temp_crypto::decrypt_aes256;
use crate::auth::{User, AuthenticationError, user_has_permission};
use crate::networking_utilities::*;
use crate::db_structure::{ColumnTable, StrictError, Value};
use crate::handlers::*;
use crate::ezql;

pub const CONFIG_FOLDER: &str = "EZconfig/";

/// Parses the inctructions sent by the client. Will be rewritten soon to accomodate EZQL
pub fn parse_instruction(instructions: &[u8], users: Arc<RwLock<HashMap<KeyString, Mutex<User>>>>, global_tables: Arc<RwLock<HashMap<KeyString, Mutex<ColumnTable>>>>, global_kv_table: Arc<RwLock<HashMap<KeyString, Mutex<Value>>>>, aes_key: &[u8]) -> Result<Instruction, ServerError> {

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
            if !global_tables.read().unwrap().contains_key(table_name) {
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
            if !global_tables.read().unwrap().contains_key(table_name) {
                let raw_table_exists = std::path::Path::new(&format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name)).exists();
                if raw_table_exists {
                    println!("Loading table from disk");
                    let disk_table = std::fs::read_to_string(format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name))?;
                    let disk_table = ColumnTable::from_csv_string(&disk_table, table_name, "temp")?;
                    {
                        let mut writer = global_tables.write().unwrap();
                        writer.insert(KeyString::from(table_name), Mutex::new(disk_table));
                    }
                    Ok(Instruction::Download(table_name.to_owned()))
                } else {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                }
            } else {
                Ok(Instruction::Download(table_name.to_owned()))
            }
        },
        "Updating" => {
            if !global_tables.read().unwrap().contains_key(table_name) {
                let raw_table_exists = std::path::Path::new(&format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name)).exists();
                if raw_table_exists {
                    println!("Loading table from disk");
                    let disk_table = std::fs::read_to_string(format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name))?;
                    let disk_table = ColumnTable::from_csv_string(&disk_table, table_name, "temp")?;
                    {
                        let mut writer = global_tables.write().unwrap();
                        writer.insert(KeyString::from(table_name), Mutex::new(disk_table));
                    }
                    Ok(Instruction::Update(table_name.to_owned()))
                } else {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                }
            } else {
                Ok(Instruction::Update(table_name.to_owned()))
            }
        },
        "Deleting" => {
            if !global_tables.read().unwrap().contains_key(table_name) {
                let raw_table_exists = std::path::Path::new(&format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name)).exists();
                if raw_table_exists {
                    println!("Loading table from disk");
                    let disk_table = std::fs::read_to_string(format!("{}/raw_tables/{}", CONFIG_FOLDER, table_name))?;
                    let disk_table = ColumnTable::from_csv_string(&disk_table, table_name, "temp")?;
                    {
                        let mut writer = global_tables.write().unwrap();
                        writer.insert(KeyString::from(table_name), Mutex::new(disk_table));
                    }
                    Ok(Instruction::Download(table_name.to_owned()))
                } else {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                }
            } else {
                Ok(Instruction::Delete(table_name.to_owned(), query.to_owned()))
            }
        }
        "KvUpload" => {
            if global_kv_table.read().unwrap().contains_key(table_name) {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' already exists. Use 'update' instead", table_name))));
            } else {
                Ok(Instruction::KvUpload(table_name.to_owned()))
            }
        },
        "KvUpdate" => {
            if !global_kv_table.read().unwrap().contains_key(table_name) {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' does not exist. Use 'upload' instead", table_name))));
            } else {
                Ok(Instruction::KvUpdate(table_name.to_owned()))
            }
        },
        "KvDownload" => {
            if !global_kv_table.read().unwrap().contains_key(table_name) {
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

// Need to redesign the server multithreading before I continue. If I have to lock the "table of tables" for each query,
// then there's no point to multithreading.
pub fn execute_single_EZQL_query(server: &mut Server, query: ezql::Query) -> Result<ColumnTable, ServerError> {

    match query.query_type {
        ezql::QueryType::DELETE => todo!(),
        ezql::QueryType::SELECT => {},
        ezql::QueryType::LEFT_JOIN => todo!(),
        ezql::QueryType::INNER_JOIN => todo!(),
        ezql::QueryType::RIGHT_JOIN => todo!(),
        ezql::QueryType::FULL_JOIN => todo!(),
        ezql::QueryType::UPDATE => todo!(),
        ezql::QueryType::INSERT => todo!(),
    }

    todo!()
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DiskThreadMessage {
    LoadTable(KeyString),
    DropTable(KeyString),
}


/// The struct that carries data relevant to the running server. 
/// Am trying to think of ways to reduce reliance on Arc<Mutex<T>>
pub struct Server {
    public_key: PublicKey,
    private_key: StaticSecret,
    listener: TcpListener,
    tables: Arc<RwLock<HashMap<KeyString, Mutex<ColumnTable>>>>,
    kv_list: Arc<RwLock<HashMap<KeyString, Mutex<Value>>>>,
    users: Arc<RwLock<HashMap<KeyString, Mutex<User>>>>,
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

        let global_tables: Arc<RwLock<HashMap<KeyString, Mutex<ColumnTable>>>> = Arc::new(RwLock::new(HashMap::new()));
        let global_kv_table: Arc<RwLock<HashMap<KeyString, Mutex<Value>>>> = Arc::new(RwLock::new(HashMap::new()));
        let users: Arc<RwLock<HashMap<KeyString, Mutex<User>>>> = Arc::new(RwLock::new(HashMap::new()));

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
            server.users.write().unwrap().insert(temp_user.username.clone(), Mutex::new(temp_user));
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

        server.users.write().unwrap().insert(KeyString::from("admin"), Mutex::new(User::admin("admin", "admin")));
    } 

    dbg!(&server.users);

    // #################################### END STARTUP SEQUENCE ###############################################


    // #################################### DATA SAVING AND LOADING LOOP ###################################################

    let full_scope: Result<(), ServerError> = std::thread::scope(|outer_scope| {
        
        
        outer_scope.spawn(move || {
            loop {
                std::thread::sleep(std::time::Duration::from_secs(10));
                println!("Background thread running good!...");
                {
                    let data = &server.tables.read().unwrap();
                    for (name, table) in data.iter() {
                        let locked_table = table.lock().unwrap();
                        match locked_table.save_to_disk_csv(CONFIG_FOLDER) {
                            Ok(_) => (),
                            Err(e) => println!("Unable to save table {} because: {}", name, e),
                        };
                    }
                    let user_lock = &server.users.read().unwrap();
                    let mut printer = String::new();
                    for (_, user) in user_lock.iter() {
                        printer.push_str(&ron::to_string(&user).unwrap());
                        printer.push('\n');
                    }
                    printer.pop();
                }
    
                {
                    let data = server.kv_list.read().unwrap();
                    for (key, value) in data.iter() {
                        let locked_value = value.lock().unwrap();
                        match locked_value.save_to_disk_raw(key, CONFIG_FOLDER) {
                            Ok(_) => (),
                            Err(e) => println!("Unable to save value of key '{}' because: {}",key, e),
                        };
                    }
                }
            }
        }); // Thread that writes in memory tables to disk



        loop {
            // Reading instructions
            let (mut stream, client_address) = match server.listener.accept() {
                Ok((n,m)) => (n, m),
                Err(e) => {return Err(ServerError::Io(e.kind()));},
            };
            println!("Accepted connection from: {}", client_address);        
            
            // let thread_global_tables: Arc<HashMap<SmartString<LazyCompact>, Mutex<ColumnTable>>> = server.tables.clone();
            // let thread_global_kv_table: Arc<HashMap<SmartString<LazyCompact>, Mutex<Value>>> = server.kv_list.clone();
            // let thread_users: Arc<HashMap<SmartString<LazyCompact>, Mutex<User>>> = Arc::clone(&users);
            // let thread_public_key: PublicKey = server.public_key;
            // let thread_private_key: StaticSecret = server.private_key.clone();
            
    
            // Spawn a thread to handle establishing connections
            let server_loop = outer_scope.spawn(move || {
    
                // ################## ESTABLISHING ENCRYPTED CONNECTION ##########################################################################################################
                match stream.write(server.public_key.as_bytes()) {
                    Ok(_) => (),
                    Err(e) => {
                        println!("failed to write server public key because: {}", e);
                        return Err(ServerError::Io(e.kind()));
                    }
                }
                println!("About to get crypto");
                let mut buffer: [u8; 32] = [0; 32];
                
                match stream.read_exact(&mut buffer){
                    Ok(_) => (),
                    Err(e) => {
                        println!("failed to read client public key because: {}", e);
                        return Err(ServerError::Io(e.kind()));
                    }
                }
                
                let client_public_key = PublicKey::from(buffer);
                
                let shared_secret = server.private_key.diffie_hellman(&client_public_key);
                let aes_key = blake3_hash(shared_secret.as_bytes());
    
                let mut auth_buffer = [0u8; 1052];
                println!("About to read auth string");
                match stream.read_exact(&mut auth_buffer) {
                    Ok(_) => (),
                    Err(e) => {
                        println!("failed to read auth_string because: {}", e);
                        return Err(ServerError::Io(e.kind()));
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
                        return Err(e);
                    }
                };
                println!("About to parse auth_string");
                let username = match bytes_to_str(&auth_string[0..512]) {
                    Ok(s) => s,
                    Err(e) => {
                        println!("failed to read auth_string from bytes because: {}", e);
                        return Err(ServerError::Utf8(e));
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
                    let user_lock = match server.users.read().unwrap().get(username) {
                        Some(locked_user) => locked_user.lock().unwrap(),
                        None => return Err(ServerError::Authentication(AuthenticationError::WrongUser(format!("Username: '{}' does not exist", username)))),
                    };
                    if !&server.users.read().unwrap().contains_key(username) {
                        println!("Username:\n\t{}\n...is wrong", username);
                        return Err(ServerError::Authentication(AuthenticationError::WrongUser(format!("Username: '{}' does not exist", username))));
                    } else if &server.users.read().unwrap()[username].lock().unwrap().password != &password {
                        println!("thread_users_lock[username].password: {:?}", user_lock.password);
                        println!("password: {:?}", password);
                        println!("Password hash:\n\t{:?}\n...is wrong", password);
                        return Err(ServerError::Authentication(AuthenticationError::WrongPassword));
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
                            return Err(ServerError::Io(e.kind()));
                        },
                    };
                }
                
                // println!("Instruction buffer[0..50]: {:x?}", &buffer[0..50]);
                let instructions = &buffer[0..instruction_size];
                println!("Parsing instructions...");
                match parse_instruction(instructions, server.users.clone(), server.tables.clone(), server.kv_list.clone(), &connection.aes_key) {
                    Ok(i) => match i {
                        
                        Instruction::Download(name) => {
                            match handle_download_request(&mut connection, &name, server.tables.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                }
                            }
                        },
                        Instruction::Upload(name) => {
                            match handle_upload_request(&mut connection, &name, server.tables.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                }
                            }
                        },
                        Instruction::Update(name) => {
                            match handle_update_request(&mut connection, &name, server.tables.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::Query(table_name, query) => {
                            match handle_query_request(&mut connection, &table_name, &query, server.tables.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::Delete(table_name, query) => {
                            match handle_delete_request(&mut connection, &table_name, &query, server.tables.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::NewUser(user_string) => {
                            match handle_new_user_request(&user_string, server.users.clone()) {
                                Ok(_) => {
                                    println!("New user added!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                            
                        },
                        Instruction::KvUpload(table_name) => {
                            match handle_kv_upload(&mut connection, &table_name, server.kv_list.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::KvUpdate(table_name) => {
                            match handle_kv_update(&mut connection, &table_name, server.kv_list.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::KvDownload(table_name) => {
                            match handle_kv_download(&mut connection, &table_name, server.kv_list.clone()) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::MetaListTables => {
                            match handle_meta_list_tables(&mut connection, server.tables.clone()) {
                                Ok(_) => {
                                    println!("Operation finished");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                }
                            }
                        }
                        Instruction::MetaListKeyValues => {
                            match handle_meta_list_key_values(&mut connection, server.kv_list.clone()) {
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
    
                Ok(())
            });
        }




    });


    Ok(())

}


