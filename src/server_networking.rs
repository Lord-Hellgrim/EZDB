use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::str::{self};
use std::time::Duration;

use x25519_dalek::{StaticSecret, PublicKey};

use crate::aes_temp_crypto::decrypt_aes256;
use crate::auth::{User, AuthenticationError, user_has_permission};
use crate::disk_utilities::{BufferPool, DiskTable, MAX_BUFFERPOOL_SIZE};
use crate::networking_utilities::*;
use crate::db_structure::{EZTable, DbVec, KeyString, Metadata, StrictError, Value};
use crate::handlers::*;
use crate::ezql::{self, parse_EZQL};
use crate::PATH_SEP;

pub const CONFIG_FOLDER: &str = "EZconfig/";
pub const MAX_PENDING_MESSAGES: usize = 10;
pub const PROCESS_MESSAGES_INTERVAL: u64 = 10;   // The number of seconds that pass before the database processes all pending write messages.

/// Parses the inctructions sent by the client. Will be rewritten soon to accomodate EZQL
pub fn parse_instruction(
    instructions: &[u8], 
    users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>>, 
    global_tables: Arc<RwLock<BTreeMap<KeyString, RwLock<EZTable>>>>, 
    global_kv_table: Arc<RwLock<BTreeMap<KeyString, RwLock<Value>>>>, 
    aes_key: &[u8],
    instruction_sender: crossbeam_channel::Sender<WriteThreadMessage>
) -> Result<Instruction, ServerError> {

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
            if global_tables.read().unwrap().contains_key(&KeyString::from(table_name)) {
                return Ok(Instruction::Query(table_name.to_owned(), query.to_owned()));
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
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
            if global_tables.read().unwrap().contains_key(&KeyString::from(table_name)) {
                Ok(Instruction::Download(table_name.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
            }
        },
        "Updating" => {
            if global_tables.read().unwrap().contains_key(&KeyString::from(table_name)) { 
                Ok(Instruction::Update(table_name.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
            }
        },
        "Deleting" => {
            if global_tables.read().unwrap().contains_key(&KeyString::from(table_name)) {
                Ok(Instruction::Delete(table_name.to_owned(), query.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
            }
        }
        "KvUpload" => {
            if global_kv_table.read().unwrap().contains_key(&KeyString::from(table_name)) {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' already exists. Use 'update' instead", table_name))));
            } else {
                Ok(Instruction::KvUpload(table_name.to_owned()))
            }
        },
        "KvUpdate" => {
            if global_kv_table.read().unwrap().contains_key(&KeyString::from(table_name)) {
                Ok(Instruction::KvUpdate(table_name.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' does not exist. Use 'upload' instead", table_name))));
            }
        },
        "KvDownload" => {
            if global_kv_table.read().unwrap().contains_key(&KeyString::from(table_name)) {
                Ok(Instruction::KvDownload(table_name.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' does not exist. Use 'upload' instead", table_name))));
            }
        },
        "MetaListTables" => {
            Ok(Instruction::MetaListTables)
        },
        "MetaListKeyValues" => {
            Ok(Instruction::MetaListKeyValues)
        },
        "MetaNewUser" => {
            Ok(Instruction::NewUser(username.to_owned()))
        }
        _ => {return Err(ServerError::Instruction(InstructionError::Invalid(action.to_owned())));},
    }
}

// Need to redesign the server multithreading before I continue. If I have to lock the "table of tables" for each query,
// then there's no point to multithreading.
pub fn execute_single_EZQL_query(query: ezql::Query) -> Result<EZTable, ServerError> {

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

#[derive(Clone, PartialEq, PartialOrd)]
pub enum WriteThreadMessage {
    UpdateMetadata(Metadata, KeyString), 
    DropTable(KeyString),
    MetaNewUser(User),
    NewKeyValue(KeyString, Value),
    UpdateKeyValue(KeyString, Value),
    NewTable(EZTable),
    DeleteRows(KeyString, DbVec),
    UpdateTable(KeyString, EZTable),
}

impl Display for WriteThreadMessage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            WriteThreadMessage::UpdateMetadata(x, y) => writeln!(f, "{}:\n{}", y, x),
            WriteThreadMessage::UpdateTable(name, table) => writeln!(f, "{}:\n{}", name, table),
            WriteThreadMessage::DropTable(x) => writeln!(f, "{}", x),
            WriteThreadMessage::DeleteRows(x, y) => writeln!(f, "{}:\n{}", x, y),
            WriteThreadMessage::NewTable(x) => writeln!(f, "{}", x),
            WriteThreadMessage::MetaNewUser(x) => writeln!(f, "{}", ron::to_string(x).unwrap()),
            WriteThreadMessage::NewKeyValue(key, value) => write!(f, "key: {}\nValue:\n{:x?}", key, value),
            WriteThreadMessage::UpdateKeyValue(key, value) => write!(f, "key: {}\nValue:\n{:x?}", key, value),
        }

    }
}


/// The struct that carries data relevant to the running server. 
/// Am trying to think of ways to reduce reliance on Arc<RwLock<T>>
pub struct Server {
    pub public_key: PublicKey,
    private_key: StaticSecret,
    pub listener: TcpListener,
    pub buffer_pool: Arc<RwLock<BufferPool>>,
    pub users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>>,
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

        let buffer_pool = BufferPool::empty(MAX_BUFFERPOOL_SIZE);
        let users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>> = Arc::new(RwLock::new(BTreeMap::new()));

        let mut server = Arc::new(Server {
            public_key: server_public_key,
            private_key: server_private_key,
            listener: l,
            buffer_pool,
            users: users,
        });

    
    println!("Reading users config into memory");
    if std::path::Path::new("EZconfig").is_dir() {
        println!("config exists");
        let temp = std::fs::read_to_string(&format!("{CONFIG_FOLDER}.users"))?;
        for line in temp.lines() {
            if line.as_bytes()[0] == b'#' {
                continue
            }
            let temp_user: User = ron::from_str(line).unwrap();
            server.users.write().unwrap().insert(KeyString::from(temp_user.username.as_str()), RwLock::new(temp_user));
        }

        let tables_on_disk_path = &format!("EZconfig{PATH_SEP}raw_tables");
        server.buffer_pool.init_tables(tables_on_disk_path)?;



    } else {
        println!("config does not exist");
        let temp = ron::to_string(&User::admin("admin", "admin")).unwrap();
        std::fs::create_dir("EZconfig").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/raw_tables").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/key_value").expect("Need IO access to initialize database");
        let mut user_file = match std::fs::File::create(format!("{CONFIG_FOLDER}.users")) {
            Ok(f) => f,
            Err(e) => return Err(ServerError::Strict(StrictError::Io(e))),
        };
        match user_file.write_all(temp.as_bytes()) {
            Ok(_) => (),
            Err(e) => panic!("failed to create config file. Server cannot run.\n\nError cause was:\n{e}"),
        };

        server.users.write().unwrap().insert(KeyString::from("admin"), RwLock::new(User::admin("admin", "admin")));
    } 

    dbg!(&server.users);

    // #################################### END STARTUP SEQUENCE ###############################################


    // #################################### DATA SAVING AND LOADING LOOP ###################################################

    let outer_thread_server = server.clone();

    let _full_scope: Result<(), ServerError> = std::thread::scope(|outer_scope| {
        
        let (write_message_sender, write_message_receiver) = crossbeam_channel::unbounded::<WriteThreadMessage>();
        
        let writer_thread = 
        outer_scope.spawn(move || {
            std::thread::sleep(Duration::from_secs(10));

            for table in outer_thread_server.buffer_pool.tables.read().unwrap().values() {
                let table_lock = table.read().unwrap();
                let table_file = outer_thread_server.buffer_pool.files
                .write()
                .unwrap()
                .get_mut(&table_lock.name)
                .unwrap()
                .write()
                .unwrap()
                .write_all(&table_lock.write_to_raw_binary())
                ;
            }

            
        }); // Thread that writes in memory tables to disk



        loop {
            // Reading instructions
            let (mut stream, client_address) = match server.listener.accept() {
                Ok((n,m)) => (n, m),
                Err(e) => {return Err(ServerError::Io(e.kind()));},
            };
            println!("Accepted connection from: {}", client_address);        

            let inner_thread_server = server.clone();
            let inner_thread_message_sender = write_message_sender.clone();
    
            // Spawn a thread to handle establishing connections
            outer_scope.spawn(move || {
    
                // ################## ESTABLISHING ENCRYPTED CONNECTION ##########################################################################################################
                match stream.write(inner_thread_server.public_key.as_bytes()) {
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
                
                let shared_secret = inner_thread_server.private_key.diffie_hellman(&client_public_key);
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
                    if !&inner_thread_server.users.read().unwrap().contains_key(&KeyString::from(username)) {
                        println!("Username:\n\t{}\n...is wrong", username);
                        return Err(ServerError::Authentication(AuthenticationError::WrongUser(format!("Username: '{}' does not exist", username))));
                    } else if &inner_thread_server.users.read().unwrap()[&KeyString::from(username)].read().unwrap().password != &password {
                        // println!("thread_users_lock[username].password: {:?}", user_lock.password);
                        // println!("password: {:?}", password);
                        // println!("Password hash:\n\t{:?}\n...is wrong", password);
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
                let instruction_sender = inner_thread_message_sender.clone();
                match parse_instruction(
                    instructions, 
                    inner_thread_server.users.clone(), 
                    inner_thread_server.buffer_pool.tables.clone(), 
                    inner_thread_server.buffer_pool.values.clone(), 
                    &connection.aes_key, 
                    instruction_sender
                ) {
                    Ok(i) => match i {
                        
                        Instruction::Download(name) => {
                            match handle_download_request(
                                &mut connection, 
                                &name, 
                                inner_thread_server.buffer_pool.tables.clone(), 
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                }
                            }
                        },
                        Instruction::Upload(name) => {
                            match handle_upload_request(
                                &mut connection, 
                                &name,
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                }
                            }
                        },
                        Instruction::Update(name) => {
                            match handle_update_request(
                                &mut connection, 
                                &name, 
                                inner_thread_server.buffer_pool.tables.clone(), 
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::Query(table_name, query) => {
                            match handle_query_request(
                                &mut connection, 
                                &table_name, 
                                &query, 
                                inner_thread_server.buffer_pool.tables.clone(), 
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::Delete(table_name, query) => {
                            match handle_delete_request(
                                &mut connection, 
                                &table_name, 
                                &query, 
                                inner_thread_server.buffer_pool.tables.clone(), 
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::NewUser(user_string) => {
                            match handle_new_user_request(
                                &mut connection, 
                                &user_string, 
                                inner_thread_server.users.clone(), 
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("New user added!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                            
                        },
                        Instruction::KvUpload(table_name) => {
                            match handle_kv_upload(
                                &mut connection, 
                                &table_name,
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::KvUpdate(table_name) => {
                            match handle_kv_update(
                                &mut connection, 
                                &table_name, 
                                inner_thread_server.buffer_pool.values.clone(), 
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::KvDownload(table_name) => {
                            match handle_kv_download(
                                &mut connection, 
                                &table_name, 
                                inner_thread_server.buffer_pool.values.clone(), 
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::MetaListTables => {
                            match handle_meta_list_tables(
                                &mut connection, 
                                inner_thread_server.buffer_pool.tables.clone(), 
                                inner_thread_message_sender
                            ) {
                                Ok(_) => {
                                    println!("Operation finished");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                }
                            }
                        }
                        Instruction::MetaListKeyValues => {
                            match handle_meta_list_key_values(
                                &mut connection, 
                                inner_thread_server.buffer_pool.values.clone(), 
                                inner_thread_message_sender
                            ) {
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


