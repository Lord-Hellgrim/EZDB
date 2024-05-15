use std::collections::BTreeMap;
use std::fmt::Display;
use std::io::{Write, Read};
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::str::{self};
use std::time::Duration;

use x25519_dalek::{StaticSecret, PublicKey};

use crate::aes_temp_crypto::decrypt_aes256;
use crate::auth::{user_has_permission, AuthenticationError, Permission, User};
use crate::disk_utilities::{BufferPool, MAX_BUFFERPOOL_SIZE};
use crate::networking_utilities::*;
use crate::db_structure::{remove_indices, DbColumn, EZTable, KeyString, Metadata, StrictError, Value};
use crate::handlers::*;
use crate::PATH_SEP;

pub const CONFIG_FOLDER: &str = "EZconfig/";
pub const MAX_PENDING_MESSAGES: usize = 10;
pub const PROCESS_MESSAGES_INTERVAL: u64 = 10;   // The number of seconds that pass before the database processes all pending write messages.

/// Parses the inctructions sent by the client. Will be rewritten soon to accomodate EZQL
pub fn parse_instruction(
    instructions: &[u8], 
    database: Arc<Database>,
    aes_key: &[u8]
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
            return Ok(Instruction::Query(query.to_owned()));
            
        }
        "Uploading" => {
            if user_has_permission(table_name, Permission::Upload, username, database.users.clone()) {
                return Ok(Instruction::Upload(table_name.to_owned()));
            } else {
                return Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        } 
        "Downloading" => {
            if database.buffer_pool.tables.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name, Permission::Read, username, database.users.clone()) 
            {
                Ok(Instruction::Download(table_name.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
            }
        },
        "Updating" => {
            if database.buffer_pool.tables.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name, Permission::Write, username, database.users.clone())
            { 
                Ok(Instruction::Update(table_name.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
            }
        },
        "Deleting" => {
            if database.buffer_pool.tables.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name, Permission::Write, username, database.users.clone())
            {
                Ok(Instruction::Delete(table_name.to_owned(), query.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
            }
        }
        "KvUpload" => {
            if database.buffer_pool.tables.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name, Permission::Upload, username, database.users.clone())
            {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' already exists. Use 'update' instead", table_name))));
            } else {
                Ok(Instruction::KvUpload(table_name.to_owned()))
            }
        },
        "KvUpdate" => {
            if database.buffer_pool.values.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name, Permission::Write, username, database.users.clone())
            {
                Ok(Instruction::KvUpdate(table_name.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' does not exist. Use 'upload' instead", table_name))));
            }
        },
        "KvDownload" => {
            if database.buffer_pool.values.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name, Permission::Read, username, database.users.clone())
            {
                Ok(Instruction::KvDownload(table_name.to_owned()))
            } else {
                return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' does not exist. Use 'upload' instead", table_name))));
            }
        },
        "MetaListTables" => {
            if user_has_permission(table_name, Permission::Read, username, database.users.clone()) {
                Ok(Instruction::MetaListTables)
            } else {
                return Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        },
        "MetaListKeyValues" => {
            if user_has_permission(table_name, Permission::Read, username, database.users.clone()) {
                Ok(Instruction::MetaListKeyValues)
            } else {
                return Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        },
        "MetaNewUser" => {
            if user_has_permission(table_name, Permission::Write, username, database.users.clone()) {
                Ok(Instruction::NewUser(username.to_owned()))
            } else {
                return Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        }
        _ => {return Err(ServerError::Instruction(InstructionError::Invalid(action.to_owned())));},
    }
}

// Need to redesign the server multithreading before I continue. If I have to lock the "table of tables" for each query,
// then there's no point to multithreading.


#[derive(Clone, PartialEq, PartialOrd)]
pub enum WriteThreadMessage {
    UpdateMetadata(Metadata, KeyString), 
    DropTable(KeyString),
    MetaNewUser(User),
    NewKeyValue(KeyString, Value),
    UpdateKeyValue(KeyString, Value),
    NewTable(EZTable),
    DeleteRows(KeyString, DbColumn),
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
}

pub struct Database {
    pub buffer_pool: BufferPool,
    pub users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>>,
}

impl Database {
    pub fn init() -> Result<Database, ServerError> {

        let buffer_pool = BufferPool::empty(MAX_BUFFERPOOL_SIZE);
        buffer_pool.init_tables(&format!("EZconfig{PATH_SEP}raw_tables"))?;
        buffer_pool.init_values(&format!("EZconfig{PATH_SEP}raw_values"))?;
        let users = BTreeMap::<KeyString, RwLock<User>>::new();
        let users = Arc::new(RwLock::new(users));
        let path = &format!("{CONFIG_FOLDER}.users");
        if std::path::Path::new(path).exists() {
            let temp = std::fs::read_to_string(path)?;
            for line in temp.lines() {
                if line.as_bytes()[0] == b'#' {
                    continue
                }
                let temp_user: User = ron::from_str(line).unwrap();
                users.write().unwrap().insert(KeyString::from(temp_user.username.as_str()), RwLock::new(temp_user));
            }
        } else {
            let mut users_file = std::fs::File::create(path)?;
            let admin = User::admin("admin", "admin");
            users_file.write(ron::to_string(&admin).unwrap().as_bytes())?;
            let users = BTreeMap::<KeyString, RwLock<User>>::new();
            let users = Arc::new(RwLock::new(users));
            users.write().unwrap().insert(KeyString::from("admin"), RwLock::new(admin));
        }

        let database = Database {
            buffer_pool: buffer_pool,
            users: users,
        };

        Ok(database)
    }
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

        let server = Arc::new(Server {
            public_key: server_public_key,
            private_key: server_private_key,
            listener: l,
        });

        if !std::path::Path::new("EZconfig").is_dir() {
            println!("config does not exist");
            std::fs::create_dir("EZconfig").expect("Need IO access to initialize database");
            std::fs::create_dir("EZconfig/raw_tables").expect("Need IO access to initialize database");
            std::fs::create_dir("EZconfig/raw_values").expect("Need IO access to initialize database");
        } else {
            println!("config folder exists");

        }
        println!("Initializing database");
        let database = Arc::new(Database::init()?);

    // #################################### END STARTUP SEQUENCE ###############################################


    // #################################### DATA SAVING AND LOADING LOOP ###################################################

    let writer_thread_db_ref = database.clone();
    
    let _full_scope: Result<(), ServerError> = std::thread::scope(|outer_scope| {
        
        let _background_thread = 
        outer_scope.spawn(move || {
            std::thread::sleep(Duration::from_secs(10));
            println!("Background thread running");
            for key in writer_thread_db_ref.buffer_pool.tables.read().unwrap().keys() {
                let mut naughty_list = writer_thread_db_ref.buffer_pool.naughty_list.write().unwrap();
                if naughty_list.contains(key) {
                    match writer_thread_db_ref.buffer_pool.write_table_to_file(key) {
                        Ok(_) => (),
                        Err(e) => match e {
                            ServerError::Io(e) => println!("{}", e),
                            e => panic!("The write thread should only ever throw IO errors and it just threw this error:\n {}", e),
                        }
                    };
                    naughty_list.remove(key);
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
            
            let thread_server = server.clone();
            let db_ref = database.clone();
            // Spawn a thread to handle establishing connections
            outer_scope.spawn(move || {
                
                // ################## ESTABLISHING ENCRYPTED CONNECTION ##########################################################################################################
                match stream.write(thread_server.public_key.as_bytes()) {
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
                
                let shared_secret = thread_server.private_key.diffie_hellman(&client_public_key);
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
                    if !db_ref.users.read().unwrap().contains_key(&KeyString::from(username)) {
                        println!("Username:\n\t{}\n...is wrong", username);
                        return Err(ServerError::Authentication(AuthenticationError::WrongUser(format!("Username: '{}' does not exist", username))));
                    } else if db_ref.users.read().unwrap()[&KeyString::from(username)].read().unwrap().password != password {
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
                match parse_instruction(
                    instructions, 
                    db_ref.clone(),
                    &connection.aes_key
                ) {
                    Ok(i) => match i {
                        
                        Instruction::Download(name) => {
                            match handle_download_request(
                                &mut connection, 
                                &name, 
                                db_ref.clone(),
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
                                db_ref.clone(),
                                &name
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
                                db_ref.clone(),
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::Query(query) => {
                            match handle_query_request(
                                &mut connection,
                                &query, 
                                db_ref.clone(),
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
                                db_ref.clone(),
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
                                db_ref.clone(),
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
                                db_ref.clone(),
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
                                db_ref.clone(),
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
                                db_ref.clone(),
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
                                db_ref.clone(),
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
                                db_ref.clone(),
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


