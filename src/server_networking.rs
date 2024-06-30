use std::collections::BTreeMap;
use std::io::{Write, Read};
use std::net::{TcpListener};
use std::sync::{Arc, RwLock};
use std::str::{self};
use std::time::Duration;
use std::convert::{TryFrom, From};

use x25519_dalek::{StaticSecret, PublicKey};

use crate::aes_temp_crypto::decrypt_aes256;
use crate::auth::{user_has_permission, AuthenticationError, Permission, User};
use crate::disk_utilities::{BufferPool, MAX_BUFFERPOOL_SIZE};
use crate::logging::Logger;
use crate::networking_utilities::*;
use crate::db_structure::KeyString;
use crate::handlers::*;
use crate::PATH_SEP;

pub const CONFIG_FOLDER: &str = "EZconfig/";
pub const MAX_PENDING_MESSAGES: usize = 10;
pub const PROCESS_MESSAGES_INTERVAL: u64 = 10;   // The number of seconds that pass before the database processes all pending write messages.



// Need to redesign the server multithreading before I continue. If I have to lock the "table of tables" for each query,
// then there's no point to multithreading.


/// The struct that carries data relevant to the running server. 
/// Am trying to think of ways to reduce reliance on Arc<RwLock<T>>
pub struct Server {
    pub public_key: PublicKey,
    pub private_key: StaticSecret,
    pub listener: TcpListener,
}

pub struct Database {
    pub buffer_pool: BufferPool,
    pub users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>>,
    pub logger: Logger,
}

impl Database {
    pub fn init() -> Result<Database, ServerError> {

        let buffer_pool = BufferPool::empty(std::sync::atomic::AtomicU64::new(MAX_BUFFERPOOL_SIZE));
        buffer_pool.init_tables(&format!("EZconfig{PATH_SEP}raw_tables"))?;
        buffer_pool.init_values(&format!("EZconfig{PATH_SEP}raw_values"))?;
        let users = BTreeMap::<KeyString, RwLock<User>>::new();
        let users = Arc::new(RwLock::new(users));
        let path = &format!("EZconfig{PATH_SEP}.users");
        if std::path::Path::new(path).exists() {
            let temp = std::fs::read_to_string(path)?;
            for line in temp.lines() {
                if line.as_bytes()[0] == b'#' {
                    continue
                }
                let temp_user: User = ron::from_str(line).unwrap();
                println!("user: {}", temp_user.username);
                users.write().unwrap().insert(KeyString::from(temp_user.username.as_str()), RwLock::new(temp_user));
            }
        } else {
            let mut users_file = std::fs::File::create(path)?;
            let admin = User::admin("admin", "admin");
            println!("user: '{:x?}'", admin.username.as_bytes());
            users_file.write_all(ron::to_string(&admin).unwrap().as_bytes())?;
            users.write().unwrap().insert(KeyString::from("admin"), RwLock::new(admin));
        }

        let database = Database {
            buffer_pool: buffer_pool,
            users: users,
            logger: Logger::init(),
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
        std::fs::File::create_new("EZconfig/log").expect("Need IO access to initialize database");
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
            println!("Background thread running");
            loop {
                std::thread::sleep(Duration::from_secs(10));
                println!("Background thread still running");
                for key in writer_thread_db_ref.buffer_pool.tables.read().unwrap().keys() {
                    let mut table_naughty_list = writer_thread_db_ref.buffer_pool.table_naughty_list.write().unwrap();
                    if table_naughty_list.contains(key) {
                        match writer_thread_db_ref.buffer_pool.write_table_to_file(key) {
                            Ok(_) => (),
                            Err(e) => match e {
                                ServerError::Io(e) => println!("{}", e),
                                e => panic!("The write thread should only ever throw IO errors and it just threw this error:\n {}", e),
                            }
                        };
                        table_naughty_list.remove(key);
                    }
                }
                

                for key in writer_thread_db_ref.buffer_pool.values.read().unwrap().keys() {
                    let mut value_naughty_list = writer_thread_db_ref.buffer_pool.value_naughty_list.write().unwrap();
                    if value_naughty_list.contains(key) {
                        match writer_thread_db_ref.buffer_pool.write_value_to_file(key) {
                            Ok(_) => (),
                            Err(e) => match e {
                                ServerError::Io(e) => println!("{}", e),
                                e => panic!("The write thread should only ever throw IO errors and it just threw this error:\n {}", e),
                            }
                        };
                        value_naughty_list.remove(key);
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
            
            let thread_server = server.clone();
            let db_ref = database.clone();
            // Spawn a thread to handle establishing connections
            outer_scope.spawn(move || {
                
                // ################## ESTABLISHING ENCRYPTED CONNECTION ##########################################################################################################
                // check_if_http();

                let mut connection = establish_connection(stream, thread_server, db_ref.clone())?;

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
                                name.as_str(), 
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
                                name.as_str()
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
                                name.as_str(), 
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
                        Instruction::Delete(table_name) => {
                            match handle_delete_request(
                                &mut connection, 
                                table_name.as_str(),
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
                                table_name.as_str(),
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
                                table_name.as_str(), 
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
                                table_name.as_str(), 
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

    // let instruction_block: Vec<&str> = instruction.split('|').collect();

    // println!("parsing 2...");
    // if instruction_block.len() != INSTRUCTION_LENGTH {
    //     return Err(ServerError::Instruction(InstructionError::Invalid("Wrong number of query fields. Query should be usernme, password, request, table_name, query(or blank)".to_owned())));
    // }
    
    println!("parsing 3...");
    let username = KeyString::try_from(&plaintext[0..64])?;
    let action = KeyString::try_from(&plaintext[64..128])?;
    let table_name = KeyString::try_from(&plaintext[128..192])?;
    let query = match String::from_utf8(Vec::from(&plaintext[192..])) {
        Ok(x) => x,
        Err(e) => return Err(ServerError::Utf8(e.utf8_error())),
    };

    if table_name.as_str() == "All" {
        return Err(ServerError::Instruction(InstructionError::InvalidTable("Table cannot be called 'All'".to_owned())));
    }

    println!("parsing 4...");
    match action.as_str() {
        "Querying" => {
            Ok(Instruction::Query(query.to_owned()))
            
        }
        "Uploading" => {
            if user_has_permission(table_name.as_str(), Permission::Upload, username.as_str(), database.users.clone()) {
                Ok(Instruction::Upload(table_name))
            } else {
                Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        } 
        "Downloading" => {
            if database.buffer_pool.tables.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name.as_str(), Permission::Read, username.as_str(), database.users.clone()) 
            {
                Ok(Instruction::Download(table_name))
            } else {
                Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_string())))
            }
        },
        "Updating" => {
            if database.buffer_pool.tables.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name.as_str(), Permission::Write, username.as_str(), database.users.clone())
            { 
                Ok(Instruction::Update(table_name))
            } else {
                Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_string())))
            }
        },
        "Deleting" => {
            if database.buffer_pool.tables.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name.as_str(), Permission::Write, username.as_str(), database.users.clone())
            {
                Ok(Instruction::Delete(table_name))
            } else {
                Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_string())))
            }
        }
        "KvUpload" => {
            if database.buffer_pool.tables.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name.as_str(), Permission::Upload, username.as_str(), database.users.clone())
            {
                Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' already exists. Use 'update' instead", table_name))))
            } else {
                Ok(Instruction::KvUpload(table_name))
            }
        },
        "KvUpdate" => {
            if database.buffer_pool.values.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name.as_str(), Permission::Write, username.as_str(), database.users.clone())
            {
                Ok(Instruction::KvUpdate(table_name))
            } else {
                Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' does not exist. Use 'upload' instead", table_name))))
            }
        },
        "KvDownload" => {
            if database.buffer_pool.values.read().unwrap().contains_key(&KeyString::from(table_name)) 
            && 
            user_has_permission(table_name.as_str(), Permission::Read, username.as_str(), database.users.clone())
            {
                Ok(Instruction::KvDownload(table_name))
            } else {
                Err(ServerError::Instruction(InstructionError::InvalidTable(format!("Entry '{}' does not exist. Use 'upload' instead", table_name))))
            }
        },
        "MetaListTables" => {
            if user_has_permission(table_name.as_str(), Permission::Read, username.as_str(), database.users.clone()) {
                Ok(Instruction::MetaListTables)
            } else {
                Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        },
        "MetaListKeyValues" => {
            if user_has_permission(table_name.as_str(), Permission::Read, username.as_str(), database.users.clone()) {
                Ok(Instruction::MetaListKeyValues)
            } else {
                Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        },
        "MetaNewUser" => {
            if user_has_permission(table_name.as_str(), Permission::Write, username.as_str(), database.users.clone()) {
                Ok(Instruction::NewUser(query))
            } else {
                Err(ServerError::Authentication(AuthenticationError::Permission))
            }
        }
        _ => Err(ServerError::Instruction(InstructionError::Invalid(action.to_string()))),
    }
}



#[cfg(test)]
mod tests {

    use super::*;

    // #[test]
    // fn test_server_init() {
    //     run_server("127.0.0.1:3004").unwrap();
    // }

}
