use std::collections::BTreeMap;
use std::io::Write;
use std::net::TcpListener;
use std::sync::{Arc, RwLock};
use std::str::{self};
use std::time::Duration;
use std::convert::{TryFrom, From};

use ezcbor::cbor::{decode_cbor, Cbor};
use eznoise::{HandshakeState, KeyPair};

use crate::auth::{user_has_permission, AuthenticationError, Permission, User};
use crate::disk_utilities::{BufferPool, MAX_BUFFERPOOL_SIZE};
use crate::logging::Logger;
use crate::utilities::{establish_connection, EzError, Instruction, InstructionError};
use crate::db_structure::KeyString;
use crate::handlers::*;
use crate::PATH_SEP;

pub const INSTRUCTION_LENGTH: usize = 284;
pub const CONFIG_FOLDER: &str = "EZconfig/";
pub const MAX_PENDING_MESSAGES: usize = 10;
pub const PROCESS_MESSAGES_INTERVAL: u64 = 10;   // The number of seconds that pass before the database processes all pending write messages.



// Need to redesign the server multithreading before I continue. If I have to lock the "table of tables" for each query,
// then there's no point to multithreading.

pub struct Database {
    pub buffer_pool: BufferPool,
    pub users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>>,
    pub logger: Logger,
}

impl Database {
    pub fn init() -> Result<Database, EzError> {
        println!("calling: Database::init()");


        let buffer_pool = BufferPool::empty(std::sync::atomic::AtomicU64::new(MAX_BUFFERPOOL_SIZE));
        buffer_pool.init_tables(&format!("EZconfig{PATH_SEP}raw_tables"))?;
        buffer_pool.init_values(&format!("EZconfig{PATH_SEP}raw_values"))?;
        let path = &format!("EZconfig{PATH_SEP}.users");
        let mut temp_users = BTreeMap::new();
        if std::path::Path::new(path).exists() {
            let temp = std::fs::read(path)?;
            temp_users = decode_cbor(&temp)?;
        } else {
            let mut users_file = std::fs::File::create(path)?;
            let admin = User::admin("admin", "admin");
            temp_users.insert(KeyString::from("admin"), admin);
            users_file.write_all(&temp_users.to_cbor_bytes())?;
        }
        let mut users = BTreeMap::new();
        for (key, value) in temp_users {
            users.insert(key, RwLock::new(value));
        }
        
        let database = Database {
            buffer_pool: buffer_pool,
            users: Arc::new(RwLock::new(users)),
            logger: Logger::init(),
        };

        Ok(database)
    }
}

pub fn get_server_static_keys() -> KeyPair {
    KeyPair::random()
}

/// The main loop of the server. Checks for incoming connections, parses their instructions, and handles them
/// Also writes tables to disk in a super primitive way. Basically a separate thread writes all the tables to disk
/// every 10 seconds. This will be improved but I would appreciate some advice here.
pub fn run_server(address: &str) -> Result<(), EzError> {
    println!("calling: run_server()");

    
    // #################################### STARTUP SEQUENCE #############################################
    println!("Starting server...\n###########################");
    
    println!("Binding to address: {address}");
    let listener = match TcpListener::bind(address) {
        Ok(value) => value,
        Err(e) => {return Err(EzError::Io(e.kind()));},
    };

    let s = get_server_static_keys();

    if !std::path::Path::new("EZconfig").is_dir() {
        println!("config does not exist");
        std::fs::create_dir("EZconfig").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/raw_tables").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/raw_values").expect("Need IO access to initialize database");
        std::fs::create_dir("EZconfig/log").expect("Need IO access to initialize database");
    } else {
        println!("config folder exists");

    }
    println!("Initializing database");
    let database = Arc::new(Database::init()?);

    // #################################### END STARTUP SEQUENCE ###############################################


    // #################################### DATA SAVING AND LOADING LOOP ###################################################

    let writer_thread_db_ref = database.clone();
    
    let _full_scope: Result<(), EzError> = std::thread::scope(|outer_scope| {
        
        let _background_thread = 
        outer_scope.spawn(move || {
            println!("Background thread running");
            loop {
                std::thread::sleep(Duration::from_secs(10));
                println!("Background thread still running");
                println!("{:?}", writer_thread_db_ref.buffer_pool.table_delete_list.read().unwrap());
                for key in writer_thread_db_ref.buffer_pool.table_delete_list.read().unwrap().iter() {
                    println!("KEY: {}", key);
                    match std::fs::remove_file(format!("EZconfig{PATH_SEP}raw_tables{PATH_SEP}{}", key.as_str())) {
                        Ok(_) => (),
                        Err(e) => println!("LINE: {} - ERROR: {}", line!(), e),
                    }
                    
                }
                println!("{:?}", writer_thread_db_ref.buffer_pool.table_delete_list.read().unwrap());
                writer_thread_db_ref.buffer_pool.table_delete_list.write().unwrap().clear();


                for key in writer_thread_db_ref.buffer_pool.value_delete_list.write().unwrap().iter() {
                    match std::fs::remove_file(format!("EZconfig{PATH_SEP}raw_values{PATH_SEP}{}", key.as_str())) {
                        Ok(_) => (),
                        Err(e) => println!("LINE: {} - ERROR: {}", line!(), e),
                    }
                }
                writer_thread_db_ref.buffer_pool.value_delete_list.write().unwrap().clear();

                for (key, table_lock) in writer_thread_db_ref.buffer_pool.tables.read().unwrap().iter() {
                    let mut table_naughty_list = writer_thread_db_ref.buffer_pool.table_naughty_list.write().unwrap();
                    if table_naughty_list.contains(key) {
                        let mut file = match std::fs::File::create(format!("EZconfig{PATH_SEP}raw_tables{PATH_SEP}{}", key.as_str())) {
                            Ok(file) => file,
                            Err(e) => {
                                println!("LINE: {} - ERROR: {}", line!(), e);
                                continue
                            },
                        };
                        file.write(&table_lock.read().unwrap().to_binary()).expect(&format!("Panic of line: {} of server_networking. The backup file could not be written.", line!()));
                        table_naughty_list.remove(key);
                    }
                }
                
                for (key, value_lock) in writer_thread_db_ref.buffer_pool.values.read().unwrap().iter() {
                    let mut value_naughty_list = writer_thread_db_ref.buffer_pool.value_naughty_list.write().unwrap();
                    if value_naughty_list.contains(key) {
                        let mut file = std::fs::File::create(format!("EZconfig{PATH_SEP}raw_values{PATH_SEP}{}", key.as_str())).expect(&format!("Panic of line: {} of server_networking. The backup file could not be created.", line!()));
                        file.write(&value_lock.read().unwrap().write_to_binary()).expect(&format!("Panic of line: {} of server_networking. The backup file could not be written.", line!()));
                        value_naughty_list.remove(key);
                    }
                }


            }
        }); // Thread that writes in memory tables to disk
        // ########################################################################################################################

        loop {
            // Reading instructions
            let (stream, client_address) = match listener.accept() {
                Ok((n,m)) => (n, m),
                Err(e) => {return Err(EzError::Io(e.kind()));},
            };
            println!("Accepted connection from: {}", client_address);

            let db_con = database.clone();
            let thread_s = s.clone();
            let (mut connection, username) = match establish_connection(thread_s, stream, db_con) {
                Ok(c) => c,
                Err(e) => continue,
            };

            let instructions = match connection.receive_c1() {
                Ok(ins) => ins,
                Err(_) => continue,
            };
            
            let db_ref = database.clone();
            // Spawn a thread to handle establishing connections
            outer_scope.spawn(move || {
                

                println!("Parsing instructions...");
                match parse_instruction(
                    &instructions, 
                    db_ref.clone(),
                ) {
                    Ok(i) => match i {
                        
                        Instruction::Query => {
                            match handle_query_request(
                                &mut connection,
                                db_ref.clone(),
                                &username
                            ) {
                                Ok(_) => {
                                    println!("Operation finished!");
                                },
                                Err(e) => {
                                    println!("Operation failed because: {}", e);
                                },
                            }
                        },
                        Instruction::NewUser => {
                            match handle_new_user_request(
                                &mut connection,
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
                        connection.send_c2(e.to_string().as_bytes())?;
                        println!("Thread finished on error: {e}");
                    },
                    
                };
    
                Ok::<(), EzError>(())
            });
        }




    });


    Ok(())

}

/// Parses the inctructions sent by the client. Will be rewritten soon to accomodate EZQL
pub fn parse_instruction(
    instructions: &[u8], 
    database: Arc<Database>,
) -> Result<Instruction, EzError> {
    println!("calling: parse_instruction()");

    
    println!("parsing 3...");
    let username = KeyString::try_from(&instructions[0..64])?;
    let action = KeyString::try_from(&instructions[64..128])?;
    let table_name = KeyString::try_from(&instructions[128..192])?;
    let blank = KeyString::try_from(&instructions[192..256])?;

    if table_name.as_str() == "All" {
        return Err(EzError::Instruction(InstructionError::InvalidTable("Table cannot be called 'All'".to_owned())));
    }

    println!("parsing 4...");
    let confirmed = match action.as_str() {
        "Querying" => {
            Ok(Instruction::Query)
            
        }
        "MetaListTables" => {
            if user_has_permission(table_name.as_str(), Permission::Read, username.as_str(), database.users.clone()) {
                Ok(Instruction::MetaListTables)
            } else {
                Err(EzError::Authentication(AuthenticationError::Permission))
            }
        },
        "MetaListKeyValues" => {
            if user_has_permission(table_name.as_str(), Permission::Read, username.as_str(), database.users.clone()) {
                Ok(Instruction::MetaListKeyValues)
            } else {
                Err(EzError::Authentication(AuthenticationError::Permission))
            }
        },
        "MetaNewUser" => {
            if user_has_permission(table_name.as_str(), Permission::Write, username.as_str(), database.users.clone()) {
                Ok(Instruction::NewUser)
            } else {
                Err(EzError::Authentication(AuthenticationError::Permission))
            }
        }
        _ => Err(EzError::Instruction(InstructionError::Invalid(action.to_string()))),
    };

    confirmed
}



#[cfg(test)]
mod tests {

    use super::*;

    // #[test]
    // fn test_server_init() {
    //     run_server("127.0.0.1:3004").unwrap();
    // }

}
