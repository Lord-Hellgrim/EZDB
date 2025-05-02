use std::collections::{BTreeMap, HashMap};
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::os::fd::{AsFd, AsRawFd, FromRawFd};
use std::sync::{Arc, RwLock};
use std::str::{self};
use std::convert::{TryFrom, From};

use ezcbor::cbor::{decode_cbor, Cbor};
use eznoise::{Connection, KeyPair};
use nix::sys::epoll::{Epoll, EpollCreateFlags, EpollEvent, EpollFlags};

use crate::auth::{check_kv_permission, check_permission, user_has_permission, Permission, User};
use crate::disk_utilities::{BufferPool, MAX_BUFFERPOOL_SIZE};
use crate::ezql::{execute_EZQL_queries, execute_kv_queries, parse_kv_queries_from_binary, parse_queries_from_binary};
use crate::logging::Logger;
// use crate::query_execution::StreamBuffer;
use crate::thread_pool::{initialize_thread_pool, Job};
use crate::utilities::*;
use crate::PATH_SEP;

pub const INSTRUCTION_LENGTH: usize = 284;
pub const CONFIG_FOLDER: &str = "EZconfig/";
pub const MAX_PENDING_MESSAGES: usize = 10;
pub const PROCESS_MESSAGES_INTERVAL: u64 = 10;   // The number of seconds that pass before the database processes all pending write messages.



// Need to redesign the server multithreading before I continue. If I have to lock the "table of tables" for each query,
// then there's no point to multithreading.


pub enum StreamStatus {
    Fresh,
    Handshake1,
    Handshake2,
    Authenticated,
    Veteran(usize /* number of requests processed */),
    Http,
}

impl StreamStatus {
    pub fn bump(&mut self) {
        *self = match self {
            StreamStatus::Fresh => StreamStatus::Handshake1,
            StreamStatus::Handshake1 => StreamStatus::Handshake2,
            StreamStatus::Handshake2 => StreamStatus::Authenticated,
            StreamStatus::Authenticated => StreamStatus::Veteran(1),
            StreamStatus::Veteran(x) => StreamStatus::Veteran(*x+1),
            StreamStatus::Http => unreachable!("Should never bump an http connection")
        };
    }
}

pub struct Database {
    pub buffer_pool: BufferPool,
    pub users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>>,
    pub logger: Logger,
}

impl Database {
    pub fn init() -> Result<Database, EzError> {
        println!("calling: Database::init()");

        if !std::path::Path::new("EZconfig").is_dir() {
            println!("config does not exist");
            std::fs::create_dir("EZconfig").expect("Need IO access to initialize database");
            std::fs::create_dir("EZconfig/raw_tables").expect("Need IO access to initialize database");
            std::fs::create_dir("EZconfig/raw_values").expect("Need IO access to initialize database");
            std::fs::create_dir("EZconfig/log").expect("Need IO access to initialize database");
        } else {
            println!("config folder exists");
        }

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

    pub fn contains_table(&self, table_name: KeyString) -> bool {
        self.buffer_pool.tables.read().unwrap().contains_key(&table_name)
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
    
    println!("Initializing database");
    let database = Arc::new(Database::init()?);
    
    let s = get_server_static_keys();
    
    println!("Starting server...\n###########################");

    println!("Binding to address: {address}");
    let listener = match TcpListener::bind(address) {
        Ok(value) => value,
        Err(e) => {return Err(EzError{tag: ErrorTag::Io, text: e.kind().to_string()});},
    };

    listener.set_nonblocking(true)?;

    let epoll = Epoll::new(EpollCreateFlags::empty()).unwrap();

    epoll.add(listener.as_fd(), EpollEvent::new(EpollFlags::EPOLLIN, listener.as_raw_fd() as u64)).unwrap();

    let mut events = vec![EpollEvent::empty(); 100];

    let mut unsigned_streams: HashMap<u64, (TcpStream, u64)> = HashMap::new();
    let mut virgin_connections: HashMap<u64, Connection> = HashMap::new();
    let mut stream_statuses: HashMap<u64, (StreamStatus, Option<eznoise::HandshakeState>)> = HashMap::new();
    let mut pending_jobs = HashMap::new();
    let mut read_buffer = [0u8;4096];

    let thread_handler = initialize_thread_pool(8, database.clone());
    
    loop {
        
        let number_of_events = match epoll.wait(&mut events, 5 as u8) {
            Ok(number) => number,
            Err(e) => {
                println!("{}", e);
                0
            },
        };

        // println!("number of events: {}", number_of_events);

        let mut remove_list = Vec::new();
        for (fd, con) in virgin_connections.iter() {
            println!("con.opened: {}", con.opened);
            if get_current_time() - con.opened > 5 {
                stream_statuses.remove(fd);
                remove_list.push(fd.clone());
            }
        }
        
        for fd in remove_list {
            if let Some(conn) = virgin_connections.remove(&fd) {
                // Remove from epoll
                println!("Removed dead connection");
                epoll.delete(conn.stream.as_fd()).unwrap_or_else(|e| {
                    eprintln!("Failed to remove fd {} from epoll: {}", fd, e);
                });
                
                // Explicitly close the stream
                drop(conn);
            }
        }
        
        let mut remove_list = Vec::new();
        for (fd, con) in thread_handler.open_connections.lock().unwrap().iter() {
            println!("Open connection number: {}", fd);
            if get_current_time() - con.opened > 5 {
                stream_statuses.remove(fd);
                remove_list.push(fd.clone());
            }
        }

        for fd in remove_list {
            
            if let Some(conn) = thread_handler.open_connections.lock().unwrap().remove(&fd) {
                // Remove from epoll
                println!("Removed dead connection");

                epoll.delete(conn.stream.as_fd()).unwrap_or_else(|e| {
                    eprintln!("Failed to remove fd {} from epoll: {}", fd, e);
                });
        
                // Explicitly close the stream
                drop(conn);
            }
        }

        let mut remove_list = Vec::new();
        for (fd, (_, timestamp)) in unsigned_streams.iter() {
            println!("con.opened: {}", timestamp);
            if get_current_time() - timestamp > 5 {
                stream_statuses.remove(fd);
                remove_list.push(fd.clone());
            }
        }
        
        for fd in remove_list {
            if let Some((conn, _)) = unsigned_streams.remove(&fd) {
                // Remove from epoll
                println!("Removed dead connection");
                epoll.delete(conn.as_fd()).unwrap_or_else(|e| {
                    eprintln!("Failed to remove fd {} from epoll: {}", fd, e);
                });
                
                // Explicitly close the stream
                drop(conn);
            }
        }
        
        let db_con = database.clone();
        'events: for i in 0..number_of_events {
            println!("Number of events: {}", number_of_events);
            if events[i].data() == listener.as_raw_fd() as u64 {
                let (mut stream, client_address) = match listener.accept() {
                    Ok((n,m)) => (n, m),
                    Err(e) => return Err(EzError{tag: ErrorTag::Io, text: e.kind().to_string()}),
                };
                println!("Accepted connection from: {}", client_address);
                let key = stream.as_raw_fd() as u64;
                
                let handshakestate = Some(eznoise::ESTABLISH_CONNECTION_STEP_1(&mut stream, s.clone()).unwrap());
                let handshakestate = Some(eznoise::ESTABLISH_CONNECTION_STEP_2(&mut stream, handshakestate.unwrap()).unwrap());
                stream_statuses.insert(key, (StreamStatus::Handshake1, handshakestate));

                epoll.add(stream.as_fd(), EpollEvent::new(EpollFlags::EPOLLIN, key)).unwrap();
                unsigned_streams.insert(key, (stream, get_current_time()));
            } else {
                let fd = events[i].data();
                match stream_statuses.remove(&fd) {
                    Some((mut status, handshakestate)) => match status {
                        StreamStatus::Fresh => {

                        },
                        StreamStatus::Handshake1 => {
                            println!("handshake1");
                            let (stream, _) = unsigned_streams.remove(&fd).unwrap();
                            let connection = eznoise::ESTABLISH_CONNECTION_STEP_3(stream, handshakestate.unwrap()).unwrap();
                            connection.stream.set_nonblocking(true)?;
                            if virgin_connections.contains_key(&fd) {
                                todo!()
                            } else {
                                virgin_connections.insert(fd, connection);
                            }
                            status.bump();
                            stream_statuses.insert(fd, (status, None));
                        },
                        StreamStatus::Handshake2 => {
                            println!("handshake2");
                            let inner_db_con = db_con.clone();
                            let connection = virgin_connections.get_mut(&fd).unwrap();
                            match authenticate_client(connection, inner_db_con) {
                                Ok(_) => {
                                    status.bump();
                                    stream_statuses.insert(fd, (status, None));
                                },
                                Err(e) => {
                                    interior_log(e);
                                    virgin_connections.remove(&fd);
                                    let stream = unsafe { TcpStream::from_raw_fd(fd as i32) };
                                    epoll.delete( stream.as_fd() ).unwrap();
                                }
                            };
                        }
                        StreamStatus::Authenticated => {
                            println!("Authenticated");
                            let mut connection = match virgin_connections.remove(&fd) {
                                Some(x) => x,
                                None => panic!("Unexpectedly dropped authenticated client"),
                            };
                            let mut expected_length_bytes = [0u8;8];
                            match connection.stream.read_exact(&mut expected_length_bytes) {
                                Ok(_) => (),
                                Err(e) => {
                                    println!("Failed to receive command because: {}", e);
                                    continue 'events;
                            },
                            };
                            let expected_length = u64_from_le_slice(&expected_length_bytes) as usize;
                            let mut pending_job: Vec<u8> = Vec::new();
                            let mut total_read = 0;
                            for _ in 0..10_000 {
                                println!("STUCK HERE!");
                                let to_read = std::cmp::min(4096, expected_length - total_read);
                                let bytes_received= match connection.stream.read(&mut read_buffer[..to_read]) {
                                    Ok(x) => x,
                                    Err(e) => {
                                        match e.kind() {
                                            std::io::ErrorKind::WouldBlock => {
                                                println!("Insanity check");
                                                break
                                            },
                                            e => {
                                                println!("Error: {}", e);
                                                drop(connection);
                                                continue 'events
                                            },
                                        }
                                    },
                                };
                                pending_job.extend_from_slice(&read_buffer[0..bytes_received]);
                                total_read += bytes_received;
                            }
                            
                            println!("SANITY CHECK!: Expected: {}\t\tTotal_read: {}", expected_length, total_read);
                            if total_read == expected_length {
                                thread_handler.push_job(Job{connection, data: pending_job});
                                status.bump();
                            } else {
                                pending_jobs.insert(fd, (expected_length, total_read, pending_job));
                            }


                            // match read_known_length(&mut connection.stream) {
                            //     Ok(data) => {
                            //         thread_handler.push_job(Job{connection, data});
                            //     },
                            //     Err(e) => {
                            //         println!("Failed to receive command because: {}", e);
                            //     }
                            // };
                            stream_statuses.insert(fd, (status, None));



                        },
                        StreamStatus::Veteran(_rounds) => {
                            println!("Veteran");
                            let mut connection = match thread_handler.open_connections.lock().unwrap().remove(&fd) {
                                Some(x) => x,
                                None => panic!("Unexpectedly dropped authenticated client"),
                            };

                            let (expected_length, mut total_read, mut pending_job) = match pending_jobs.remove(&fd) {
                                Some(x) => x,
                                None => {
                                    let mut expected_length_bytes = [0u8;8];
                                    match connection.stream.read_exact(&mut expected_length_bytes) {
                                        Ok(_) => (),
                                        Err(e) => {
                                            println!("Failed to receive command because: {}", e);
                                            
                                            continue 'events
                                        },
                                    };
                                    let expected_length = u64_from_le_slice(&expected_length_bytes) as usize;
                                    let pending_job: Vec<u8> = Vec::new();
                                    let total_read = 0;
                                    (expected_length, total_read, pending_job)
                                },
                            };

                            loop {
                                let to_read = std::cmp::min(4096, expected_length - total_read);
                                let bytes_received= match connection.stream.read(&mut read_buffer[..to_read]) {
                                    Ok(x) => x,
                                    Err(e) => {
                                        match e.kind() {
                                            std::io::ErrorKind::WouldBlock => break,
                                            _ => {
                                                drop(connection);
                                                continue 'events
                                            },
                                        }
                                    },
                                };
                                pending_job.extend_from_slice(&read_buffer[0..bytes_received]);
                                total_read += bytes_received;
                            }

                            if total_read == expected_length {
                                thread_handler.push_job(Job{connection, data: pending_job});
                                status.bump();
                            } else {
                                pending_jobs.insert(fd, (expected_length, total_read, pending_job));
                            }
                            
                            // match read_known_length(&mut connection.stream) {
                            //     Ok(data) => {
                            //         thread_handler.push_job(Job{connection, data});
                            //     },
                            //     Err(e) => {
                            //         println!("Failed to receive command because: {}", e);
                            //     }
                            // };

                            stream_statuses.insert(fd, (status, None));
                        },
                        StreamStatus::Http => todo!(),
                    },
                    None => println!("Stream must have been dropped unexpectedly. Carry on.")
                }
            }
        }

    }

}

pub fn answer_query(binary: &[u8], connection: &mut Connection, db_ref: Arc<Database>) -> Result<Vec<u8>, EzError> {

    println!("query hash: {:?}", ez_hash(binary));

    let queries = parse_queries_from_binary(&binary)?;

    check_permission(&queries, connection.peer.as_str(), db_ref.users.clone())?;
    let requested_table = match execute_EZQL_queries(queries, db_ref) {
        Ok(res) => match res {
            Some(table) => table.to_binary(),
            None => "None.".as_bytes().to_vec(),
        },
        Err(e) => format!("ERROR -> Could not process query because of error: '{}'", e.to_string()).as_bytes().to_vec(),
    };

    Ok(requested_table)
}

pub fn answer_kv_query(binary: &[u8], connection: &mut Connection, db_ref: Arc<Database>) -> Result<Vec<u8>, EzError> {

    let queries = parse_kv_queries_from_binary(&binary)?;

    check_kv_permission(&queries, connection.peer.as_str(), db_ref.users.clone())?;
    let query_results: Vec<Result<Option<crate::db_structure::Value>, EzError>> = execute_kv_queries(queries, db_ref);

    let binary = kv_query_results_to_binary(&query_results);
    

    Ok(binary)

}

pub fn perform_administration(_binary: &[u8], _db_ref: Arc<Database>) -> Result<Vec<u8>, EzError> {
    todo!()
}

pub fn perform_maintenance(db_ref: Arc<Database>) -> Result<(), EzError> {
    println!("calling_perform_maintenance()");
    // println!("{:?}", db_ref.buffer_pool.table_delete_list.read().unwrap());
    for key in db_ref.buffer_pool.table_delete_list.read().unwrap().iter() {
        println!("KEY: {}", key);
        match std::fs::remove_file(format!("EZconfig{PATH_SEP}raw_tables{PATH_SEP}{}", key.as_str())) {
            Ok(_) => (),
            Err(e) => println!("LINE: {} - ERROR: {}", line!(), e),
        }
    }

    // println!("{:?}", db_ref.buffer_pool.table_delete_list.read().unwrap());
    db_ref.buffer_pool.table_delete_list.write().unwrap().clear();

    for key in db_ref.buffer_pool.value_delete_list.write().unwrap().iter() {
        match std::fs::remove_file(format!("EZconfig{PATH_SEP}raw_values{PATH_SEP}{}", key.as_str())) {
            Ok(_) => (),
            Err(e) => println!("LINE: {} - ERROR: {}", line!(), e),
        }
    }
    db_ref.buffer_pool.value_delete_list.write().unwrap().clear();

    for (key, table_lock) in db_ref.buffer_pool.tables.read().unwrap().iter() {
        // println!("key: {}", key);
        let mut table_naughty_list = db_ref.buffer_pool.table_naughty_list.write().unwrap();
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
    
    for (key, value) in db_ref.buffer_pool.values.read().unwrap().iter() {
        let mut value_naughty_list = db_ref.buffer_pool.value_naughty_list.write().unwrap();
        if value_naughty_list.contains(key) {
            let mut file = std::fs::File::create(format!("EZconfig{PATH_SEP}raw_values{PATH_SEP}{}", key.as_str())).expect(&format!("Panic of line: {} of server_networking. The backup file could not be created.", line!()));
            file.write(&value.write_to_binary()).expect(&format!("Panic of line: {} of server_networking. The backup file could not be written.", line!()));
            value_naughty_list.remove(key);
        }
    }

    Ok(())
}

pub fn interior_log(e: EzError) {
    println!("{}", e);
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
    let _blank = KeyString::try_from(&instructions[192..256])?;

    if table_name.as_str() == "All" {
        return Err(EzError{tag: ErrorTag::Instruction, text: "Table cannot be called 'All'".to_owned()});
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
                Err(EzError{tag: ErrorTag::Authentication, text: format!("User '{}' does not have permission to list tables", username)})
            }
        },
        "MetaListKeyValues" => {
            if user_has_permission(table_name.as_str(), Permission::Read, username.as_str(), database.users.clone()) {
                Ok(Instruction::MetaListKeyValues)
            } else {
                Err(EzError{tag: ErrorTag::Authentication, text: format!("User '{}' does not have permission to list key-value pairs", username)})

            }
        },
        "MetaNewUser" => {
            if user_has_permission(table_name.as_str(), Permission::Write, username.as_str(), database.users.clone()) {
                Ok(Instruction::NewUser)
            } else {
                Err(EzError{tag: ErrorTag::Authentication, text: format!("User '{}' does not have permission to create a new user", username)})

            }
        }
        _ => Err(EzError{tag: ErrorTag::Instruction, text: format!("Action: '{}' is not valid", action)}),
    };

    confirmed
}
