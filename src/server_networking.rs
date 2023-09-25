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
        return Err(ServerError::Authentication(AuthenticationError::WrongPassword(pass_hash.to_owned())));
    } else {
        match action {
            "Querying" => {
                if !global_tables.lock().unwrap().contains_key(table_name) {
                    return Err(ServerError::Instruction(InstructionError::InvalidTable(table_name.to_owned())));
                } else {
                    Ok(Instruction::Query(table_name.to_owned(), query.to_owned()))
                }
            }
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


fn handle_download_request(mut stream: TcpStream, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    
    match stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let mutex_binding = global_tables.lock().unwrap();
    let requested_table = mutex_binding.get(name).expect("Instruction parser should have verified table");
    let requested_csv = requested_table.to_csv_string();

    let response = data_send_and_confirm(&mut stream, &requested_csv)?;

    if response == "OK" {
        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}


fn handle_upload_request(mut stream: TcpStream, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {

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
            Ok(_) => println!("Informed client of unstrictness"),
            Err(e) => {return Err(ServerError::Io(e));},
        },
    };

    Ok("OK".to_owned())
}
    
    
pub fn handle_update_request(mut stream: TcpStream, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {
    
    match stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let (csv, total_read) = receive_data(&mut stream)?;

    let mut mutex_binding = global_tables.lock().unwrap();

    let requested_table = mutex_binding.get_mut(name).expect("Instruction parser should have verified existence of table");
    
    match requested_table.update(&csv) {
        Ok(_) => {
            stream.write(total_read.to_string().as_bytes())?;
        },
        Err(e) => {
            stream.write(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e));
        },
    };

    Ok("OK".to_owned())
}


fn handle_query_request(mut stream: TcpStream, name: &str, query: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {
    match stream.write("OK".as_bytes()) {
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

    let response = data_send_and_confirm(&mut stream, &requested_csv)?;

    if response == "OK" {
        return Ok("OK".to_owned())
    } else {
        return Err(ServerError::Confirmation(response))
    }
}


pub fn server(address: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    println!("Starting server...\n###########################");
    println!("Binding to address: {address}");
    // ########### TIMING BLOCK ###############################################
    let start = rdtsc();
    let l = match TcpListener::bind(address) {
        Ok(value) => value,
        Err(e) => {return Err(ServerError::Io(e));},
    };
    let stop = rdtsc();
    time_print("Cycles to initialize TcpListener", stop-start);
    // ########################################################################
    println!("Reading users config into memory");

    // ########### TIMING BLOCK ###############################################
    let start = rdtsc();
    // let temp = String::from("admin;admin;127.0.0.1;false;ALL;ALL;true");
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
    let stop = rdtsc();
    time_print("Cycles to read Users", stop-start);
    // ########################################################################
    
    dbg!(&users);
    
    /* This is the main loop of the function. Here we accept incoming connections and process them */
    loop {
        // ########### TIMING BLOCK ###############################################
        let start = rdtsc();
        let begin = std::time::Instant::now();
        // Reading instructions
        let (mut stream, client_address) = match l.accept() {
            Ok((n,m)) => (n, m),
            Err(e) => {return Err(ServerError::Io(e));},
        };
        let stop = rdtsc();
        let end = begin.elapsed().as_millis();
        println!("Millis to accept: {}", end);
        time_print("Cycles to accept connection", stop-start);
        //#######################################################################
        println!("Accepted connection from: {}", client_address);

        // Spawn a new thread for each connection for some semblence of scalability
        // ####################### TIMING BLOCK ####################################
        let start = rdtsc();
        let thread_global = global_tables.clone();
        let thread_users = users.clone();
        let stop = rdtsc();
        time_print("Cycles to clone 2 arc", stop-start);
        // ##########################################################################
        
        std::thread::spawn(move || {

            // loop while connection is still open
            'connection: loop {

                let mut buffer: [u8; INSTRUCTION_BUFFER] = [0; INSTRUCTION_BUFFER];
                println!("Initialized string buffer");
                // ####################### TIMING BLOCK ####################################
                let start = rdtsc();
                match stream.read(&mut buffer) {
                    Ok(n) => {
                        println!("Read {n} bytes");
                    },
                    Err(e) => {
                        return Err(ServerError::Io(e));
                    },
                };
                let stop = rdtsc();
                time_print("Cycles to read instructions", stop-start);
                // ##########################################################################
                
                
                println!("Parsing instructions...");
                // ####################### TIMING BLOCK ####################################
                let start = rdtsc();
                match parse_instruction(&buffer, &thread_users, thread_global.clone()) {
                    Ok(i) => match i {
                        
                        Instruction::Upload(name) => {
                            let stop = rdtsc();
                            time_print("Cycles to parse instructions", stop-start);
                            // ##########################################################################
                            // ####################### TIMING BLOCK ####################################
                            let start = rdtsc();
                            match handle_upload_request(stream, &name, thread_global.clone()) {
                                Ok(_) => {
                                    let stop = rdtsc();
                                    time_print("Cycles to handle upload request", stop-start);
                                    // ##########################################################################
                                    println!("Operation finished!");
                                    return Ok(());
                                },
                                Err(e) => {return Err(e);}
                            }
                        },
                        Instruction::Download(name) => {
                            // ####################### TIMING BLOCK ####################################
                        let start = rdtsc();
                        match handle_download_request(stream, &name, thread_global.clone()) {
                            Ok(_) => {
                                    let stop = rdtsc();
                                    time_print("Cycles to clone 2 arc", stop-start);
                                    // ##########################################################################
                                    println!("Operation finished!");
                                    return Ok(());
                                },
                                Err(e) => {return Err(e);}
                            }
                        }
                        Instruction::Update(name) => {
                            // ####################### TIMING BLOCK ####################################
                            let start = rdtsc();
                            match handle_update_request(stream, &name, thread_global.clone()) {
                                Ok(_) => {
                                    let stop = rdtsc();
                                    time_print("Cycles to clone 2 arc", stop-start);
                                    // ##########################################################################
                                    println!("Operation finished!");
                                    return Ok(());
                                },
                                Err(e) => {return Err(e);},
                            }
                        }
                        Instruction::Query(table_name, query) => {
                            // ####################### TIMING BLOCK ####################################
                            let start = rdtsc();
                            match handle_query_request(stream, &table_name, &query, thread_global.clone()) {
                                Ok(_) => {
                                    let stop = rdtsc();
                                    time_print("Cycles to clone 2 arc", stop-start);
                                    // ##########################################################################
                                    println!("Operation finished!");
                                    return Ok(());
                                },
                                Err(e) => {return Err(e);},
                            }
                        }
                    }
                    
                    Err(e) => {
                        stream.write(&e.to_string().as_bytes())?;
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