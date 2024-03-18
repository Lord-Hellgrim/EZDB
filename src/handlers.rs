use std::{collections::HashMap, io::Write, sync::{Arc, RwLock}};

use crate::{auth::User, db_structure::{EZTable, DbVec, KeyString, Metadata, Value}, networking_utilities::*, server_networking::{Server, WriteThreadMessage, CONFIG_FOLDER}};

use crate::PATH_SEP;


/// Handles a download request from a client. A download request is a request for a whole table with no filters.
pub fn handle_download_request(
    connection: &mut Connection, 
    name: &str, 
    global_tables: Arc<RwLock<HashMap<KeyString, RwLock<EZTable>>>>, 
    disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>
) -> Result<(), ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };

    let global_read_binding = global_tables.read().unwrap();

    if !global_read_binding.contains_key(&KeyString::from(name)) {
        
    }

    let requested_table = global_read_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table").read().unwrap();
    let requested_csv = requested_table.to_string();
    println!("Requested_csv.len(): {}", requested_csv.len());

    let response = data_send_and_confirm(connection, requested_csv.as_bytes())?;

    if response == "OK" {
        
        let mut metadelta = requested_table.metadata.clone();
        metadelta.times_accessed += 1;
        metadelta.last_access = get_current_time();
        
        match disk_thread_sender.try_send(WriteThreadMessage::UpdateMetadata(metadelta, KeyString::from(name))) {
            Ok(_) => {},
            Err(e) => match e {
                crossbeam_channel::TrySendError::Disconnected(_e) => panic!("write thread has closed. Server is dead!!!"),
                crossbeam_channel::TrySendError::Full(_e) => todo!("I don't exactly know what to do here yet"),
            }
        };
        return Ok(());
        
    } else {
        return Err(ServerError::Confirmation(response))
    }

}

/// Handles an upload request from a client. An upload request uploads a whole csv string that will be parsed into a ColumnTable.
pub fn handle_upload_request(
    connection: &mut Connection, 
    name: &str,
    disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>
) -> Result<String, ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let csv = receive_data(connection)?;

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    match EZTable::from_csv_string(bytes_to_str(&csv)?, name, "test") {
        Ok(table) => {
            println!("About to write: {:x?}", "OK".as_bytes());
            match connection.stream.write("OK".as_bytes()) {
                Ok(_) => {
                    println!("Confirmed correctness with client");
                },
                Err(e) => {return Err(ServerError::Io(e.kind()));},
            };

            println!("Appending to global");
            println!("{:?}", &table.header);

            match disk_thread_sender.try_send(WriteThreadMessage::NewTable(table)) {
                Ok(_) => {},
                Err(e) => match e {
                    crossbeam_channel::TrySendError::Disconnected(_e) => panic!("write thread has closed. Server is dead!!!"),
                    crossbeam_channel::TrySendError::Full(_e) => todo!("I don't exactly know what to do here yet"),
                }
            };
        },
        Err(e) => match connection.stream.write(e.to_string().as_bytes()){
            Ok(_) => println!("Informed client of unstrictness"),
            Err(e) => {return Err(ServerError::Io(e.kind()));},
        },
    };
    

    Ok("OK".to_owned())
}
    
/// Handles an update request from a client. Executes a .update method on the designated table.
/// This will be rewritten to use EZQL soon
pub fn handle_update_request(connection: &mut Connection, name: &str, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<EZTable>>>>, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<String, ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let csv = receive_data(connection)?;
    let csv = bytes_to_str(&csv)?;

    match EZTable::from_csv_string(csv, "insert", "system") {
        Ok(table) => {
            match disk_thread_sender.try_send(WriteThreadMessage::UpdateTable(KeyString::from(name), table)) {
                Ok(_) => {
                    connection.stream.write_all("OK".as_bytes())?;
                },
                Err(e) => match e {
                    crossbeam_channel::TrySendError::Disconnected(_e) => panic!("write thread has closed. Server is dead!!!"),
                    crossbeam_channel::TrySendError::Full(_e) => todo!("I don't exactly know what to do here yet"),
                }
            };
        },
        Err(e) => {
            connection.stream.write_all(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e));
        },
    };

    Ok("OK".to_owned())
}

/// This will be totally rewritten to handle EZQL. Don't worry about this garbage.
pub fn handle_query_request(connection: &mut Connection, name: &str, query: &str, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<EZTable>>>>, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<String, ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    let mutex_binding = global_tables.read().unwrap();
    let requested_table = mutex_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table");
    // PARSE INSTRUCTION
    let query_type: &str;
    match query.find("..") {
        Some(_) => query_type = "range",
        None => query_type = "list"
    };
    
    let requested_csv: String;
    if query_type == "range" {
        let parsed_query: Vec<&str> = query.split("..").collect();
        requested_csv = requested_table.read().unwrap().query_range((parsed_query[0], parsed_query[1]))?;
    } else {
        let parsed_query = query.split(',').collect();
        requested_csv = requested_table.read().unwrap().query_list(parsed_query)?;
    }

    let response = data_send_and_confirm(connection, requested_csv.as_bytes())?;
    
    if response == "OK" {
        return Ok("OK".to_owned())
    } else {
        return Err(ServerError::Confirmation(response))
    }
}

/// This will be rewritten to use EZQL soon.
pub fn handle_delete_request(connection: &mut Connection, name: &str, query: &str, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<EZTable>>>>, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<String, ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;
    
    let mutex_binding = global_tables.write().unwrap();
    let requested_table = mutex_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table");
    // PARSE INSTRUCTION
    let query_type: &str;
    match query.find("..") {
        Some(_) => query_type = "range",
        None => query_type = "list"
    };
    
    let requested_csv: String;
    if query_type == "range" {
        let parsed_query: Vec<&str> = query.split("..").collect();
        requested_csv = requested_table.write().unwrap().query_range((parsed_query[0], parsed_query[1]))?;
    } else {
        let parsed_query = query.split(',').collect();
        requested_csv = requested_table.write().unwrap().query_list(parsed_query)?;
    }

    let response = data_send_and_confirm(connection, requested_csv.as_bytes())?;
    
    if response == "OK" {
        return Ok("OK".to_owned())
    } else {
        return Err(ServerError::Confirmation(response))
    }
}

/// Handles a create user request from a client. The user requesting the new user must have permission to create users
pub fn handle_new_user_request(connection: &mut Connection, user_string: &str, users: Arc<RwLock<HashMap<KeyString, RwLock<User>>>>, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<(), ServerError> {

    let user: User = ron::from_str(user_string).unwrap();

    match disk_thread_sender.try_send(WriteThreadMessage::MetaNewUser(user)) {
        Ok(_) => {
            connection.stream.write_all("OK".as_bytes())?;
        },
        Err(e) => match e {
            crossbeam_channel::TrySendError::Disconnected(_e) => panic!("write thread has closed. Server is dead!!!"),
            crossbeam_channel::TrySendError::Full(_e) => todo!("I don't exactly know what to do here yet"),
        }
    };

    Ok(())

}

/// Handles a key value upload request.
pub fn handle_kv_upload(connection: &mut Connection, key: &str, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    
    let value = receive_data(connection)?;
    let value = Value::new(&connection.user, &value);
    // println!("value: {:?}", value);

    println!("About to check for strictness");
    match connection.stream.write("OK".as_bytes()) {
        Ok(_) => {
            println!("Confirmed correctness with client");
        },
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };

    println!("Appending to global");
    match disk_thread_sender.try_send(WriteThreadMessage::NewKeyValue(KeyString::from(key), value)) {
        Ok(_) => {
            connection.stream.write_all("OK".as_bytes())?;
        },
        Err(e) => match e {
            crossbeam_channel::TrySendError::Disconnected(_e) => panic!("write thread has closed. Server is dead!!!"),
            crossbeam_channel::TrySendError::Full(_e) => todo!("I don't exactly know what to do here yet"),
        }
    };
    


    Ok(())

}

/// Overwrites an existing value. If no existing value has this key, return error.
pub fn handle_kv_update(connection: &mut Connection, key: &str, global_kv_table: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    
    let value = receive_data(connection)?;
    let value = Value::new(&connection.user, &value);
    // println!("value: {:?}", value);

    println!("About to check for strictness");
    match connection.stream.write("OK".as_bytes()) {
        Ok(_) => {
            println!("Confirmed correctness with client");
        },
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };

    println!("Appending to global");
    match disk_thread_sender.try_send(WriteThreadMessage::NewKeyValue(KeyString::from(key), value)) {
        Ok(_) => {
            connection.stream.write_all("OK".as_bytes())?;
        },
        Err(e) => match e {
            crossbeam_channel::TrySendError::Disconnected(_e) => panic!("write thread has closed. Server is dead!!!"),
            crossbeam_channel::TrySendError::Full(_e) => todo!("I don't exactly know what to do here yet"),
        }
    };
    


    Ok(())

}

/// Handles a download request of a value associated with the given key. 
/// Returns error if no value with that key exists or if user doesn't have permission.
pub fn handle_kv_download(connection: &mut Connection, name: &str, global_kv_table: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let read_binding = global_kv_table.read().unwrap();
    let requested_value = read_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table").read().unwrap();

    let response = data_send_and_confirm(connection, &requested_value.body)?;

    if response == "OK" {

        let mut metadelta = requested_value.metadata.clone();
        metadelta.times_accessed += 1;
        metadelta.last_access = get_current_time();

        match disk_thread_sender.try_send(WriteThreadMessage::UpdateMetadata(metadelta, KeyString::from(name))) {
            Ok(_) => {},
            Err(e) => match e {
                crossbeam_channel::TrySendError::Disconnected(_e) => panic!("write thread has closed. Server is dead!!!"),
                crossbeam_channel::TrySendError::Full(_e) => todo!("I don't exactly know what to do here yet"),
            }
        };

        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}

/// Handles the request for the list of tables.
pub fn handle_meta_list_tables(connection: &mut Connection, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<EZTable>>>>, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    let mutex_binding = global_tables.read().unwrap();
    let mut memory_table_names: Vec<&KeyString> = mutex_binding.keys().collect();

    let mut disk_table_names = Vec::new();
    for file in std::fs::read_dir(format!("EZconfig{PATH_SEP}raw_tables")).unwrap() {
        match file {
            Ok(f) => disk_table_names.push(KeyString::from(f.file_name().into_string().unwrap().as_str())),
            Err(e) => println!("error while reading directory entries: {e}"),
        }
    }

    for item in disk_table_names.iter() {
        memory_table_names.push(item);
    }

    memory_table_names.sort();
    memory_table_names.dedup();

    let mut printer = String::new();
    for table_name in memory_table_names {
        printer.push_str(table_name.as_str());
        printer.push('\n');
    }


    println!("tables_list: {}", printer);

    let response = data_send_and_confirm(connection, printer.as_bytes())?;

    if response == "OK" {
        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}

/// Handles the request for a list of keys with associated binary blobs
pub fn handle_meta_list_key_values(connection: &mut Connection, global_kv_table: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>, disk_thread_sender: crossbeam_channel::Sender<WriteThreadMessage>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    let mutex_binding = global_kv_table.read().unwrap();

    let mut memory_table_names: Vec<&KeyString> = mutex_binding.keys().collect();

    let mut disk_table_names = Vec::new();
    for file in std::fs::read_dir(format!("EZconfig{PATH_SEP}key_value")).unwrap() {
        match file {
            Ok(f) => disk_table_names.push(KeyString::from(f.file_name().into_string().unwrap().as_str())),
            Err(e) => println!("error while reading directory entries: {e}"),
        }
    }

    for item in disk_table_names.iter() {
        memory_table_names.push(item);
    }

    memory_table_names.sort();
    memory_table_names.dedup();

    let mut printer = String::new();
    for key in memory_table_names {
        printer.push_str(key.as_str());
        printer.push('\n');
    }

    println!("tables_list: {}", printer);

    let response = data_send_and_confirm(connection, printer.as_bytes())?;

    if response == "OK" {
        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}



// ################################# MESSAGE HANDLERS ##########################################################

pub fn handle_message_update_metadata(server_handle: Arc<Server>, metadata: Metadata, table_name: KeyString) -> Result<(), ServerError> {

    let global_tables = server_handle.buffer_pool.tables.write().unwrap();
    let mut table = match global_tables.get(&table_name) {
        Some(t) => t.write().unwrap(),
        None => todo!("Need to redesign error handling."),
    };

    table.metadata = metadata;

    Ok(())
}

pub fn handle_message_update_table(server_handle: Arc<Server>, table_name: KeyString, table: EZTable) -> Result<(), ServerError> {

    let global_tables = server_handle.buffer_pool.tables.write().unwrap();
    let mut source_table = match global_tables.get(&table_name) {
        Some(t) => t.write().unwrap(),
        None => todo!("Need to redesign error handling."),
    };

    source_table.update(&table)?;

    Ok(())
}

pub fn handle_message_drop_table(server_handle: Arc<Server>, table_name: KeyString) -> Result<(), ServerError> {
    server_handle.buffer_pool.tables.write().unwrap().remove(&table_name);
    Ok(())
}

pub fn handle_message_delete_rows(server_handle: Arc<Server>, table_name: KeyString, rows: DbVec) -> Result<(), ServerError> {

    let mut the_table = server_handle.buffer_pool.tables.write().unwrap();
    let mut mutatable = the_table.get_mut(&table_name).unwrap().write().unwrap();

    mutatable.delete_by_vec(rows)?;

    Ok(())
}

pub fn handle_message_new_table(server_handle: Arc<Server>, table: EZTable) -> Result<(), ServerError> {

    server_handle.buffer_pool.tables.write().unwrap().insert(table.name.clone(), RwLock::new(table));

    Ok(())
}

pub fn handle_message_new_key_value(server_handle: Arc<Server>, key: KeyString, value: Value) -> Result<(), ServerError> {

    server_handle.buffer_pool.values .write().unwrap().insert(key, RwLock::new(value));


    Ok(())
}

pub fn handle_message_update_key_value(server_handle: Arc<Server>, key: KeyString, value: Value) -> Result<(), ServerError> {

    server_handle.buffer_pool.values.write().unwrap().entry(key).and_modify(|v| *v = RwLock::new(value));

    Ok(())
}

pub fn handle_message_meta_new_user(server_handle: Arc<Server>, user: User) -> Result<(), ServerError> {

    server_handle.users.write().unwrap().insert(KeyString::from(user.username.as_str()), RwLock::new(user));


    Ok(())
}