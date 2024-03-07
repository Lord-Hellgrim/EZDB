use std::{collections::HashMap, io::Write, sync::{Arc, RwLock}};

use crate::{networking_utilities::*, db_structure::{ColumnTable, Value}, auth::User};

use smartstring::{SmartString, LazyCompact};
use crate::PATH_SEP;

pub type KeyString = SmartString<LazyCompact>;

/// Handles a download request from a client. A download request is a request for a whole table with no filters.
pub fn handle_download_request(connection: &mut Connection, name: &str, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<ColumnTable>>>>) -> Result<(), ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };

    let read_binding = global_tables.read().unwrap();
    let requested_table = read_binding.get(name).expect("Instruction parser should have verified table");
    let requested_csv = requested_table.read().unwrap().to_string();
    println!("Requested_csv.len(): {}", requested_csv.len());

    let response = data_send_and_confirm(connection, requested_csv.as_bytes())?;

    if response == "OK" {

        // TODO: Need to implement logging. Can't lock the table to write to metadata.
        return Ok(());
        // requested_table.metadata.last_access = get_current_time();

        // requested_table.metadata.times_accessed += 1;
        // println!("metadata: {}", requested_table.metadata);

    } else {
        return Err(ServerError::Confirmation(response))
    }

}

/// Handles an upload request from a client. An upload request uploads a whole csv string that will be parsed into a ColumnTable.
pub fn handle_upload_request(connection: &mut Connection, name: &str, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<ColumnTable>>>>) -> Result<String, ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let csv = receive_data(connection)?;

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    match ColumnTable::from_csv_string(bytes_to_str(&csv)?, name, "test") {
        Ok(mut table) => {
            println!("About to write: {:x?}", "OK".as_bytes());
            match connection.stream.write("OK".as_bytes()) {
                Ok(_) => {
                    println!("Confirmed correctness with client");
                },
                Err(e) => {return Err(ServerError::Io(e.kind()));},
            };

            println!("Appending to global");
            println!("{:?}", &table.header);
            table.metadata.last_access = get_current_time();
            table.metadata.created_by = KeyString::from(connection.user.clone());
        
            table.metadata.times_accessed += 1;
            
            global_tables.write().unwrap().insert(KeyString::from(table.name.clone()), RwLock::new(table));

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
pub fn handle_update_request(connection: &mut Connection, name: &str, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<ColumnTable>>>>) -> Result<String, ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let csv = receive_data(connection)?;

    let mut mutex_binding = global_tables.write().unwrap();

    let requested_table = mutex_binding.get_mut(name).expect("Instruction parser should have verified existence of table");
    
    match requested_table.write().unwrap().update_from_csv(bytes_to_str(&csv)?) {
        Ok(_) => {
            connection.stream.write_all("OK".as_bytes())?;
        },
        Err(e) => {
            connection.stream.write_all(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e));
        },
    };

    Ok("OK".to_owned())
}

/// This will be totally rewritten to handle EZQL. Don't worry about this garbage.
pub fn handle_query_request(connection: &mut Connection, name: &str, query: &str, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<ColumnTable>>>>) -> Result<String, ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    let mutex_binding = global_tables.read().unwrap();
    let requested_table = mutex_binding.get(name).expect("Instruction parser should have verified table");
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
pub fn handle_delete_request(connection: &mut Connection, name: &str, query: &str, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<ColumnTable>>>>) -> Result<String, ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    
    let mutex_binding = global_tables.write().unwrap();
    let requested_table = mutex_binding.get(name).expect("Instruction parser should have verified table");
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
pub fn handle_new_user_request(user_string: &str, users: Arc<RwLock<HashMap<KeyString, RwLock<User>>>>) -> Result<(), ServerError> {

    let user: User = ron::from_str(user_string).unwrap();

    let mut user_lock = users.write().unwrap();
    user_lock.insert(KeyString::from(user.username.clone()), RwLock::new(user));


    Ok(())

}

/// Handles a key value upload request.
pub fn handle_kv_upload(connection: &mut Connection, name: &str, global_kv_table: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let value = receive_data(connection)?;
    // println!("value: {:?}", value);

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    match connection.stream.write("OK".as_bytes()) {
        Ok(_) => {
            println!("Confirmed correctness with client");
        },
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };

    println!("Appending to global");
    
    let value = Value::new(&connection.user, &value);

    let mut global_kv_table_lock = global_kv_table.write().unwrap();
    global_kv_table_lock.insert(KeyString::from(name), RwLock::new(value));
    println!("value from table: {:x?}", global_kv_table_lock.get(name).unwrap().read().unwrap().body);


    Ok(())

}

/// Overwrites an existing value. If no existing value has this key, return error.
pub fn handle_kv_update(connection: &mut Connection, name: &str, global_kv_table: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let value = receive_data(connection)?;

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    match connection.stream.write("OK".as_bytes()) {
        Ok(_) => {
            println!("Confirmed correctness with client");
        },
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };

    println!("Appending to global");
    
    let value = Value::new(&connection.user, &value);

    global_kv_table.write().unwrap().insert(KeyString::from(name), RwLock::new(value));


    Ok(())
}

/// Handles a download request of a value associated with the given key. 
/// Returns error if no value with that key exists or if user doesn't have permission.
pub fn handle_kv_download(connection: &mut Connection, name: &str, global_kv_table: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let read_binding = global_kv_table.read().unwrap();
    let requested_value = read_binding.get(name).expect("Instruction parser should have verified table");

    // println!("Requested_value: {:x?}", requested_value.body);

    let response = data_send_and_confirm(connection, &requested_value.read().unwrap().body)?;

    if response == "OK" {

        // TODO: Implement proper logging to handle metadata. Can't lock the tables to write metadata.

        // requested_value.metadata.last_access = get_current_time();

        // requested_value.metadata.times_accessed += 1;
        // println!("metadata: {}", requested_value.metadata.to_string());

        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}

/// Handles the request for the list of tables.
pub fn handle_meta_list_tables(connection: &mut Connection, global_tables: Arc<RwLock<HashMap<KeyString, RwLock<ColumnTable>>>>) -> Result<(), ServerError> {

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
            Ok(f) => disk_table_names.push(KeyString::from(f.file_name().into_string().unwrap())),
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
        printer.push_str(table_name);
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
pub fn handle_meta_list_key_values(connection: &mut Connection, global_kv_table: Arc<RwLock<HashMap<KeyString, RwLock<Value>>>>) -> Result<(), ServerError> {

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
            Ok(f) => disk_table_names.push(KeyString::from(f.file_name().into_string().unwrap())),
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
        printer.push_str(key);
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
