use std::{collections::BTreeMap, io::Write, sync::{Arc, RwLock}, thread::current};

use crate::{auth::User, db_structure::{EZTable, DbVec, KeyString, Metadata, Value}, networking_utilities::*, server_networking::{Server, WriteThreadMessage, CONFIG_FOLDER}};

use crate::PATH_SEP;


/// Handles a download request from a client. A download request is a request for a whole table with no filters.
pub fn handle_download_request(
    connection: &mut Connection, 
    name: &str, 
    global_tables: Arc<RwLock<BTreeMap<KeyString, RwLock<EZTable>>>>, 

) -> Result<(), ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };

    let mut requested_csv = String::new();
    {
        let global_read_binding = global_tables.read().unwrap();
    
        let requested_table = global_read_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table").read().unwrap();
        requested_csv = requested_table.to_string();
        println!("Requested_csv.len(): {}", requested_csv.len());
    }

    let response = data_send_and_confirm(connection, requested_csv.as_bytes())?;

    if response == "OK" {
        
        let current_time = get_current_time();
        let global_read_binding = global_tables.read().unwrap();

        let mut requested_table = global_read_binding[&KeyString::from(name)].write().unwrap();

        requested_table.metadata.times_accessed += 1;
        requested_table.metadata.last_access = get_current_time();
        
        return Ok(());
        
    } else {
        return Err(ServerError::Confirmation(response))
    }

}

/// Handles an upload request from a client. An upload request uploads a whole csv string that will be parsed into a ColumnTable.
pub fn handle_upload_request(
    connection: &mut Connection,
    global_tables: Arc<RwLock<BTreeMap<KeyString, RwLock<EZTable>>>>,
    name: &str,

) -> Result<String, ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let csv = receive_data(connection)?;

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    let table = match EZTable::from_csv_string(bytes_to_str(&csv)?, name, &connection.user) {
        Ok(table) => {
            println!("About to write: {:x?}", "OK".as_bytes());
            match connection.stream.write("OK".as_bytes()) {
                Ok(_) => {
                    println!("Confirmed correctness with client");
                },
                Err(e) => {return Err(ServerError::Io(e.kind()));},
            };
           table
        },
        Err(e) => {
            connection.stream.write(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e))
        },
    };

    global_tables.write().unwrap().insert(KeyString::from(name), RwLock::new(table));
    

    Ok("OK".to_owned())
}
    
/// Handles an update request from a client. Executes a .update method on the designated table.
/// This will be rewritten to use EZQL soon
pub fn handle_update_request(connection: &mut Connection, name: &str, global_tables: Arc<RwLock<BTreeMap<KeyString, RwLock<EZTable>>>>,) -> Result<String, ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let csv = receive_data(connection)?;
    let csv = bytes_to_str(&csv)?;

    match EZTable::from_csv_string(csv, "insert", "system") {
        Ok(table) => {
            let read_binding = global_tables.read().unwrap();
            read_binding
            .get(&KeyString::from(name))
            .unwrap()
            .write()
            .unwrap()
            .update(&table)
            ?;
        },
        Err(e) => {
            connection.stream.write_all(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e));
        },
    };

    Ok("OK".to_owned())
}

/// This will be totally rewritten to handle EZQL. Don't worry about this garbage.
pub fn handle_query_request(connection: &mut Connection, name: &str, query: &str, global_tables: Arc<RwLock<BTreeMap<KeyString, RwLock<EZTable>>>>,) -> Result<String, ServerError> {
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
pub fn handle_delete_request(connection: &mut Connection, name: &str, query: &str, global_tables: Arc<RwLock<BTreeMap<KeyString, RwLock<EZTable>>>>,) -> Result<String, ServerError> {
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
pub fn handle_new_user_request(connection: &mut Connection, user_string: &str, users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>>,) -> Result<(), ServerError> {
    
    
    let user: User = ron::from_str(user_string).unwrap();
    let mut user_lock = users.write().unwrap();
    user_lock.insert(KeyString::from(user.username.as_str()), RwLock::new(user));
    
    connection.stream.write("OK".as_bytes())?;

    Ok(())

}


/// Handles a key value upload request.
pub fn handle_kv_upload(connection: &mut Connection, key: &str, global_kv_table: Arc<RwLock<BTreeMap<KeyString, RwLock<Value>>>>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    
    let value = receive_data(connection)?;
    let value = Value::new(key, &connection.user, &value);
    
    let mut kv_table_binding = global_kv_table.write().unwrap();
    kv_table_binding.insert(KeyString::from(key), RwLock::new(value));

    connection.stream.write("OK".as_bytes())?;
    
    Ok(())

}

/// Overwrites an existing value. If no existing value has this key, return error.
pub fn handle_kv_update(connection: &mut Connection, key: &str, global_kv_table: Arc<RwLock<BTreeMap<KeyString, RwLock<Value>>>>,) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    
    let value = receive_data(connection)?;
    let value = Value::new(key, &connection.user, &value);
    
    {
        global_kv_table
            .read()
            .unwrap()
            .get(&KeyString::from(key))
            .unwrap()
            .write()
            .unwrap()
            .update(value);
    }

    connection.stream.write("OK".as_bytes())?;

    Ok(())

}

/// Handles a download request of a value associated with the given key. 
/// Returns error if no value with that key exists or if user doesn't have permission.
pub fn handle_kv_download(connection: &mut Connection, name: &str, global_kv_table: Arc<RwLock<BTreeMap<KeyString, RwLock<Value>>>>,) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let read_binding = global_kv_table.read().unwrap();
    let requested_value = read_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table").read().unwrap();

    let response = data_send_and_confirm(connection, &requested_value.body)?;

    if response == "OK" {

        let values = global_kv_table
            .read()
            .unwrap();
        
        let mut this_value = values.get(&KeyString::from(name))
            .unwrap()
            .write()
            .unwrap();
        
        this_value.metadata.last_access = get_current_time();

        this_value.metadata.times_accessed += 1;
        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}

/// Handles the request for the list of tables.
pub fn handle_meta_list_tables(connection: &mut Connection, global_tables: Arc<RwLock<BTreeMap<KeyString, RwLock<EZTable>>>>,) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    let mut tables = BTreeMap::new();
    for (table_name, table) in global_tables.read().unwrap().iter() {
        tables.insert(table_name.clone(), table.read().unwrap().header.clone());
    }

    let mut printer = String::new();
    for (table_name, table_header) in tables.iter() {
        printer.push_str(table_name.as_str());
        printer.push('\n');
        for item in table_header {
            printer.push_str(&item.to_string());
            printer.push_str(";\t");
        }
        printer.push('\n');
    }
    printer.pop();

    let response = data_send_and_confirm(connection, printer.as_bytes())?;

    if response == "OK" {
        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}

/// Handles the request for a list of keys with associated binary blobs
pub fn handle_meta_list_key_values(connection: &mut Connection, global_kv_table: Arc<RwLock<BTreeMap<KeyString, RwLock<Value>>>>,) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    let mut values = Vec::new();
    for value_name in global_kv_table.read().unwrap().keys() {
        values.push(value_name.clone());
    }

    let mut printer = String::new();
    for value_name in values.iter() {
        printer.push_str(value_name.as_str());
        printer.push('\n');
    }
    printer.pop();

    let response = data_send_and_confirm(connection, printer.as_bytes())?;

    if response == "OK" {
        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}