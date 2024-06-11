use std::{collections::BTreeMap, fs::File, io::Write, sync::{atomic::Ordering, Arc, RwLock}};

use crate::{auth::User, db_structure::{EZTable, KeyString, Value}, ezql::{execute_EZQL_queries, parse_serial_query}, networking_utilities::*, server_networking::Database};

use crate::PATH_SEP;


/// Handles a download request from a client. A download request is a request for a whole table with no filters.
pub fn handle_download_request(
    connection: &mut Connection, 
    name: &str, 
    database: Arc<Database>,

) -> Result<(), ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };

    let requested_csv: String;
    {
        let global_read_binding = database.buffer_pool.tables.read().unwrap();
    
        let requested_table = global_read_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table").read().unwrap();
        requested_csv = requested_table.to_string();
        println!("Requested_csv.len(): {}", requested_csv.len());
    }

    let response = data_send_and_confirm(connection, requested_csv.as_bytes())?;

    if response == "OK" {
        
        let global_read_binding = database.buffer_pool.tables.read().unwrap();

        let requested_table = global_read_binding[&KeyString::from(name)].read().unwrap();

        requested_table.metadata.times_accessed.fetch_add(1, Ordering::Relaxed);
        requested_table.metadata.last_access.store(get_current_time(), Ordering::Relaxed);
        
        Ok(())
        
    } else {
        Err(ServerError::Confirmation(response))
    }

}

/// Handles an upload request from a client. An upload request uploads a whole csv string that will be parsed into a ColumnTable.
pub fn handle_upload_request(
    connection: &mut Connection,
    database: Arc<Database>,
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
            match connection.stream.write_all("OK".as_bytes()) {
                Ok(_) => {
                    println!("Confirmed correctness with client");
                },
                Err(e) => {return Err(ServerError::Io(e.kind()));},
            };
           table
        },
        Err(e) => {
            connection.stream.write_all(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e))
        },
    };
    {
        let table_name = table.name;
        database.buffer_pool.tables.write().unwrap().insert(KeyString::from(name), RwLock::new(table));
        database.buffer_pool.table_naughty_list.write().unwrap().insert(table_name);
        let f = File::create(format!("EZconfig{PATH_SEP}raw_tables{PATH_SEP}{table_name}")).expect("There should never be a duplicate file name");
        database.buffer_pool.files.write().unwrap().insert(table_name, RwLock::new(f));
    }
    

    Ok("OK".to_owned())
}
    
/// Handles an update request from a client. Executes a .update method on the designated table.
/// This will be rewritten to use EZQL soon
pub fn handle_update_request(
    connection: &mut Connection, 
    name: &str, 
    database: Arc<Database>,
) -> Result<String, ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let csv = receive_data(connection)?;
    let csv = bytes_to_str(&csv)?;

    match EZTable::from_csv_string(csv, "insert", "system") {
        Ok(table) => {
            let read_binding = database.buffer_pool.tables.read().unwrap();
            read_binding
            .get(&KeyString::from(name))
            .unwrap()
            .write()
            .unwrap()
            .update(&table)
            ?;
            database.buffer_pool.table_naughty_list.write().unwrap().insert(table.name);
        },
        Err(e) => {
            connection.stream.write_all(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e));
        },
    };

    Ok("OK".to_owned())
}

pub fn handle_query_request(
    connection: &mut Connection, 
    query: &str, 
    database: Arc<Database>
) -> Result<String, ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    // PARSE INSTRUCTION

    let username = connection.user.clone();
    let queries = parse_serial_query(query)?;

    let requested_csv = match execute_EZQL_queries(queries, database, &username) {
        Ok(table) => table,
        Err(e) => format!("ERROR -> Could not process query because of error: '{}'", e.to_string()),
    };

    println!("result_table: {}", requested_csv);

    let response = data_send_and_confirm(connection, requested_csv.as_bytes())?;
    
    if response == "OK" {
        Ok("OK".to_owned())
    } else {
        Err(ServerError::Confirmation(response))
    }
}

/// This will be rewritten to use EZQL soon.
pub fn handle_delete_request(
    connection: &mut Connection, 
    name: &str,
    database: Arc<Database>,
) -> Result<(), ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;
    
    let mut mutex_binding = database.buffer_pool.tables.write().unwrap();
    mutex_binding.remove(&KeyString::from(name)).expect("Instruction parser should have verified table");

    let mut mutex_binding = database.buffer_pool.files.write().unwrap();
    if mutex_binding.contains_key(&KeyString::from(name)) {
        mutex_binding.remove(&KeyString::from(name)).expect("Instruction parser should have verified table");
        std::fs::remove_file(&format!("EZconfig{PATH_SEP}raw_tables{PATH_SEP}{name}"))?;
    }
    
    connection.stream.write_all("OK".as_bytes())?;

    Ok(())
}

/// Handles a create user request from a client. The user requesting the new user must have permission to create users
pub fn handle_new_user_request(
    connection: &mut Connection, 
    user_string: &str, 
    database: Arc<Database>,
) -> Result<(), ServerError> {
    
    
    let user: User = ron::from_str(user_string).unwrap();
    let mut user_lock = database.users.write().unwrap();
    user_lock.insert(KeyString::from(user.username.as_str()), RwLock::new(user));
    
    connection.stream.write_all("OK".as_bytes())?;

    Ok(())

}


/// Handles a key value upload request.
pub fn handle_kv_upload(
    connection: &mut Connection, 
    key: &str, 
    database: Arc<Database>,
) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    println!("about to receive data");
    
    let value = receive_data(connection)?;
    let value = Value::new(key, &connection.user, &value);
    let value_name = value.name;
    
    println!("data received");
    println!("data: {:x?}", value.body);
    {
        let mut kv_table_binding = match database.buffer_pool.values.try_write() {
            Ok(binding) => binding,
            Err(e) => panic!("error: {e}"),
        };
        println!("kv_table_bound");
        kv_table_binding.insert(KeyString::from(key), RwLock::new(value));
        println!("value inserted");
        database.buffer_pool.table_naughty_list.write().unwrap().insert(value_name);
        println!("naughty list updated");
    }
    println!("locks dropped");

    println!("data written");

    connection.stream.write_all("OK".as_bytes())?;
    
    Ok(())

}

/// Overwrites an existing value. If no existing value has this key, return error.
pub fn handle_kv_update(
    connection: &mut Connection, 
    key: &str, 
    database: Arc<Database>,
) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    
    let value = receive_data(connection)?;
    let value = Value::new(key, &connection.user, &value);
    
    {
        let value_name = value.name;
        database.buffer_pool.values
            .read()
            .unwrap()
            .get(&KeyString::from(key))
            .unwrap()
            .write()
            .unwrap()
            .update(value);
        database.buffer_pool.table_naughty_list.write().unwrap().insert(value_name);
    }

    connection.stream.write_all("OK".as_bytes())?;

    Ok(())

}

/// Handles a download request of a value associated with the given key. 
/// Returns error if no value with that key exists or if user doesn't have permission.
pub fn handle_kv_download(
    connection: &mut Connection, 
    name: &str, 
    database: Arc<Database>,
) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;


    let read_binding = database.buffer_pool.values.read().unwrap();
    let requested_value = read_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table").read().unwrap();

    println!("handle_kv_download: sending data");
    let response = data_send_and_confirm(connection, &requested_value.body)?;
    println!("handle_kv_download: data sent");

    if response == "OK" {
        {

            println!("handle_kv_download: about to lock values for metadata update");
            
            let values = database.buffer_pool.values
            .read()
            .unwrap();
        
            println!("handle_kv_download: values table lock acquired");

            let mut this_value = values.get(&KeyString::from(name))
                .unwrap()
                .read()
                .unwrap();

            println!("handle_kv_download: value entry lock acquired");

            this_value.metadata.last_access.store(get_current_time(), Ordering::Relaxed);
            this_value.metadata.times_accessed.fetch_add(1, Ordering::Relaxed);

            println!("handle_kv_download: metadata updated");
        }
        
        Ok(())
    } else {
        Err(ServerError::Confirmation(response))
    }

}

/// Handles the request for the list of tables.
pub fn handle_meta_list_tables(
    connection: &mut Connection, 
    database: Arc<Database>,
) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    let mut tables = BTreeMap::new();
    for (table_name, table) in database.buffer_pool.tables.read().unwrap().iter() {
        tables.insert(*table_name, table.read().unwrap().header.clone());
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
        Ok(())
    } else {
        Err(ServerError::Confirmation(response))
    }

}

/// Handles the request for a list of keys with associated binary blobs
pub fn handle_meta_list_key_values(
    connection: &mut Connection, 
    database: Arc<Database>,
) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;

    let mut values = Vec::new();
    for value_name in database.buffer_pool.values.read().unwrap().keys() {
        values.push(value_name.clone());
    }

    let mut printer = String::new();
    if values.len() != 0 {
        for value_name in values.iter() {
            printer.push_str(value_name.as_str());
            printer.push('\n');
        }
        printer.pop();
    } else {
        printer.push_str("No key value pairs in database");
    }

    let response = data_send_and_confirm(connection, printer.as_bytes())?;

    if response == "OK" {
        Ok(())
    } else {
        Err(ServerError::Confirmation(response))
    }

}