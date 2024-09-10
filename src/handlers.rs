use std::{collections::BTreeMap, fs::File, sync::{atomic::Ordering, Arc, RwLock}};

use aes_gcm::Key;
use ezcbor::cbor::decode_cbor;

use crate::{auth::{check_permission, User}, utilities::{receive_encrypted_data, send_encrypted_data}}; 
use crate::db_structure::{ColumnTable, KeyString, Value};
use crate::ezql::{execute_EZQL_queries, parse_serial_query}; 
use crate::utilities::{Connection, EzError, data_send_and_confirm, get_current_time, bytes_to_str, };
use crate::server_networking::Database;

use crate::PATH_SEP;


/// Handles a download request from a client. A download request is a request for a whole table with no filters.
pub fn handle_download_request(
    connection: &mut Connection, 
    name: &str, 
    database: Arc<Database>,

) -> Result<(), EzError> {
    println!("calling: handle_download_request()");

    let requested_csv: Vec<u8>;
    {
        let global_read_binding = database.buffer_pool.tables.read().unwrap();
    
        let requested_table = global_read_binding.get(&KeyString::from(name)).expect("Instruction parser should have verified table").read().unwrap();
        requested_csv = requested_table.write_to_binary();
        println!("Requested_csv.len(): {}", requested_csv.len());
    }

    let response = data_send_and_confirm(connection, &requested_csv)?;

    if response == "OK" {
        
        let global_read_binding = database.buffer_pool.tables.read().unwrap();

        let requested_table = global_read_binding[&KeyString::from(name)].read().unwrap();

        requested_table.metadata.times_accessed.fetch_add(1, Ordering::Relaxed);
        requested_table.metadata.last_access.store(get_current_time(), Ordering::Relaxed);
        
        Ok(())
        
    } else {
        Err(EzError::Confirmation(response))
    }

}

/// Handles an upload request from a client. An upload request uploads a whole csv string that will be parsed into a ColumnTable.
pub fn handle_upload_request(
    connection: &mut Connection,
    database: Arc<Database>,
    name: &str,

) -> Result<String, EzError> {
    println!("calling: handle_upload_request()");

    

    let csv = receive_encrypted_data(connection)?;

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    let table = match ColumnTable::from_csv_string(bytes_to_str(&csv)?, name, &connection.user) {
        Ok(table) => {
            println!("About to write: {:x?}", "OK".as_bytes());
            send_encrypted_data("OK".as_bytes(), connection)?;
           table
        },
        Err(e) => {
            send_encrypted_data(e.to_string().as_bytes(), connection)?;
            return Err(EzError::Strict(e))
        },
    };
    {
        let table_name = table.name;
        println!("table_name: {}", table_name);
        database.buffer_pool.tables.write().unwrap().insert(KeyString::from(name), RwLock::new(table));
        database.buffer_pool.table_naughty_list.write().unwrap().insert(table_name);
        
    }
    

    Ok("OK".to_owned())
}
    
/// Handles an update request from a client. Executes a .update method on the designated table.
/// This will be rewritten to use EZQL soon
pub fn handle_update_request(
    connection: &mut Connection, 
    name: &str, 
    database: Arc<Database>,
) -> Result<String, EzError> {
    println!("calling: handle_update_request()");


    let csv = receive_encrypted_data(connection)?;
    let csv = bytes_to_str(&csv)?;

    match ColumnTable::from_csv_string(csv, "insert", "system") {
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
            send_encrypted_data(e.to_string().as_bytes(), connection)?;
            return Err(EzError::Strict(e));
        },
    };

    Ok("OK".to_owned())
}


pub fn handle_query_request(
    connection: &mut Connection,
    database: Arc<Database>
) -> Result<String, EzError> {
    // PARSE INSTRUCTION
    println!("calling: handle_query_request()");
    
    
    todo!()
    // let queries = parse_serial_query(query)?;

    // check_permission(&queries, &connection.user, database.users.clone())?;
    // let requested_table = match execute_EZQL_queries(queries, database) {
    //     Ok(res) => match res {
    //         Some(table) => table.write_to_binary(),
    //         None => "None.".as_bytes().to_vec(),
    //     },
    //     Err(e) => format!("ERROR -> Could not process query because of error: '{}'", e.to_string()).as_bytes().to_vec(),
    // };

    // let response = data_send_and_confirm(connection, &requested_table)?;
    
    // if response == "OK" {
    //     Ok("OK".to_owned())
    // } else {
    //     Err(EzError::Confirmation(response))
    // }
}

/// This will be rewritten to use EZQL soon.
pub fn handle_delete_request(
    connection: &mut Connection, 
    name: &str,
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_delete_request()");

    
    let mut mutex_binding = database.buffer_pool.tables.write().unwrap();
    mutex_binding.remove(&KeyString::from(name)).expect("Instruction parser should have verified table");

    database.buffer_pool.table_delete_list.write().unwrap().insert(KeyString::from(name));
    
    send_encrypted_data("OK".as_bytes(), connection)?;

    Ok(())
}

/// Handles a create user request from a client. The user requesting the new user must have permission to create users
pub fn handle_new_user_request(
    connection: &mut Connection,
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_new_user_request()");

    
    
    // let user: User = decode_cbor(user_string).unwrap();
    // let mut user_lock = database.users.write().unwrap();
    // user_lock.insert(KeyString::from(user.username.as_str()), RwLock::new(user));
    
    send_encrypted_data("OK".as_bytes(), connection)?;

    Ok(())

}


/// Handles a key value upload request.
pub fn handle_kv_upload(
    connection: &mut Connection, 
    key: &str, 
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_kv_upload()");


    println!("about to receive data");
    
    let value = receive_encrypted_data(connection)?;
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

    send_encrypted_data("OK".as_bytes(), connection)?;
    
    Ok(())

}

/// Overwrites an existing value. If no existing value has this key, return error.
pub fn handle_kv_update(
    connection: &mut Connection, 
    key: &str, 
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_kv_update()");

    
    let value = receive_encrypted_data(connection)?;
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

    send_encrypted_data("OK".as_bytes(), connection)?;

    Ok(())

}


/// This will be rewritten to use EZQL soon.
pub fn handle_kv_delete(
    connection: &mut Connection, 
    name: &str,
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_kv_delete()");


    let mut mutex_binding = database.buffer_pool.values.write().unwrap();
    mutex_binding.remove(&KeyString::from(name)).expect("Instruction parser should have verified value");

    database.buffer_pool.value_delete_list.write().unwrap().insert(KeyString::from(name));
    
    send_encrypted_data("OK".as_bytes(), connection)?;

    Ok(())
}



/// Handles a download request of a value associated with the given key. 
/// Returns error if no value with that key exists or if user doesn't have permission.
pub fn handle_kv_download(
    connection: &mut Connection, 
    name: &str, 
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_kv_download()");


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
        Err(EzError::Confirmation(response))
    }

}

/// Handles the request for the list of tables.
pub fn handle_meta_list_tables(
    connection: &mut Connection, 
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_meta_list_tables()");


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
        Err(EzError::Confirmation(response))
    }

}

/// Handles the request for a list of keys with associated binary blobs
pub fn handle_meta_list_key_values(
    connection: &mut Connection, 
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_meta_list_key_values()");

    
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
        Err(EzError::Confirmation(response))
    }

}