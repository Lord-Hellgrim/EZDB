use std::{collections::BTreeMap, sync::{Arc, RwLock}};

use ezcbor::cbor::decode_cbor;
use eznoise::Connection;

use crate::{auth::User, utilities::ErrorTag};
use crate::utilities::{EzError, KeyString};
use crate::server_networking::Database;

#[allow(unused)]
use crate::PATH_SEP;


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

    match connection.SEND_C2(printer.as_bytes()) {
        Ok(_) => Ok(()),
        Err(e) => Err(e.into()),
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

    connection.SEND_C2(printer.as_bytes())?;
    let response = connection.RECEIVE_C1()?;
    let response = String::from_utf8(response)?;

    if response == "OK" {
        Ok(())
    } else {
        Err(EzError{tag: ErrorTag::Confirmation, text: response})
    }

}


/// Handles a create user request from a client. The user requesting the new user must have permission to create users
pub fn handle_new_user_request(
    connection: &mut Connection,
    database: Arc<Database>,
) -> Result<(), EzError> {
    println!("calling: handle_new_user_request()");

    let user_bytes = connection.RECEIVE_C1()?;
    let user: User = decode_cbor(&user_bytes)?;

    let mut user_lock = database.users.write().unwrap();
    user_lock.insert(KeyString::from(user.username.as_str()), RwLock::new(user));
    
    connection.SEND_C2("OK".as_bytes())?;

    Ok(())

}