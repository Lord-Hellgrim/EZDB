use std::{sync::{Arc, Mutex}, collections::HashMap, io::Write};

use crate::{networking_utilities::*, db_structure::{ColumnTable, Value}, auth::User};

use smartstring::{SmartString, LazyCompact};

pub type KeyString = SmartString<LazyCompact>;





pub fn handle_download_request(mut connection: &mut Connection, name: &str, global_tables: Arc<Mutex<HashMap<KeyString, ColumnTable>>>) -> Result<(), ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let mut mutex_binding = global_tables.lock().unwrap();
    let requested_table = mutex_binding.get_mut(name).expect("Instruction parser should have verified table");
    let requested_csv = requested_table.to_string();
    println!("Requested_csv.len(): {}", requested_csv.len());

    let response = data_send_and_confirm(&mut connection, requested_csv.as_bytes())?;

    if response == "OK" {
        requested_table.metadata.last_access = get_current_time();

        requested_table.metadata.times_accessed += 1;
        println!("metadata: {}", requested_table.metadata.to_string());

        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}


pub fn handle_upload_request(mut connection: &mut Connection, name: &str, global_tables: Arc<Mutex<HashMap<KeyString, ColumnTable>>>) -> Result<String, ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };
    connection.stream.flush()?;


    let (csv, total_read) = receive_data(&mut connection)?;

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    let instant = std::time::Instant::now();
    match ColumnTable::from_csv_string(bytes_to_str(&csv)?, name, "test") {
        Ok(mut table) => {
            println!("About to write: {:x?}", format!("{}", total_read).as_bytes());
            println!("Which is: {}", bytes_to_str(format!("{}", total_read).as_bytes())?);
            match connection.stream.write(format!("{}", total_read).as_bytes()) {
                Ok(_) => {
                    println!("Time to check strictness: {}", instant.elapsed().as_millis());
                    println!("Confirmed correctness with client");
                },
                Err(e) => {return Err(ServerError::Io(e));},
            };

            println!("Appending to global");
            println!("{:?}", &table.header);
            table.metadata.last_access = get_current_time();
            table.metadata.created_by = KeyString::from(connection.user.clone());
        
            table.metadata.times_accessed += 1;
            
            global_tables.lock().unwrap().insert(KeyString::from(table.name.clone()), table);

        },
        Err(e) => match connection.stream.write(e.to_string().as_bytes()){
            Ok(_) => println!("Informed client of unstrictness"),
            Err(e) => {return Err(ServerError::Io(e));},
        },
    };
    

    Ok("OK".to_owned())
}
    
    
pub fn handle_update_request(mut connection: &mut Connection, name: &str, global_tables: Arc<Mutex<HashMap<KeyString, ColumnTable>>>) -> Result<String, ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };
    connection.stream.flush()?;


    let (csv, total_read) = receive_data(&mut connection)?;

    let mut mutex_binding = global_tables.lock().unwrap();

    let requested_table = mutex_binding.get_mut(name).expect("Instruction parser should have verified existence of table");
    
    match requested_table.update_from_csv(bytes_to_str(&csv)?) {
        Ok(_) => {
            connection.stream.write(total_read.to_string().as_bytes())?;
        },
        Err(e) => {
            connection.stream.write(e.to_string().as_bytes())?;
            return Err(ServerError::Strict(e));
        },
    };

    Ok("OK".to_owned())
}


pub fn handle_query_request(mut connection: &mut Connection, name: &str, query: &str, global_tables: Arc<Mutex<HashMap<KeyString, ColumnTable>>>) -> Result<String, ServerError> {
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };
    connection.stream.flush()?;


    
    let mutex_binding = global_tables.lock().unwrap();
    let requested_table = mutex_binding.get(name).expect("Instruction parser should have verified table");
    let requested_csv: String;
    // PARSE INSTRUCTION
    let query_type;
    match query.find("..") {
        Some(_) => query_type = "range",
        None => query_type = "list"
    };

    if query_type == "range" {
        let parsed_query: Vec<&str> = query.split("..").collect();
        requested_csv = requested_table.query_range((parsed_query[0], parsed_query[1]))?;
    } else {
        let parsed_query = query.split(',').collect();
        requested_csv = requested_table.query_list(parsed_query)?;
    }

    let response = data_send_and_confirm(&mut connection, requested_csv.as_bytes())?;
    
    if response == "OK" {
        return Ok("OK".to_owned())
    } else {
        return Err(ServerError::Confirmation(response))
    }
}


pub fn handle_new_user_request(user_string: &str, users: Arc<Mutex<HashMap<KeyString, User>>>) -> Result<(), ServerError> {

    let user: User = ron::from_str(user_string).unwrap();

    let mut user_lock = users.lock().unwrap();
    user_lock.insert(KeyString::from(user.username.clone()), user);


    Ok(())

}

pub fn handle_kv_upload(mut connection: &mut Connection, name: &str, global_kv_table: Arc<Mutex<HashMap<KeyString, Value>>>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };
    connection.stream.flush()?;


    let (value, total_read) = receive_data(&mut connection)?;
    println!("value: {:?}", value);

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    match connection.stream.write(format!("{}", total_read).as_bytes()) {
        Ok(_) => {
            println!("Confirmed correctness with client");
        },
        Err(e) => {return Err(ServerError::Io(e));},
    };

    println!("Appending to global");
    
    let value = Value::new(&connection.user, &value);

    let mut global_kv_table_lock = global_kv_table.lock().unwrap();
    global_kv_table_lock.insert(KeyString::from(name), value);
    println!("value from table: {:x?}", global_kv_table_lock.get(name).unwrap().body);


    Ok(())

}

pub fn handle_kv_update(mut connection: &mut Connection, name: &str, global_kv_table: Arc<Mutex<HashMap<KeyString, Value>>>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };
    connection.stream.flush()?;


    let (value, total_read) = receive_data(&mut connection)?;

    // Here we create a ColumnTable from the csv and supplied name
    println!("About to check for strictness");
    match connection.stream.write(format!("{}", total_read).as_bytes()) {
        Ok(_) => {
            println!("Confirmed correctness with client");
        },
        Err(e) => {return Err(ServerError::Io(e));},
    };

    println!("Appending to global");
    
    let value = Value::new(&connection.user, &value);

    global_kv_table.lock().unwrap().insert(KeyString::from(name), value);


    Ok(())
}

pub fn handle_kv_download(mut connection: &mut Connection, name: &str, global_kv_table: Arc<Mutex<HashMap<KeyString, Value>>>) -> Result<(), ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };
    connection.stream.flush()?;


    let mut mutex_binding = global_kv_table.lock().unwrap();
    let requested_value = mutex_binding.get_mut(name).expect("Instruction parser should have verified table");

    println!("Requested_value: {:x?}", requested_value.body);

    let response = data_send_and_confirm(&mut connection, &requested_value.body)?;

    if response == "OK" {
        requested_value.metadata.last_access = get_current_time();

        requested_value.metadata.times_accessed += 1;
        println!("metadata: {}", requested_value.metadata.to_string());

        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}