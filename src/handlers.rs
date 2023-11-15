use std::{sync::{Arc, Mutex}, collections::HashMap, io::Write};

use crate::{networking_utilities::*, db_structure::StrictTable, logger::get_current_time, auth::User};



pub fn handle_download_request(mut connection: &mut Connection, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<(), ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let mut mutex_binding = global_tables.lock().unwrap();
    let mut requested_table = mutex_binding.get_mut(name).expect("Instruction parser should have verified table");
    let requested_csv = requested_table.to_csv_string();
    println!("Requested_csv: {}", requested_csv);

    let response = data_send_and_confirm(&mut connection, &requested_csv)?;

    if response == "OK" {
        requested_table.metadata.last_access = get_current_time();

        requested_table.metadata.times_accessed += 1;
        println!("metadata: {}", requested_table.metadata.to_string());

        return Ok(())
    } else {
        return Err(ServerError::Confirmation(response))
    }

}


pub fn handle_upload_request(mut connection: &mut Connection, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {

    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote OK as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let (csv, total_read) = receive_data(&mut connection)?;

    // Here we create a StrictTable from the csv and supplied name
    println!("About to check for strictness");
    let instant = std::time::Instant::now();
    match StrictTable::from_csv_string(&csv, name) {
        Ok(mut table) => {
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
            table.metadata.created_by = connection.peer.Username.clone();
        
            table.metadata.times_accessed += 1;
            
            global_tables.lock().unwrap().insert(table.name.clone(), table);

        },
        Err(e) => match connection.stream.write(e.to_string().as_bytes()){
            Ok(_) => println!("Informed client of unstrictness"),
            Err(e) => {return Err(ServerError::Io(e));},
        },
    };
    

    Ok("OK".to_owned())
}
    
    
pub fn handle_update_request(mut connection: &mut Connection, name: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {
    
    match connection.stream.write("OK".as_bytes()) {
        Ok(n) => println!("Wrote {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };

    let (csv, total_read) = receive_data(&mut connection)?;

    let mut mutex_binding = global_tables.lock().unwrap();

    let requested_table = mutex_binding.get_mut(name).expect("Instruction parser should have verified existence of table");
    
    match requested_table.update(&csv) {
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


pub fn handle_query_request(mut connection: &mut Connection, name: &str, query: &str, global_tables: Arc<Mutex<HashMap<String, StrictTable>>>) -> Result<String, ServerError> {
    match connection.stream.write("OK".as_bytes()) {
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

    let response = data_send_and_confirm(&mut connection, &requested_csv)?;
    
    if response == "OK" {
        return Ok("OK".to_owned())
    } else {
        return Err(ServerError::Confirmation(response))
    }
}


pub fn handle_new_user_request(user_string: &str, users: Arc<Mutex<HashMap<String, User>>>) -> Result<(), ServerError> {

    let user = User::from_str(user_string)?;

    let mut user_lock = users.lock().unwrap();
    user_lock.insert(user.Username.clone(), user);


    Ok(())

}

pub fn handle_kv_upload(mut connection: &mut Connection, name: &str, global_kv_table: Arc<Mutex<HashMap<String, &[u8]>>>) -> Result<(), ServerError> {



    Ok(())
}

pub fn handle_kv_update(mut connection: &mut Connection, name: &str, global_kv_table: Arc<Mutex<HashMap<String, &[u8]>>>) -> Result<(), ServerError> {

    

    Ok(())
}

pub fn handle_kv_download(mut connection: &mut Connection, name: &str, global_kv_table: Arc<Mutex<HashMap<String, &[u8]>>>) -> Result<(), ServerError> {

    

    Ok(())
}