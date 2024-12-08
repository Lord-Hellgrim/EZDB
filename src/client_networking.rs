use std::str::{self};

use eznoise::{initiate_connection, Connection};

use crate::auth::User;
use crate::db_structure::{ColumnTable, KeyString, Metadata, Value};
use crate::ezql::{KvQuery, Query};
use crate::utilities::{bytes_to_str, ksf, parse_response, u64_from_le_slice, EzError, Instruction};
use crate::PATH_SEP;


pub enum Response {
    Message(String),
    Table(ColumnTable),
}

pub fn make_connection(address: &str, username: &str, password: &str) -> Result<Connection, EzError> {
    let mut connection = initiate_connection(address)?;
    let mut auth_buffer = [0u8;1024];
    if username.len() > 512 || password.len() > 512 {
        return Err(EzError::Authentication("Username and password must each be less than 512 bytes".to_owned()))
    }
    auth_buffer[0..username.len()].copy_from_slice(username.as_bytes());
    auth_buffer[512..512+password.len()].copy_from_slice(username.as_bytes());
    
    connection.SEND_C1(&auth_buffer)?;
    println!("HERE!!!");

    Ok(connection)
}

/// Send an EZQL query to the database server
pub fn oneshot_query(
    address: &str,
    username: &str,
    password: &str,
    query: &Query,
) -> Result<ColumnTable, EzError> {

    let mut connection = make_connection(address, username, password).unwrap();

    let query = query.to_binary();
    let mut packet = Vec::new();
    packet.extend_from_slice(KeyString::from("QUERY").raw());
    packet.extend_from_slice(&query);
    connection.SEND_C1(&packet)?;
    
    let response = connection.RECEIVE_C2()?;

    match ColumnTable::from_binary(Some("RESULT"), &response) {
        Ok(table) => Ok(table),
        Err(e) => Err(e),
    }
}

pub fn send_query(connection: &mut Connection, query: &Query) -> Result<ColumnTable, EzError> {

    let query = query.to_binary();
    let mut packet = Vec::new();
    packet.extend_from_slice(KeyString::from("QUERY").raw());
    packet.extend_from_slice(&query);
    connection.SEND_C1(&packet)?;
    
    let response = connection.RECEIVE_C2()?;

    match ColumnTable::from_binary(Some("RESULT"), &response) {
        Ok(table) => Ok(table),
        Err(e) => Err(e),
    }
}

pub fn send_kv_queries(connection: &mut Connection, queries: &[KvQuery]) -> Result<Vec<Result<Option<Value>, EzError>>, EzError> {

    let mut packet = Vec::new();
    packet.extend_from_slice(ksf("KVQUERY").raw());
    for query in queries {
        packet.extend_from_slice(&query.to_binary());
    }

    connection.SEND_C1(&packet)?;

    let response = connection.RECEIVE_C2()?;

    let number_of_responses = u64_from_le_slice(&response[0..8]) as usize;
    let mut offsets = Vec::new();
    for i in 0..number_of_responses {
        let offset = u64_from_le_slice(&response[8+8*i..8+8*i+8]) as usize;
        offsets.push(offset);
    }
    
    let body = &response[8+8*offsets.len()..];
    
    let mut results = Vec::new();
    for i in 0..offsets.len() {
        let current_blob: &[u8];
        if i == offsets.len() {
            current_blob = &body[offsets[i]..];
        } else {
            current_blob = &body[offsets[i]..offsets[i+1]];
        }

        let tag = KeyString::try_from(&current_blob[0..64])?;
        match tag.as_str() {
            "VALUE" => {
                let name = KeyString::try_from(&current_blob[64..128])?;
                let len = u64_from_le_slice(&current_blob[128..136]) as usize;
                let value = current_blob[136..136+len].to_vec();
                let value = Value {name, body: value, metadata: Metadata::new("bleh")};
                results.push(Ok(Some(value)));
            },
            "ERROR" => {

            } ,
            "NONE"  => {
                results.push(Ok(None));
            },
            other => {
                results.push(Err(EzError::Query(format!("Incorrectly formatted response. '{}' is not a valid response type", other))));
            }
        }

    }

    todo!()

}


#[cfg(test)]
mod tests {
    #![allow(unused)]
    use std::{fs::remove_file, path::Path, time::Duration};

    use crate::{db_structure::ColumnTable, ezql::RangeOrListOrAll, utilities::ksf};

    use super::*;

    #[test]
    fn test_send_SELECT() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let query = Query::SELECT { 
            table_name: ksf("good_table"),
            primary_keys: RangeOrListOrAll::All,
            columns: vec![ksf("id"), ksf("name"), ksf("price")],
            conditions: Vec::new() 
        };

        let response = oneshot_query(address, username, password, &query).unwrap();
        println!("{}", response);
    }

    #[test]
    fn test_open_connection() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let query = Query::SELECT { 
            table_name: ksf("good_table"),
            primary_keys: RangeOrListOrAll::All,
            columns: vec![ksf("id"), ksf("name"), ksf("price")],
            conditions: Vec::new() 
        };

        let mut connection = make_connection(address, username, password).unwrap();

        let response1 = send_query(&mut connection, &query).unwrap();
        // std::thread::sleep(Duration::from_millis(500));
        let response2 = send_query(&mut connection, &query).unwrap();
        println!("{}", response1);
        println!("{}", response2);

        assert_eq!(response1, response2);
    }


}
