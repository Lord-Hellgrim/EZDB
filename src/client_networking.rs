use std::str::{self};

use eznoise::{initiate_connection, Connection};

use crate::auth::User;
use crate::db_structure::{ColumnTable, KeyString};
use crate::ezql::Query;
use crate::utilities::{bytes_to_str, parse_response, EzError, Instruction};
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
pub fn send_query(
    address: &str,
    username: &str,
    password: &str,
    query: Query,
) -> Result<ColumnTable, EzError> {
    println!("calling: send_query()");

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


#[cfg(test)]
mod tests {
    #![allow(unused)]
    use std::{fs::remove_file, path::Path};

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

        let response = send_query(address, username, password, query).unwrap();
        println!("{}", response);
    }


}
