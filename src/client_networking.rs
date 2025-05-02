use std::str::{self};

use eznoise::{initiate_connection, Connection};

use crate::db_structure::{ColumnTable, Metadata, Value};
use crate::ezql::{KvQuery, Query};
use crate::utilities::{ksf, kv_query_results_from_binary, KeyString, u64_from_le_slice, ErrorTag, EzError};
// use crate::PATH_SEP;


pub enum Response {
    Message(String),
    Table(ColumnTable),
}


pub fn make_connection(address: &str, username: &str, password: &str) -> Result<Connection, EzError> {
    let mut connection = initiate_connection(address)?;
    let mut auth_buffer = [0u8;1024];
    if username.len() > 512 || password.len() > 512 {
        return Err(EzError{ tag: ErrorTag::Authentication, text: "Username and password must each be less than 512 bytes".to_owned()})
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

    send_query(&mut connection, query)
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

    let results = kv_query_results_from_binary(&response)?;

    

    Ok(results)
}


#[cfg(test)]
mod tests {
    #![allow(unused, non_snake_case)]
    use std::{fs::remove_file, path::Path, time::Duration};

    use crate::{db_structure::{ColumnTable, DbValue}, ezql::{Condition, RangeOrListOrAll, Test, TestOp}, testing_tools::random_column_table, utilities::{ez_hash, ksf}};

    use super::*;

    #[test]
    fn test_api() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";

        let mut connection = make_connection(address, username, password).unwrap();

        let mut query = Query::new_select("good_table")
            .and_condition(ksf("id"), TestOp::Equals, 1);

    }

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
    fn test_send_CREATE() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let query = Query::CREATE {
            table: random_column_table(5, 1000),
        };

        println!("query hash: {:?}", ez_hash(&query.to_binary()));

        let response = oneshot_query(address, username, password, &query).unwrap();
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

        let mut response1 = send_query(&mut connection, &query).unwrap();
        std::thread::sleep(Duration::from_millis(500));
        let mut response2 = send_query(&mut connection, &query).unwrap();
        println!("{}", response1);
        println!("{}", response2);

        assert_eq!(response1.sort(), response2.sort());
    }

    #[test]
    fn test_kv_query() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let mut connection = make_connection(address, username, password).unwrap();

        let kv_queries = vec![
            KvQuery::Read(ksf("core1")),
            KvQuery::Read(ksf("core2")),
            KvQuery::Read(ksf("core3")),
            KvQuery::Read(ksf("core4")),
        ];

        let results = send_kv_queries(&mut connection, &kv_queries).unwrap();
        for result in results {
            println!("{:?}", result);
        }
    }


}
