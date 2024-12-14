use std::sync::Arc;

use eznoise::Connection;

use crate::{db_structure::{ColumnTable, KeyString}, ezql::{OpOrCond, RangeOrListOrAll, Statistic, Update}, server_networking::Database};


pub struct StreamBuffer<'a> {
    connection: &'a mut Connection,
    buffer: Box<[u8]>,
    end_pointer: usize,
}

impl <'a> StreamBuffer<'a> {

    pub fn flush()

    pub fn push(&mut self, data: &[u8]) {
        if self.end_pointer + data.len() > self.buffer.len() {
            let len = self.buffer.len();
            let diff = len - self.end_pointer;
            self.buffer[self.end_pointer..len].copy_from_slice(&data[0..diff]);

        }
    }
}


pub enum Query {
    CREATE{table: ColumnTable},
    SELECT{table_name: KeyString, primary_keys: RangeOrListOrAll, columns: Vec<KeyString>, conditions: Vec<OpOrCond>},
    LEFT_JOIN{left_table_name: KeyString, right_table_name: KeyString, match_columns: (KeyString, KeyString), primary_keys: RangeOrListOrAll},
    INNER_JOIN,
    RIGHT_JOIN,
    FULL_JOIN,
    UPDATE{table_name: KeyString, primary_keys: RangeOrListOrAll, conditions: Vec<OpOrCond>, updates: Vec<Update>},
    INSERT{table_name: KeyString, inserts: ColumnTable},
    DELETE{primary_keys: RangeOrListOrAll, table_name: KeyString, conditions: Vec<OpOrCond>},
    SUMMARY{table_name: KeyString, columns: Vec<Statistic>},
}

pub fn execute_queries(queries: Vec<Query>, database: Arc<Database>, connection: &mut Connection) {
    
    for query in queries {
        
    }

}