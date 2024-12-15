use std::sync::Arc;

use eznoise::Connection;

use crate::{db_structure::{ColumnTable, DbColumn, DbType, KeyString}, ezql::{filter_keepers, OpOrCond, RangeOrListOrAll, Statistic, Update}, server_networking::Database, utilities::{ErrorTag, EzError}};

pub const BUFCAP: usize = 65535;


pub fn zero_slice(slice: &mut [u8]) {
    for i in slice {
        *i = 0;
    }
}

pub struct StreamBuffer<'a> {
    connection: &'a mut Connection,
    end_pointer: usize,
    buffer: [u8;BUFCAP],
}

impl <'a> StreamBuffer<'a> {

    pub fn new(connection: &mut Connection) -> StreamBuffer {
        let buffer = [0u8;BUFCAP];
        let end_pointer = 0;
        StreamBuffer {
            connection,
            end_pointer,
            buffer,
        }
    }

    pub fn flush(&mut self) -> Result<(), EzError> {
        let result = match self.connection.SEND_C2(&self.buffer[0..self.end_pointer]) {
            Ok(_) => Ok(()),
            Err(_) => Err(EzError { tag: ErrorTag::Io, text: format!("Something went wrong while trying to flush StreamBuffer.") }),
        };
        self.end_pointer = 0;
        zero_slice(&mut self.buffer);
        result
    }

    pub fn push(&mut self, data: &[u8]) -> Result<(), EzError> {
        let mut data_pointer = 0;
        let mut len = data.len();
        let mut space = BUFCAP - self.end_pointer;
        while len > space {
            self.buffer[self.end_pointer..BUFCAP].copy_from_slice(&data[data_pointer..data_pointer + space]);
            data_pointer += space;
            len -= space;
            self.flush()?;
            space = BUFCAP;
        }
        if len > 0 {
            self.buffer[self.end_pointer..self.end_pointer+len].copy_from_slice(&data[data_pointer..data_pointer + len]);
            self.end_pointer += len;
        }

        Ok(())
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

pub fn execute_queries(queries: Vec<Query>, database: Arc<Database>, streambuffer: &mut StreamBuffer) -> Result<(), EzError> {
    
    for query in queries {
        match query {
            Query::CREATE { table } => todo!(),
            Query::SELECT { table_name, primary_keys, columns, conditions } => {
                if database.contains_table(table_name) {
                    let tables = database.buffer_pool.tables.read().unwrap();
                    let table = tables.get(&table_name).unwrap().read().unwrap();
                    let keepers = filter_keepers(&conditions, &primary_keys, &table)?;
                    let mut table_size = 0;
                    for col in columns {
                        if table.columns.contains_key(&col) {
                            match &table.columns[&col] {
                                DbColumn::Ints(vec) => table_size += 4*keepers.len(),
                                DbColumn::Texts(vec) => types.push(DbType::Text),
                                DbColumn::Floats(vec) => types.push(DbType::Float),
                            }
                        }
                    }
                }
            },
            Query::LEFT_JOIN { left_table_name, right_table_name, match_columns, primary_keys } => todo!(),
            Query::INNER_JOIN => todo!(),
            Query::RIGHT_JOIN => todo!(),
            Query::FULL_JOIN => todo!(),
            Query::UPDATE { table_name, primary_keys, conditions, updates } => todo!(),
            Query::INSERT { table_name, inserts } => todo!(),
            Query::DELETE { primary_keys, table_name, conditions } => todo!(),
            Query::SUMMARY { table_name, columns } => todo!(),
        }
    }

    Ok(())

}

#[cfg(test)]
mod tests {
    use crate::client_networking::make_connection;

    use super::*;

    #[test]
    fn test_streambuffer() {
        let address = "127.0.0.1:3004";
        let username = "admin";
        let password = "admin";
        let mut connection = make_connection(address, username, password).unwrap();

        let mut streambuffer = StreamBuffer::new(&mut connection);
        streambuffer.push(&[1,2,3,4,5,6]).unwrap();
        println!("len: {}", streambuffer.end_pointer);
        streambuffer.push(&[16u8;100000]).unwrap();
        println!("len: {}", streambuffer.end_pointer);

    }
}