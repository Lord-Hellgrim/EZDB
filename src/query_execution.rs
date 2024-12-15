use std::{collections::{BTreeMap, BTreeSet}, sync::Arc};

use eznoise::Connection;

use crate::{db_structure::{remove_indices, ColumnTable, DbColumn, HeaderItem, KeyString, TableKey}, ezql::{filter_keepers, OpOrCond, Operator, RangeOrListOrAll, Statistic, Test, Update}, server_networking::Database, utilities::{ErrorTag, EzError}};

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


pub struct ExecutionProgress {
    pub current_keepers: Vec<usize>,
}

#[derive(Debug, PartialEq, PartialOrd)]
pub enum DbSlice<'a> {
    Ints(&'a [i32]),
    Texts(&'a [KeyString]),
    Floats(&'a [f32]),
}

pub fn db_slice_from_column<'a>(column: &'a DbColumn) -> DbSlice<'a> {
    match column {
        DbColumn::Ints(vec) => DbSlice::Ints(vec.as_slice()),
        DbColumn::Texts(vec) => DbSlice::Texts(vec.as_slice()),
        DbColumn::Floats(vec) => DbSlice::Floats(vec.as_slice()),
    }
}


pub struct SubTable<'a> {
    pub header: BTreeSet<HeaderItem>,
    pub columns: BTreeMap<KeyString, DbSlice<'a>>,
}

impl SubTable<'_> {
    pub fn get_primary_key_col_index(&self) -> KeyString {
        
        for item in &self.header {
            if item.key == TableKey::Primary {
                return item.name;
            }
        }

        unreachable!("There should always be a primary key")
    }

    pub fn len(&self) -> usize {
        

        match &self.columns.values().next() {
            Some(column) => match column {
                DbSlice::Floats(col) => col.len(),
                DbSlice::Ints(col) => col.len(),
                DbSlice::Texts(col) => col.len(),
            },
            None => 0,
        }
    }
}

pub fn make_subtable<'a>(table: &'a ColumnTable, start_row: usize, end_row: usize, column_names: &[KeyString]) -> Result<SubTable<'a>, EzError> {    

    let mut subtable_header = BTreeSet::new();
    let mut subtable_columns = BTreeMap::new();
    for name in column_names {
        match table.columns.get(name) {
            Some(column) => {
                let header_item = table.header
                    .iter()
                    .find(|&x| x.name==*name)
                    .expect("This should be safe since the header must always have a corresponding entry to the column name")
                    .clone();
                subtable_header.insert(header_item);
                subtable_columns.insert(*name, db_slice_from_column(column));
            },
            None => return Err(EzError { tag: ErrorTag::Query, text: format!("No column named '{}' in table '{}'", name, table.name) }),
        };
    }

    Ok(
        SubTable {
            header: subtable_header,
            columns: subtable_columns,
        }
    )
}


pub fn keys_to_indexes_subtable(table: &SubTable, keys: &RangeOrListOrAll) -> Result<Vec<usize>, EzError> {
    // println!("calling: keys_to_indexes()");

    let mut indexes = Vec::new();

    match keys {
        RangeOrListOrAll::Range(ref start, ref stop) => {
            match table.columns[&table.get_primary_key_col_index()] {
                DbSlice::Ints(column) => {
                    let first = match column.binary_search(&start.to_i32()) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    let last = match column.binary_search(&stop.to_i32()) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    indexes = (first..last).collect();
                },
                DbSlice::Texts(column) => {
                    let first = match column.binary_search(start) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    let last = match column.binary_search(stop) {
                        Ok(x) => x,
                        Err(x) => x,
                    };
                    indexes = (first..last).collect();
                },
                DbSlice::Floats(_n) => {
                    unreachable!("There should never be a float primary key")
                },
            }
        },
        RangeOrListOrAll::List(ref keys) => {
            match table.columns[&table.get_primary_key_col_index()] {
                DbSlice::Ints(column) => {
                    if keys.len() > column.len() {
                        return Err(EzError{tag: ErrorTag::Query, text: "There are more keys requested than there are indexes to get".to_owned()})
                    }
                    let mut keys = keys.clone();
                    keys.sort();
                    let mut key_index: usize = 0;
                    for index in 0..keys.len() {
                        if column[index] == keys[key_index].to_i32() {
                            indexes.push(index);
                            key_index += 1;
                        }
                    }
                },
                DbSlice::Floats(_) => {
                    unreachable!("There should never be a float primary key")
                },
                DbSlice::Texts(column) => {
                    if keys.len() > column.len() {
                        return Err(EzError{tag: ErrorTag::Query, text: "There are more keys requested than there are indexes to get".to_owned()})
                    }
                    let mut keys = keys.clone();
                    keys.sort();
                    let mut key_index = 0;
                    for index in 0..column.len() {
                        if column[index] == keys[key_index] {
                            indexes.push(index);
                            key_index += 1;
                        }
                    }
                },
            }
        },
        RangeOrListOrAll::All => indexes = (0..table.len()).collect(),
    };

    Ok(indexes)
}


pub fn filter_keepers_subtable(conditions: &Vec<OpOrCond>, primary_keys: &RangeOrListOrAll, table: &SubTable) -> Result<Vec<usize>, EzError> {
    // println!("calling: filter_keepers()");

    let indexes = keys_to_indexes_subtable(table, primary_keys)?;
    
    if conditions.is_empty() {
        return Ok(indexes);
    }
    let mut keepers = Vec::<usize>::new();
    let mut current_op = Operator::OR;
    for condition in conditions.iter() {
        match condition {
            OpOrCond::Op(op) => current_op = *op,
            OpOrCond::Cond(cond) => {
                if !table.columns.contains_key(&cond.attribute) {
                    return Err(EzError{tag: ErrorTag::Query, text: format!("table does not contain column {}", cond.attribute)})
                }
                let column = &table.columns[&cond.attribute];
                if current_op == Operator::OR {
                    for index in &indexes {
                        match &cond.test {
                            Test::Equals(bar) => {
                                match column {
                                    DbSlice::Ints(col) => if col[*index] == bar.to_i32() {keepers.push(*index)},
                                    DbSlice::Floats(col) => if col[*index] == bar.to_f32() {keepers.push(*index)},
                                    DbSlice::Texts(col) => if col[*index] == *bar {keepers.push(*index)},
                                }
                            },
                            Test::NotEquals(bar) => {
                                match column {
                                    DbSlice::Ints(col) => if col[*index] != bar.to_i32() {keepers.push(*index)},
                                    DbSlice::Floats(col) => if col[*index] != bar.to_f32() {keepers.push(*index)},
                                    DbSlice::Texts(col) => if col[*index] != *bar {keepers.push(*index)},
                                }
                            },
                            Test::Less(bar) => {
                                match column {
                                    DbSlice::Ints(col) => if col[*index] < bar.to_i32() {keepers.push(*index)},
                                    DbSlice::Floats(col) => if col[*index] < bar.to_f32() {keepers.push(*index)},
                                    DbSlice::Texts(col) => if col[*index] < *bar {keepers.push(*index)},
                                }
                            },
                            Test::Greater(bar) => {
                                match column {
                                    DbSlice::Ints(col) => if col[*index] > bar.to_i32() {keepers.push(*index)},
                                    DbSlice::Floats(col) => if col[*index] > bar.to_f32() {keepers.push(*index)},
                                    DbSlice::Texts(col) => if col[*index] > *bar {keepers.push(*index)},
                                }
                            },
                            Test::Starts(bar) => {
                                match column {
                                    DbSlice::Texts(col) => if col[*index].as_str().starts_with(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'starts_with' on text values".to_owned()}),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbSlice::Texts(col) => if col[*index].as_str().ends_with(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'ends_with' on text values".to_owned()}),
                                }
                            },
                            Test::Contains(bar) => {
                                match column {
                                    DbSlice::Texts(col) => if col[*index].as_str().contains(bar.as_str()) {keepers.push(*index)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'contains' on text values".to_owned()}),
                                }
                            },
                        }
                    }
                } else {
                    let mut losers = Vec::new();
                    for keeper in &keepers {
                        match &cond.test {
                            Test::Equals(bar) => {
                                match column {
                                    DbSlice::Ints(col) => if col[*keeper] == bar.to_i32() {losers.push(*keeper)},
                                    DbSlice::Floats(col) => if col[*keeper] == bar.to_f32() {losers.push(*keeper)},
                                    DbSlice::Texts(col) => if col[*keeper] == *bar {losers.push(*keeper)},
                                }
                            },
                            Test::NotEquals(bar) => {
                                match column {
                                    DbSlice::Ints(col) => if col[*keeper] != bar.to_i32() {losers.push(*keeper)},
                                    DbSlice::Floats(col) => if col[*keeper] != bar.to_f32() {losers.push(*keeper)},
                                    DbSlice::Texts(col) => if col[*keeper] != *bar {losers.push(*keeper)},
                                }
                            },
                            Test::Less(bar) => {
                                match column {
                                    DbSlice::Ints(col) => if col[*keeper] < bar.to_i32() {losers.push(*keeper)},
                                    DbSlice::Floats(col) => if col[*keeper] < bar.to_f32() {losers.push(*keeper)},
                                    DbSlice::Texts(col) => if col[*keeper] < *bar {losers.push(*keeper)},
                                }
                            },
                            Test::Greater(bar) => {
                                match column {
                                    DbSlice::Ints(col) => if col[*keeper] > bar.to_i32() {losers.push(*keeper)},
                                    DbSlice::Floats(col) => if col[*keeper] > bar.to_f32() {losers.push(*keeper)},
                                    DbSlice::Texts(col) => if col[*keeper] > *bar {losers.push(*keeper)},
                                }
                            },
                            Test::Starts(bar) => {
                                match column {
                                    DbSlice::Texts(col) => if col[*keeper].as_str().starts_with(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'starts_with' on text values".to_owned()}),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbSlice::Texts(col) => if col[*keeper].as_str().ends_with(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'ends_with' on text values".to_owned()}),
                                }
                            },
                            Test::Contains(bar) => {
                                match column {
                                    DbSlice::Texts(col) => if col[*keeper].as_str().contains(bar.as_str()) {losers.push(*keeper)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'contains' on text values".to_owned()}),
                                }
                            },
                        }
                    }
                    remove_indices(&mut keepers, &losers);
                }
            },
        }
    }

    Ok(keepers)
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