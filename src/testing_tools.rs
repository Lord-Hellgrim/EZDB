use std::{collections::{BTreeMap, BTreeSet}, sync::atomic::AtomicU64};

use rand::{distributions::Standard, prelude::Distribution, Rng};

use crate::{db_structure::{ColumnTable, DbColumn, DbType, HeaderItem, KeyString, Metadata, TableKey}, ezql::{AltStatistic, Condition, OpOrCond, Operator, Query, RangeOrListOrAll, StatOp, Statistic, Test, Update, UpdateOp}, utilities::get_current_time};


fn random_vec<T>(max_length: usize) -> Vec<T>  where Standard: Distribution<T> {

    let mut rng = rand::thread_rng();

    let len = rng.gen_range(1..max_length);
    let mut output = Vec::new();
    for _ in 0..len {
        output.push(rng.gen());
    }

    output
}

fn random_string(max_len: usize) -> String {
    let mut rng = rand::thread_rng();

    let mut output = String::new();

    let len = rng.gen_range(1..max_len);
    for _ in 0..len {
        let c: u8 = rng.gen_range(65..122);
        output.push(c as char);
    }

    output
}

fn random_keystring() -> KeyString {
    let s = random_string(64);
    KeyString::from(s.as_str())
}

fn random_metadata() -> Metadata {
    let mut rng = rand::thread_rng();

    let created_by = random_keystring();
    let last_access = AtomicU64::from(get_current_time());
    let times_accessed = AtomicU64::from(rng.gen_range(0..200_000));
    Metadata {
        last_access,
        times_accessed,
        created_by,
    }

}


pub fn random_column_table() -> ColumnTable {
    let mut rng = rand::thread_rng();

    let num_columns = rng.gen_range(3..30);
    let num_rows = rng.gen_range(1..10000);

    let mut header = BTreeSet::new();
    for _ in 0..num_columns {
        let name = random_keystring();
        let kind: u8 = rng.gen_range(0..3);
        let kind = match kind {
            0 => DbType::Int,
            1 => DbType::Text,
            2 => DbType::Float,
            _ => unreachable!("Kind is a range from [0, 3)")
        };
        let key = TableKey::None;
        header.insert(HeaderItem{name, kind, key});
    }
    let name = random_keystring();
    let kind: u8 = rng.gen_range(0..2);
    let kind = match kind {
        0 => DbType::Int,
        1 => DbType::Text,
        _ => unreachable!("Kind is a range from [0, 3)")
    };
    let key = TableKey::Primary;
    header.insert(HeaderItem{name, kind, key});

    let mut cols = BTreeMap::new();

    for item in &header {
        
        let name = item.name;
        match item.kind {
            DbType::Int => {
                let mut col: Vec<i32> = Vec::new();
                for _ in 0..num_rows {
                    col.push(rng.gen());
                }
                cols.insert(name, DbColumn::Ints(col));
            },
            DbType::Float => {
                let mut col: Vec<f32> = Vec::new();
                for _ in 0..num_rows {
                    col.push(rng.gen());
                }
                cols.insert(name, DbColumn::Floats(col));
            },
            DbType::Text => {
                let mut col: Vec<KeyString> = Vec::new();
                for _ in 0..num_rows {
                    col.push(random_keystring());
                }
                cols.insert(name, DbColumn::Texts(col));
            },
        }
    }

    let metadata = random_metadata();

    ColumnTable {
        metadata,
        name,
        header,
        columns: cols,
    }

}


fn random_range_or_list_or_all() -> RangeOrListOrAll {
    let mut rng = rand::thread_rng();
    let n = rng.gen_range(0..3);
    match n {
        0 => RangeOrListOrAll::All,
        1 => RangeOrListOrAll::Range(random_keystring(), random_keystring()),
        2 => {
            let mut list = Vec::new();
            for _ in 0..rng.gen_range(1..1000) {
                list.push(random_keystring());
            }
            RangeOrListOrAll::List(list)
        },
        _ => unreachable!("Range is limited")
    }
}

fn random_test() -> Test {

    let mut rng = rand::thread_rng();

    match rng.gen_range(0..5) {
        0 => Test::Contains(random_keystring()),
        1 => Test::Equals(random_keystring()),
        2 => Test::NotEquals(random_keystring()),
        3 => Test::Starts(random_keystring()),
        4 => Test::Ends(random_keystring()),
        5 => Test::Greater(random_keystring()),
        6 => Test::Less(random_keystring()),
        _ => unreachable!("Range")
    }
    
}

fn random_conditions() -> Vec<OpOrCond> {
    let mut rng = rand::thread_rng();

    let mut output = Vec::new();

    for i in 0..rng.gen_range(0..10)*2 + 1 {
        if i % 2 == 0 {
            output.push(OpOrCond::Cond(Condition{ attribute: random_keystring(), test: random_test() }));
        } else {
            match rng.gen::<bool>() {
                true => output.push(OpOrCond::Op(Operator::AND)),
                false => output.push(OpOrCond::Op(Operator::OR)),
            };
        }
    }

    output
}

fn random_updates(max_length: usize) -> Vec<Update> {
    
    let mut updates = Vec::new();
    for _ in 0..rand::thread_rng().gen_range(0..max_length) {

        let attribute = random_keystring();
        let value = random_keystring();
        let operator = match rand::thread_rng().gen_range(0..6) {
            0 => UpdateOp::Append,
            1 => UpdateOp::Assign,
            2 => UpdateOp::MinusEquals,
            3 => UpdateOp::PlusEquals,
            4 => UpdateOp::Prepend,
            5 => UpdateOp::TimesEquals,
            _ => unreachable!("range")
        };
    
        updates.push(Update { attribute, operator, value });
    }

    updates

}

fn random_statistics(max_length: usize) -> Vec<Statistic> {
    
    let mut updates = Vec::new();
    for _ in 0..rand::thread_rng().gen_range(0..max_length) {

        let stat = match rand::thread_rng().gen_range(0..5) {
            0 => Statistic::SUM(random_keystring()),
            1 => Statistic::MEAN(random_keystring()),
            2 => Statistic::MEDIAN(random_keystring()),
            3 => Statistic::MODE(random_keystring()),
            4 => Statistic::STDEV(random_keystring()),
            _ => unreachable!("range")
        };
    
        updates.push(stat);
    }

    updates

}

fn random_alt_statistics(max_length: usize, max_actions: usize) -> Vec<AltStatistic> {
    
    let mut updates = Vec::new();
    for _ in 0..rand::thread_rng().gen_range(0..max_length) {

        let column = random_keystring();

        let mut actions = BTreeSet::new();
        for _ in 0..rand::thread_rng().gen_range(1..max_actions) {

            let stat = match rand::thread_rng().gen_range(0..5) {
                0 => StatOp::SUM,
                1 => StatOp::MEAN,
                2 => StatOp::MEDIAN,
                3 => StatOp::MODE,
                4 => StatOp::STDEV,
                _ => unreachable!("range")
            };
            actions.insert(stat);

        }

    
        updates.push(AltStatistic{column, actions});
    }

    updates

}

// pub enum Query {
//     SELECT{table_name: KeyString, primary_keys: RangeOrListOrAll, columns: Vec<KeyString>, conditions: Vec<OpOrCond>},
//     LEFT_JOIN{left_table_name: KeyString, right_table_name: KeyString, match_columns: (KeyString, KeyString), primary_keys: RangeOrListOrAll},
//     INNER_JOIN,
//     RIGHT_JOIN,
//     FULL_JOIN,
//     UPDATE{table_name: KeyString, primary_keys: RangeOrListOrAll, conditions: Vec<OpOrCond>, updates: Vec<Update>},
//     INSERT{table_name: KeyString, inserts: ColumnTable},
//     DELETE{primary_keys: RangeOrListOrAll, table_name: KeyString, conditions: Vec<OpOrCond>},
//     SUMMARY{table_name: KeyString, columns: Vec<Statistic>},
// }

pub fn random_query() -> Query {

    let mut rng = rand::thread_rng();
    let table_name = random_keystring();
    let right_table_name = random_keystring();
    let mut columns = Vec::new();
    for _ in 0..rng.gen_range(1..30) {
        columns.push(random_keystring());
    }
    let primary_keys = random_range_or_list_or_all();
    let conditions = random_conditions();
    let match_columns = (random_keystring(), random_keystring());
    let updates = random_updates(1000);
    let summaries = random_statistics(10);
    let alt_summaries = random_alt_statistics(10, 3);

    let query_type = rng.gen_range(0..7);
    match query_type {
        0 => {
            Query::SELECT{ table_name, primary_keys, columns, conditions }
        }
        1 => {
            Query::LEFT_JOIN { left_table_name: table_name, right_table_name, match_columns, primary_keys }
        }
        2 => {
            Query::UPDATE { table_name, primary_keys, conditions, updates }
        }
        3 => {
            Query::INSERT { table_name, inserts: random_column_table() }
        }
        4 => {
            Query::DELETE { primary_keys, table_name, conditions }
        }
        5 => {
            Query::SUMMARY { table_name, columns: summaries }
        }
        6 => {
            Query::ALTSUMMARY { table_name, columns: alt_summaries }
        }
        _ => unreachable!("range")
    }

}



#[cfg(test)]
mod tests {

    use crate::utilities::u64_from_le_slice;

    use super::*;

    #[test]
    fn test_random_column_table() {
        let table = random_column_table();
        println!("{}", table);
    }


    #[test]
    fn test_random_string() {
        for _ in 0..100 {
            let s = random_string(64);
            println!("{}", s);
        }
    }

    #[test]
    fn test_random_query() {
        for _ in 0..1000 {
            let query = random_query();
            let binary_query = query.to_binary();
            let parsed_query = match Query::from_binary(&binary_query) {
                Ok(x) => x,
                Err(e) => {
                    println!("Query:\n#################################################\n\n{}", query);
                    Query::from_binary(&binary_query).unwrap();
                    continue
                }
            };
            assert_eq!(query, parsed_query);
        }
    }

    #[test]
    fn test_query_binary_length() {
        for i in 0..1000 {
            println!("{}", i);
            let query = random_query();
            let binary_query = query.to_binary();
            let len = u64_from_le_slice(&binary_query[24..32]) as usize;
            if binary_query.len() != len {
                println!("#####################\n\nlen: {}  -  bin_len: {}\n\n", len, binary_query.len());
                println!("{}", query);
                panic!()
            }
        }

    }

}