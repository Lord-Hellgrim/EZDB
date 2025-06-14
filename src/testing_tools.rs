use std::{collections::{BTreeMap, BTreeSet}, sync::atomic::AtomicU64};

use rand::{distr::StandardUniform, prelude::Distribution, Rng};

use crate::{db_structure::{ColumnTable, DbColumn, DbType, DbValue, HeaderItem, Metadata, TableKey}, ezql::{AltTest, Condition, KvQuery, OpOrCond, Operator, Query, RangeOrListOrAll, StatOp, Statistic, Test, TestOp, Update, UpdateOp}, utilities::{get_current_time, ksf, ErrorTag, EzError, KeyString}};


fn random_vec<T>(max_length: usize) -> Vec<T>  where StandardUniform: Distribution<T> {

    let mut rng = rand::rng();

    let len = rng.random_range(1..max_length);
    let mut output = Vec::new();
    for _ in 0..len {
        output.push(rng.random());
    }

    output
}

fn random_string(max_len: usize) -> String {
    let mut rng = rand::rng();

    let mut output = String::new();

    let len = rng.random_range(1..max_len);
    for _ in 0..len {
        let c: u8 = rng.random_range(65..122);
        output.push(c as char);
    }

    output
}

fn random_keystring() -> KeyString {
    let s = random_string(64);
    KeyString::from(s.as_str())
}

#[allow(unused)]
fn random_metadata() -> Metadata {
    let mut rng = rand::rng();

    let created_by = random_keystring();
    let last_access = AtomicU64::from(get_current_time());
    let times_accessed = AtomicU64::from(rng.random_range(0..200_000));
    Metadata {
        last_access,
        times_accessed,
        created_by,
    }

}


pub fn random_column_table(max_cols: usize, max_rows: usize) -> ColumnTable {
    let mut rng = rand::rng();

    let num_columns = rng.random_range(3..max_cols);
    let num_rows = rng.random_range(1..max_rows);

    let mut header = BTreeSet::new();
    for _ in 0..num_columns {
        let name = random_keystring();
        let kind: u8 = rng.random_range(0..3);
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
    let kind: u8 = rng.random_range(0..2);
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
                    col.push(rng.random());
                }
                cols.insert(name, DbColumn::Ints(col));
            },
            DbType::Float => {
                let mut col: Vec<f32> = Vec::new();
                for _ in 0..num_rows {
                    col.push(rng.random());
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

    ColumnTable {
        name,
        header,
        columns: cols,
    }

}


fn random_range_or_list_or_all() -> RangeOrListOrAll {
    let mut rng = rand::rng();
    let n = rng.random_range(0..3);
    match n {
        0 => RangeOrListOrAll::All,
        1 => RangeOrListOrAll::Range(random_keystring(), random_keystring()),
        2 => {
            let mut list = Vec::new();
            for _ in 0..rng.random_range(1..1000) {
                list.push(random_keystring());
            }
            RangeOrListOrAll::List(list)
        },
        _ => unreachable!("Range is limited")
    }
}

fn random_db_value() -> DbValue {
    let mut rng = rand::rng();

    match rng.random_range(0..3) {
        0 => DbValue::Int(rng.random()),
        1 => DbValue::Float(rng.random()),
        2 => DbValue::Text(random_keystring()),
        _ => unreachable!("Range is limited"),
    }
}

#[allow(unused)]
fn random_test() -> Test {

    let mut rng = rand::rng();

    match rng.random_range(0..5) {
        0 => Test::Contains(random_db_value()),
        1 => Test::Equals(random_db_value()),
        2 => Test::NotEquals(random_db_value()),
        3 => Test::Starts(random_db_value()),
        4 => Test::Ends(random_db_value()),
        5 => Test::Greater(random_db_value()),
        6 => Test::Less(random_db_value()),
        _ => unreachable!("Range")
    }
    
}

#[allow(unused)]
fn random_alt_test() -> AltTest {

    let mut rng = rand::rng();

    match rng.random_range(0..5) {
        0 => AltTest{op: TestOp::Contains, value: random_db_value()},
        1 => AltTest{op: TestOp::Equals, value: random_db_value()},
        2 => AltTest{op: TestOp::NotEquals, value: random_db_value()},
        3 => AltTest{op: TestOp::Starts, value: random_db_value()},
        4 => AltTest{op: TestOp::Ends, value: random_db_value()},
        5 => AltTest{op: TestOp::Greater, value: random_db_value()},
        6 => AltTest{op: TestOp::Less, value: random_db_value()},
        _ => unreachable!("Range")
    }
    
}

fn random_test_op() -> TestOp {

    let mut rng = rand::rng();

    match rng.random_range(0..7) {
        0 => TestOp::Contains,
        1 => TestOp::Equals,
        2 => TestOp::NotEquals,
        3 => TestOp::Starts,
        4 => TestOp::Ends,
        5 => TestOp::Greater,
        6 => TestOp::Less,
        _ => unreachable!("Range")
    }
    
}

fn random_conditions() -> Vec<OpOrCond> {
    let mut rng = rand::rng();

    let mut output = Vec::new();

    for i in 0..rng.random_range(0..10)*2 + 1 {
        if i % 2 == 0 {
            output.push(OpOrCond::Cond(Condition{ attribute: random_keystring(), op: random_test_op(), value: random_db_value() }));
        } else {
            match rng.random::<bool>() {
                true => output.push(OpOrCond::Op(Operator::AND)),
                false => output.push(OpOrCond::Op(Operator::OR)),
            };
        }
    }

    output
}

fn random_updates(max_length: usize) -> Vec<Update> {
    
    let mut updates = Vec::new();
    for _ in 0..rand::rng().random_range(0..max_length) {

        let attribute = random_keystring();
        let value = random_db_value();
        let operator = match rand::rng().random_range(0..6) {
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

fn random_statistics(max_length: usize, max_actions: usize) -> Vec<Statistic> {
    
    let mut updates = Vec::new();
    for _ in 0..rand::rng().random_range(0..max_length) {

        let column = random_keystring();

        let mut actions = BTreeSet::new();
        for _ in 0..rand::rng().random_range(1..max_actions) {

            let stat = match rand::rng().random_range(0..5) {
                0 => StatOp::SUM,
                1 => StatOp::MEAN,
                2 => StatOp::MEDIAN,
                3 => StatOp::MODE,
                4 => StatOp::STDEV,
                _ => unreachable!("range")
            };
            actions.insert(stat);

        }

    
        updates.push(Statistic{column, actions});
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

    let mut rng = rand::rng();
    let table_name = random_keystring();
    let right_table_name = random_keystring();
    let mut columns = Vec::new();
    for _ in 0..rng.random_range(1..30) {
        columns.push(random_keystring());
    }
    let primary_keys = random_range_or_list_or_all();
    let conditions = random_conditions();
    let match_columns = (random_keystring(), random_keystring());
    let updates = random_updates(1000);
    let alt_summaries = random_statistics(10, 3);

    let query_type = rng.random_range(0..8);
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
            Query::INSERT { table_name, inserts: random_column_table(10, 100) }
        }
        4 => {
            Query::DELETE { primary_keys, table_name, conditions }
        }
        5 => {
            Query::SUMMARY { table_name, columns: alt_summaries }
        },
        6 => {
            Query::CREATE { table: random_column_table(10, 100) }
        }
        7 => {
            Query::DROP { table_name: random_keystring() }
        }
        _ => unreachable!("range")
    }

}

pub fn random_kv_query() -> KvQuery {
    let mut rng = rand::rng();

    let query_type = rng.random_range(0..4);
    match query_type {
        0 => KvQuery::Create(random_keystring(), random_vec(100)),
        1 => KvQuery::Read(random_keystring()),
        2 => KvQuery::Update(random_keystring(), random_vec(100)),
        3 => KvQuery::Delete(random_keystring()),
        _ => panic!()
    }
}


pub fn create_fixed_table(n: usize) -> ColumnTable {
    let ints: Vec<i32> = (0..n).map(|n| n as i32).collect();
    let floats: Vec<f32> = (0..n).map(|n| n as f32).collect();
    let mut texts: Vec<KeyString> = Vec::new();
    for i in 0..n {
        texts.push(KeyString::from(format!("text{i}").as_str()));
    }

    let mut table = ColumnTable::create_empty("fixed_table", "test");
    table.add_column(ksf("ints"), DbColumn::Ints(ints)).unwrap();
    table.add_column(ksf("floats"), DbColumn::Floats(floats)).unwrap();
    table.add_column(ksf("texts"), DbColumn::Texts(texts)).unwrap();

    table
}

pub fn random_ez_error() -> EzError {
    let mut rng = rand::rng();
    let tag = match rng.random_range(0..19) {
        0 => ErrorTag::Utf8,
        1 => ErrorTag::Io,
        2 => ErrorTag::Instruction,
        3 => ErrorTag::Confirmation,
        4 => ErrorTag::Authentication,
        5 => ErrorTag::Crypto,
        6 => ErrorTag::ParseInt,
        7 => ErrorTag::ParseFloat,
        8 => ErrorTag::ParseResponse,
        9 => ErrorTag::ParseUser,
        10 => ErrorTag::OversizedData,
        11 => ErrorTag::Decompression,
        12 => ErrorTag::Query,
        13 => ErrorTag::Debug,
        14 => ErrorTag::NoMoreBufferSpace,
        15 => ErrorTag::Unimplemented,
        16 => ErrorTag::Serialization,
        17 => ErrorTag::Deserialization,
        18 => ErrorTag::Structure,
        _ => unreachable!()
    };
    let text = random_keystring().as_str().to_string();
    EzError{tag, text}
}



#[cfg(test)]
mod tests {

    use crate::{ezql::{append_conditions, conditions_from_binary}, utilities::u64_from_le_slice};

    use super::*;

    #[test]
    fn test_conditions() {
        for _ in 0..1000 {
            let conditions = random_conditions();
            let mut binary = Vec::new();
            append_conditions(&mut binary, &conditions);
            let parsed = conditions_from_binary(&binary).unwrap();
            assert_eq!(conditions, parsed);
        }
    }

    #[test]
    fn test_alt_test() {
        for _ in 0..1000 {
            let test = random_alt_test();
            let binary = test.to_binary();
            let parsed = AltTest::from_binary(&binary).unwrap();
            assert_eq!(test, parsed);
        }
    }

    #[test]
    fn test_random_column_table() {
        for _ in 0..100 {
            let table = random_column_table(10, 10);
            let binary = table.to_binary();
            let parsed = ColumnTable::from_binary(Some(table.name.as_str()), &binary).unwrap();
            assert_eq!(table, parsed);
        }
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
                    dbg!(query);
                    println!("{}", e);
                    panic!();
                },
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

    #[test]
    fn test_fixed_table() {
        let table = create_fixed_table(10);
        println!("{}", table);
    }

    #[test]
    fn test_random_kv_query() {
        for _ in 0..100 {
            let kv_query = random_kv_query();
            let bin_query = kv_query.to_binary();
            let parsed_query = KvQuery::from_binary(&bin_query).unwrap();
            assert_eq!(kv_query, parsed_query);
        }
    }

}