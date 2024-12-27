use std::{collections::{BTreeSet, HashMap, HashSet}, fmt::Display, str::FromStr, sync::Arc};

use crate::{db_structure::{remove_indices, table_from_inserts, ColumnTable, DbColumn, DbValue, Metadata, Value}, server_networking::Database, utilities::{i32_from_le_slice, ksf, mean_f32_slice, mean_i32_slice, median_f32_slice, median_i32_slice, mode_i32_slice, mode_string_slice, print_sep_list, stdev_f32_slice, stdev_i32_slice, sum_f32_slice, sum_i32_slice, u64_from_le_slice, usize_from_le_slice, ErrorTag, EzError, KeyString}};

use crate::PATH_SEP;


#[derive(Clone, Debug, PartialEq, PartialOrd, Default)]
pub struct Join {
    pub table: KeyString,
    pub join_column: (KeyString, KeyString),
}

// #[derive(Clone, Debug, PartialEq, PartialOrd, Default, Eq, Ord)]
// pub struct Inserts {
//     pub value_columns: Vec<KeyString>,
//     pub new_values: String,
// }

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct Statistic{
    pub column: KeyString,
    pub actions: BTreeSet<StatOp>,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum StatOp {
    SUM,
    MEAN,
    MEDIAN,
    MODE,
    STDEV,
}

impl Display for StatOp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatOp::SUM => write!(f, "SUM"),
            StatOp::MEAN => write!(f, "MEAN"),
            StatOp::MEDIAN => write!(f, "MEDIAN"),
            StatOp::MODE => write!(f, "MODE"),
            StatOp::STDEV => write!(f, "STDEV"),
        }
    }
}

pub fn statistics_to_binary(statistics: &[Statistic]) -> Vec<u8> {
    let mut stats = Vec::new();
    for item in statistics {
        stats.extend_from_slice(item.column.raw());
        stats.push(item.actions.len() as u8);
        for stat in &item.actions {
            match stat {
                StatOp::SUM => stats.push(0),
                StatOp::MEAN => stats.push(1),
                StatOp::MEDIAN => stats.push(2),
                StatOp::MODE => stats.push(3),
                StatOp::STDEV => stats.push(4),
            }
        }
    }
    
    stats
}

pub fn append_statistics(binary: &mut Vec<u8>, statistics: &[Statistic]) -> u64 {
    let mut i: u64 = 0;
    for item in statistics {
        binary.extend_from_slice(item.column.raw());
        i += 64;
        binary.push(item.actions.len() as u8);
        i += 1;
        for stat in &item.actions {
            i += 1;
            match stat {
                StatOp::SUM => binary.push(0),
                StatOp::MEAN => binary.push(1),
                StatOp::MEDIAN => binary.push(2),
                StatOp::MODE => binary.push(3),
                StatOp::STDEV => binary.push(4),
            }
        }
    }
    i
}


pub fn statistics_from_binary(binary: &[u8]) -> Result<Vec<Statistic>, EzError> {
    let mut stats = Vec::new();

    let mut i = 0;
    while i < binary.len() {
        let column = KeyString::try_from(&binary[i..i+64])?;
        i += 64;
        let len = binary[i];
        i += 1;
        let mut actions = BTreeSet::new();
        for j in 0..len as usize {
            let action = match binary[i+j] {
                0 => StatOp::SUM,
                1 => StatOp::MEAN,
                2 => StatOp::MEDIAN,
                3 => StatOp::MODE,
                4 => StatOp::STDEV,
                other => return Err(EzError{tag: ErrorTag::Query, text: format!("Unparseable stat op: '{}'", other)}),
            };
            actions.insert(action);
        }

        stats.push(Statistic{column, actions});

        i += len as usize;
    }

    Ok(stats)

}


#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum KvQuery {
    Create(KeyString, Vec<u8>),
    Read(KeyString),
    Update(KeyString, Vec<u8>),
    Delete(KeyString),
}

impl Display for KvQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            KvQuery::Create(key_string, vec) => write!(f, "Create: '{}':\n{:x?}", key_string, vec),
            KvQuery::Read(key_string) => write!(f, "Read: '{}'", key_string),
            KvQuery::Update(key_string, vec) => write!(f, "Update: '{}':\n{:x?}", key_string, vec),
            KvQuery::Delete(key_string) => write!(f, "Delete: '{}'", key_string),
        }
    }
}

impl KvQuery {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut binary = Vec::new();
        match self {
            KvQuery::Create(key_string, vec) => {
                binary.extend_from_slice(ksf("CREATE").raw());
                binary.extend_from_slice(key_string.raw());
                binary.extend_from_slice(&vec.len().to_le_bytes());
                binary.extend_from_slice(vec);
            },
            KvQuery::Read(key_string) => {
                binary.extend_from_slice(ksf("READ").raw());
                binary.extend_from_slice(key_string.raw());
            },
            KvQuery::Update(key_string, vec) => {
                binary.extend_from_slice(ksf("UPDATE").raw());
                binary.extend_from_slice(key_string.raw());
                binary.extend_from_slice(&vec.len().to_le_bytes());
                binary.extend_from_slice(vec);
            },
            KvQuery::Delete(key_string) => {
                binary.extend_from_slice(ksf("DELETE").raw());
                binary.extend_from_slice(key_string.raw());
            },
        };

        binary
    }

    pub fn from_binary(binary: &[u8]) -> Result<KvQuery, EzError> {
        if binary.len() < 128 {
            return Err(EzError{tag: ErrorTag::Query, text: "KV query needs to be at least 128 bytes (type and key)".to_owned()})
        }

        let kind = KeyString::try_from(&binary[0..64])?;
        let key = KeyString::try_from(&binary[64..128])?;
        match kind.as_str() {
            "CREATE" => {
                let len = usize_from_le_slice(&binary[128..136]);
                let mut value = Vec::with_capacity(len);
                value.extend_from_slice(&binary[136..136+len]);
                Ok(KvQuery::Create(key, value))
            }
            "READ" => {
                Ok(KvQuery::Read(key))
            }
            "UPDATE" => {
                let len = usize_from_le_slice(&binary[128..136]);
                let mut value = Vec::with_capacity(len);
                value.extend_from_slice(&binary[136..136+len]);
                Ok(KvQuery::Update(key, value))
            }
            "DELETE" => {
                Ok(KvQuery::Delete(key))
            }
            other => Err(EzError{tag: ErrorTag::Deserialization, text: format!("Unsupported KvQuery type '{}'", other)})
        }
    }
}

pub fn parse_kv_queries_from_binary(binary: &[u8]) -> Result<Vec<KvQuery>, EzError> {
    if binary.len() < 128 {
        return Err(EzError{tag: ErrorTag::Query, text: "Binary is too short. Cannot be a valid KvQuery".to_owned()})
    }
    let mut queries = Vec::new();
    let mut counter = 0;
    while counter < binary.len() {
        let query = KvQuery::from_binary(&binary[counter..])?;
        match &query {
            KvQuery::Create(_, vec) => counter += 128 + 8 + vec.len(),
            KvQuery::Read(_) => counter += 128,
            KvQuery::Update(_, vec) => counter += 128 + 8 + vec.len(),
            KvQuery::Delete(_) => counter += 128,
        };
        queries.push(query);
    }

    Ok(queries)
}


//  - INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
//  - SELECT(table_name: products, primary_keys: *, columns: (price, stock), conditions: ((price greater-than 500) AND (stock less-than 1000)))
//  - UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))
//  - DELETE(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))
//  - SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))
//  - LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)


/// A database query that has already been parsed from EZQL (see EZQL.txt)
#[derive(Clone, Debug, PartialEq, PartialOrd)]
#[allow(non_camel_case_types)]
pub enum Query {
    CREATE{table: ColumnTable},
    DROP{table_name: KeyString},
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

impl Display for Query {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // println!("calling: Query::fmt()");

        let mut printer = String::new();
        match self {
            Query::SELECT { table_name, primary_keys, columns, conditions } => {
                printer.push_str(&format!("SELECT(table_name: {}, primary_keys: {}, columns: {}, conditions: ({}))",
                        table_name,
                        primary_keys,
                        print_sep_list(columns, ", "),
                        print_sep_list(conditions, " "),
                ));

            },
            Query::LEFT_JOIN { left_table_name: left_table, right_table_name: right_table, match_columns, primary_keys } => {
                printer.push_str(&format!("LEFT_JOIN(left_table: {}, right_table: {}, primary_keys: {}, match_columns: ({}, {}))",
                        left_table,
                        right_table,
                        primary_keys,
                        match_columns.0,
                        match_columns.1,
                ));
            },
            Query::UPDATE{ table_name, primary_keys, conditions, updates } => {
                printer.push_str(&format!("UPDATE(table_name: {}, primary_keys: {}, conditions: ({}), updates: ({}))",
                        table_name,
                        primary_keys,
                        print_sep_list(conditions, " "),
                        print_sep_list(updates, ", "),
                ));
            },
            Query::INSERT{ table_name, inserts } => {

                let new_values = inserts.to_string();
                let mut temp = String::from("");
                for line in new_values.lines() {
                    temp.push_str(&format!("({line}), "));
                }
                temp.pop();
                temp.pop();
                
                let value_columns = inserts.header.iter().map(|n| n.name).collect::<Vec<KeyString>>();
                printer.push_str(&format!("INSERT(table_name: {}, value_columns: ({}), new_values: ({}))",
                        table_name,
                        print_sep_list(&value_columns, ", "),
                        temp,
                ));
            },
            Query::DELETE { primary_keys, table_name, conditions } => {
                printer.push_str(&format!("DELETE(table_name: {}, primary_keys: {}, conditions: ({}))",
                        table_name,
                        primary_keys,
                        print_sep_list(conditions, " "),
                ));
            },
            Query::SUMMARY { table_name, columns } => {
                printer.push_str(&format!("SUMMARY(table_name: {}, stats: (",table_name));
                for column in columns {
                    printer.push_str(column.column.as_str());
                    printer.push_str(" -> ");
                    for action in &column.actions {
                        printer.push_str(&format!("{}, ", action));
                    }
                    printer.push(')');
                }
            },
            Query::CREATE { table } => printer.push_str(&format!("CREATE(table_name: {}", table.name)),
            Query::DROP { table_name } => printer.push_str(&format!("DROP(table_name: {}", table_name)),
            Query::INNER_JOIN => todo!(),
            Query::RIGHT_JOIN => todo!(),
            Query::FULL_JOIN => todo!(),
        }


        write!(f, "{}", printer)
    }

}

impl Default for Query {
    fn default() -> Self {
        Self::new()
    }
}

impl Query {
    pub fn new() -> Self {
        // println!("calling: Query::new()");

        Query::SELECT {
            table_name: KeyString::from("__RESULT__"),
            primary_keys: RangeOrListOrAll::All,
            columns: Vec::new(),
            conditions: Vec::new(),
        }
    }

    pub fn blank(keyword: &str) -> Result<Query, EzError> {
        // println!("calling: Query::blank()");

        match keyword {
            "INSERT" => Ok(Query::INSERT{ table_name: KeyString::new(), inserts: ColumnTable::blank(&BTreeSet::new(), KeyString::new(), "blank") }),
            "SELECT" => Ok(Query::SELECT{ table_name: KeyString::new(), primary_keys: RangeOrListOrAll::All, columns: Vec::new(), conditions: Vec::new()  }),
            "UPDATE" => Ok(Query::UPDATE{ table_name: KeyString::new(), primary_keys: RangeOrListOrAll::All, conditions: Vec::new(), updates: Vec::new() }),
            "DELETE" => Ok(Query::DELETE{ table_name: KeyString::new(), primary_keys: RangeOrListOrAll::All, conditions: Vec::new() }),
            "LEFT_JOIN" => Ok(Query::LEFT_JOIN{ left_table_name: KeyString::new(), right_table_name: KeyString::new(), match_columns: (KeyString::new(), KeyString::new()), primary_keys: RangeOrListOrAll::All }),
            "FULL_JOIN" => Ok(Query::FULL_JOIN),
            "INNER_JOIN" => Ok(Query::INNER_JOIN),
            "SUMMARY" => Ok(Query::SUMMARY{ table_name: KeyString::new(), columns: Vec::new() }),
            _ => return Err(EzError{tag: ErrorTag::Query, text: format!("Query type: '{}' is not supported", keyword)}),
        }
    }

    pub fn get_primary_keys_ref(&self) -> Option<&RangeOrListOrAll> {
        // println!("calling: Query::get_primary_keys_ref()");

        match self {
            Query::SELECT { table_name: _, primary_keys, columns: _, conditions: _ } => Some(primary_keys),
            Query::LEFT_JOIN { left_table_name: _, right_table_name: _, match_columns: _, primary_keys } => Some(primary_keys),
            Query::UPDATE { table_name: _, primary_keys, conditions: _, updates: _ } => Some(primary_keys),
            Query::DELETE { primary_keys, table_name: _, conditions: _ } => Some(primary_keys),
            _ => None
        }
    }

    pub fn get_table_name(&self) -> KeyString {
        // println!("calling: Query::get_table_name()");

        match self {
            Query::SELECT { table_name, primary_keys: _, columns: _, conditions: _ } => *table_name,
            Query::LEFT_JOIN { left_table_name, right_table_name: _, match_columns: _, primary_keys: _ } => *left_table_name,
            Query::UPDATE { table_name, primary_keys: _, conditions: _, updates: _ } => *table_name,
            Query::INSERT { table_name, inserts: _ } => *table_name,
            Query::DELETE { primary_keys: _, table_name, conditions: _ } => *table_name,
            Query::SUMMARY { table_name, columns: _ } => *table_name,
            Query::INNER_JOIN => todo!(),
            Query::RIGHT_JOIN => todo!(),
            Query::FULL_JOIN => todo!(),
            Query::CREATE { table } => table.name,
            Query::DROP { table_name } => *table_name,
        }
    }


    pub fn to_binary(&self) -> Vec<u8> {
        let mut binary = Vec::with_capacity(1024);
        let mut handles = [0u8;32];
        match self {
            Query::SELECT { table_name, primary_keys, columns, conditions } => {
                let binary_primary_keys = primary_keys.to_binary();
                let binary_columns = columns.iter().map(|n| n.raw().to_vec()).flatten().collect::<Vec<u8>>();
                let mut binary_conditions = Vec::new();
                for condition in conditions {
                    binary_conditions.extend_from_slice(&condition.to_binary());
                }
                // let binary_conditions = conditions.iter().map(|n| n.to_binary()).flatten().collect::<Vec<u8>>();
                handles[0..8].copy_from_slice(&binary_primary_keys.len().to_le_bytes());
                handles[8..16].copy_from_slice(&binary_columns.len().to_le_bytes());
                handles[16..24].copy_from_slice(&binary_conditions.len().to_le_bytes());
                binary.extend_from_slice(&handles);
                binary.extend_from_slice(KeyString::from("SELECT").raw());
                binary.extend_from_slice(table_name.raw());
                binary.extend_from_slice(&binary_primary_keys);
                binary.extend_from_slice(&binary_columns);
                binary.extend_from_slice(&binary_conditions);
                let len = &binary.len().to_le_bytes();
                binary[24..32].copy_from_slice(len);
            },
            Query::LEFT_JOIN { left_table_name, right_table_name, match_columns, primary_keys } => {
                let binary_primary_keys = primary_keys.to_binary();
                handles[0..8].copy_from_slice(&binary_primary_keys.len().to_le_bytes());
                binary.extend_from_slice(&handles);
                binary.extend_from_slice(KeyString::from("LEFT_JOIN").raw());
                binary.extend_from_slice(left_table_name.raw());
                binary.extend_from_slice(right_table_name.raw());
                binary.extend_from_slice(match_columns.0.raw());
                binary.extend_from_slice(match_columns.1.raw());
                binary.extend_from_slice(&binary_primary_keys);
                let len = &binary.len().to_le_bytes();
                binary[24..32].copy_from_slice(len);

            },
            Query::INNER_JOIN => todo!(),
            Query::RIGHT_JOIN => todo!(),
            Query::FULL_JOIN => todo!(),
            Query::UPDATE { table_name, primary_keys, conditions, updates } => {
                let binary_primary_keys = primary_keys.to_binary();
                let binary_updates = updates_to_binary(updates);
                let binary_conditions = conditions.iter().map(|n| n.to_binary()).flatten().collect::<Vec<u8>>();
                handles[0..8].copy_from_slice(&binary_primary_keys.len().to_le_bytes());
                handles[8..16].copy_from_slice(&binary_conditions.len().to_le_bytes());
                handles[16..24].copy_from_slice(&binary_updates.len().to_le_bytes());
                binary.extend_from_slice(&handles);
                binary.extend_from_slice(KeyString::from("UPDATE").raw());
                binary.extend_from_slice(table_name.raw());
                binary.extend_from_slice(&binary_primary_keys);
                binary.extend_from_slice(&binary_conditions);
                binary.extend_from_slice(&binary_updates);
                let len = &binary.len().to_le_bytes();
                binary[24..32].copy_from_slice(len);
            },
            Query::INSERT { table_name, inserts } => {
                let table = inserts.to_binary();
                handles[0..8].copy_from_slice(&table.len().to_le_bytes());
                binary.extend_from_slice(&handles);
                binary.extend_from_slice(KeyString::from("INSERT").raw());
                binary.extend_from_slice(table_name.raw());
                binary.extend_from_slice(&table);
                let len = &binary.len().to_le_bytes();
                binary[24..32].copy_from_slice(len);

            },
            Query::DELETE { primary_keys, table_name, conditions } => {
                let binary_primary_keys = primary_keys.to_binary();
                let binary_conditions = conditions.iter().map(|n| n.to_binary()).flatten().collect::<Vec<u8>>();
                handles[0..8].copy_from_slice(&binary_primary_keys.len().to_le_bytes());
                handles[8..16].copy_from_slice(&binary_conditions.len().to_le_bytes());
                binary.extend_from_slice(&handles);
                binary.extend_from_slice(KeyString::from("DELETE").raw());
                binary.extend_from_slice(table_name.raw());
                binary.extend_from_slice(&binary_primary_keys);
                binary.extend_from_slice(&binary_conditions);
                let len = &binary.len().to_le_bytes();
                binary[24..32].copy_from_slice(len);

            },
            Query::SUMMARY { table_name, columns } => {
                let stats = statistics_to_binary(columns);
                handles[0..8].copy_from_slice(&stats.len().to_le_bytes());
                binary.extend_from_slice(&handles);
                binary.extend_from_slice(KeyString::from("SUMMARY").raw());
                binary.extend_from_slice(table_name.raw());
                binary.extend_from_slice(&stats);
                let len = &binary.len().to_le_bytes();
                binary[24..32].copy_from_slice(len);
                
            },
            Query::CREATE { table } => {
                let table_name = table.name;
                let table = table.to_binary();
                handles[0..8].copy_from_slice(&table.len().to_le_bytes());
                binary.extend_from_slice(&handles);
                binary.extend_from_slice(KeyString::from("CREATE").raw());
                binary.extend_from_slice(table_name.raw());
                binary.extend_from_slice(&table);
                let len = &binary.len().to_le_bytes();
                binary[24..32].copy_from_slice(len);
            },
            Query::DROP { table_name } => {
                let table_name = table_name;
                binary.extend_from_slice(&handles);
                binary.extend_from_slice(KeyString::from("DROP").raw());
                binary.extend_from_slice(table_name.raw());
                let len = &binary.len().to_le_bytes();
                binary[24..32].copy_from_slice(len);
            },
        }
        binary
    }

    pub fn from_binary(binary: &[u8]) -> Result<Query, EzError> {
        if binary.len() < 160 { // TODO: Check actual minimum
            return Err(EzError{tag: ErrorTag::Deserialization, text: "Binary is smaller than minimum valid binary".to_owned()})
        }
        let handles = &binary[0..32];
        let body = &binary[32..];
        let query_type = KeyString::try_from(&body[0..64]).unwrap();
        let table_name = KeyString::try_from(&body[64..128]).unwrap();
        match query_type.as_str() {
            "INSERT" => {
                let inserts_len = u64_from_le_slice(&handles[0..8]) as usize;
                let inserts = ColumnTable::from_binary(Some("inserts"), &body[128..128+inserts_len])?;
                Ok( Query::INSERT { table_name, inserts })
            },
            "SELECT" => {
                let pk_length = u64_from_le_slice(&handles[0..8]) as usize;
                let cols_length = u64_from_le_slice(&handles[8..16]) as usize;
                let conds_length = u64_from_le_slice(&handles[16..24]) as usize;
                let primary_keys = RangeOrListOrAll::from_binary(&body[128..128+pk_length]).unwrap();
                let mut columns = Vec::new();
                for chunk in body[128+pk_length..128+pk_length+cols_length].chunks(64) {
                    columns.push(KeyString::try_from(chunk).unwrap());
                }
                let conditions = conditions_from_binary(&body[128+pk_length+cols_length..128+pk_length+cols_length+conds_length]).unwrap();

                Ok(Query::SELECT { table_name, primary_keys, columns, conditions })

            },
            "UPDATE" => {
                let pk_length = u64_from_le_slice(&handles[0..8]) as usize;
                let conds_length = u64_from_le_slice(&handles[8..16]) as usize;
                let updates_len = u64_from_le_slice(&handles[16..24]) as usize;
                let primary_keys = RangeOrListOrAll::from_binary(&body[128..128+pk_length])?;
                let conditions = conditions_from_binary(&body[128+pk_length..128+pk_length+conds_length])?;
                let updates = updates_from_binary(&body[128+pk_length+conds_length..128+pk_length+conds_length+updates_len])?;
                Ok( Query::UPDATE { table_name, primary_keys, conditions, updates } )
            },
            "DELETE" => {
                
                let pk_length = u64_from_le_slice(&handles[0..8]) as usize;
                let conds_length = u64_from_le_slice(&handles[8..16]) as usize;
                let primary_keys = RangeOrListOrAll::from_binary(&body[128..128+pk_length]).unwrap();
                let conditions = conditions_from_binary(&body[128+pk_length..128+pk_length+conds_length]).unwrap();

                Ok(Query::DELETE { table_name, primary_keys, conditions })
            },
            "LEFT_JOIN" => {
                
                let pk_len = u64_from_le_slice(&handles[0..8]) as usize;
                let right_table_name = KeyString::try_from(&body[128..192])?;
                let match1 = KeyString::try_from(&body[192..256])?;
                let match2 = KeyString::try_from(&body[256..320])?;
                let match_columns = (match1, match2);
                let primary_keys = RangeOrListOrAll::from_binary(&body[320..320+pk_len])?;
                
                Ok( Query::LEFT_JOIN { left_table_name: table_name, right_table_name, match_columns, primary_keys } )
            },
            "FULL_JOIN" => {
                todo!()
            },
            "INNER_JOIN" => {
                todo!()
            },
            "SUMMARY" => {
                let stat_len = u64_from_le_slice(&handles[0..8]) as usize;
                let columns = statistics_from_binary(&body[128..128+stat_len])?;

                Ok( Query::SUMMARY { table_name, columns } )

            },
            "CREATE" => {
                let table_len = u64_from_le_slice(&handles[0..8]) as usize;
                let table = ColumnTable::from_binary(None, &body[128..128+table_len])?;
                Ok( Query::CREATE { table })
            },
            "DROP" => {
                Ok( Query::DROP { table_name })
            }
            _ => return Err(EzError{tag: ErrorTag::Query, text: format!("Query type '{}' is not supported", query_type)}),
        }

    }
}

pub fn parse_queries_from_binary(binary: &[u8]) -> Result<Vec<Query>, EzError> {
    if binary.len() < 160 {
        return Err(EzError{tag: ErrorTag::Query, text: "Binary is too short. Cannot be a valid query".to_owned()})
    }
    let mut queries = Vec::new();
    let mut counter = 0;
    while counter < binary.len() {
        let len = u64_from_le_slice(&binary[counter+24..counter+32]) as usize;
        let block = &binary[counter..counter + len];
        let query = Query::from_binary(block)?;
        queries.push(query);
        counter += len;
    }

    Ok(queries)
}

pub fn queries_to_binary(queries: &[Query]) -> Vec<u8> {
    let mut binary = Vec::new();
    for query in queries {
        binary.extend_from_slice(&query.to_binary());
    }

    binary
}

pub fn append_primary_keys(binary: &mut Vec<u8>, primary_keys: &RangeOrListOrAll) -> u64{
    let mut i = 0;
    match primary_keys {
        RangeOrListOrAll::Range(from, to) => {
            binary.extend_from_slice(KeyString::from("RANGE").raw());
            binary.extend_from_slice(from.raw());
            binary.extend_from_slice(to.raw());
            i = 192;
        },
        RangeOrListOrAll::List(vec) => {
            binary.extend_from_slice(KeyString::from("LIST").raw());
            binary.extend_from_slice(&vec.len().to_le_bytes());
            i += 72;
            for s in vec {
                binary.extend_from_slice(s.raw());
                i += 64;
            }

        },
        RangeOrListOrAll::All => {
            binary.extend_from_slice(KeyString::from("ALL").raw());
            i = 64
        },
    };
    i
}

pub fn append_conditions(binary: &mut Vec<u8>, conditions: &Vec<OpOrCond>) -> u64{
    let mut i: u64 = 0;
    for condition in conditions {
        match condition {
            OpOrCond::Cond(condition) => {
                i += 200;
                binary.extend_from_slice(condition.attribute.raw());
                binary.extend_from_slice(&condition.test.to_binary());
            },
            OpOrCond::Op(operator) => {
                i+= 64;
                binary.extend_from_slice(operator.to_keystring().raw());
            },
        }
    }

    i

}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Update {
    pub attribute: KeyString,
    pub operator: UpdateOp,
    pub value: DbValue,
}

impl Display for Update {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // println!("calling: Update::fmt()");

        let op = match self.operator {
            UpdateOp::Assign => "=",
            UpdateOp::PlusEquals => "+=",
            UpdateOp::MinusEquals => "-=",
            UpdateOp::TimesEquals => "*=",
            UpdateOp::Append => "append",
            UpdateOp::Prepend => "prepend",
        };
        write!(f, "({} {} {})", self.attribute.as_str(), op, self.value.to_string())
    }
}

impl FromStr for Update {
    type Err = EzError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // println!("calling: Update::from_str()");

        let output: Update;
        let mut t = s.split_whitespace();
        if s.split_whitespace().count() < 3 {
            return Err(EzError{tag: ErrorTag::Query, text: "Not enough values for an Update".to_owned()})
        }
        if s.split_whitespace().count() == 3 {
            output = Update {
                attribute: KeyString::from(t.next().unwrap()),
                operator: UpdateOp::from_str(t.next().unwrap())?,
                value: DbValue::Text(KeyString::from(t.next().unwrap())),
            };
        } else {
            let mut acc = Vec::new();
            let mut buf = String::new();
            let mut inside = false;
            for c in s.chars() {
                if acc.len() > 3 {break;}
                // println!("buf: {}", buf);
                if c.is_whitespace() {
                    if inside {
                        buf.push(c);
                        continue;
                    } else {
                        acc.push(buf.clone());
                        buf.clear();
                        // println!("acc: {:?}", acc);
                        continue;
                    }
                } else if c == '"' {
                    inside ^= true;
                    continue;
                } else {
                    buf.push(c);
                }
            }
            acc.push(buf);

            if acc.len() == 3 {
                output = Update {
                    attribute: KeyString::from(acc[0].as_str()),
                    operator: UpdateOp::from_str(acc[1].as_str())?,
                    value: DbValue::Text(KeyString::from(acc[2].as_str())),
                };
            } else {
                return Err(EzError{tag: ErrorTag::Query, text: format!("Update: '{}' could not be parsed from string", ksf(s))})
            }
        }

        Ok(output)
    }
}

impl Update {

    pub fn blank() -> Self{
        // println!("calling: Update::blank()");

        Update {
            attribute: KeyString::new(),
            operator: UpdateOp::Assign,
            value: DbValue::Text(KeyString::new()),
        }
    }

    pub fn to_binary(&self) -> [u8;144] {
        let mut binary = [0u8;144];
        binary[0..64].copy_from_slice(self.attribute.raw());
        binary[64..72].copy_from_slice(&self.operator.to_binary());
        binary[72..144].copy_from_slice(&self.value.to_binary());
        binary
    }

    pub fn from_binary(binary: &[u8]) -> Result<Update, EzError> {
        if binary.len() != 144 {
            return Err(EzError { tag: ErrorTag::Deserialization, text: format!("Update binaries are exactly 144 bytes") })
        }
        let attribute = KeyString::try_from(&binary[0..64])?;
        let operator = UpdateOp::from_binary(&binary[64..72])?;
        let value = DbValue::from_binary(&binary[72..144])?;
        Ok(Update { attribute, operator, value })
    }
}

pub fn updates_to_binary(updates: &[Update]) -> Vec<u8> {
    let mut binary = Vec::new();

    for update in updates {
        binary.extend_from_slice(&update.to_binary());
    }

    binary
}


pub fn updates_from_binary(binary: &[u8]) -> Result<Vec<Update>, EzError> {
    let mut updates = Vec::new();

    for chunk in binary.chunks(144) {

        updates.push(Update::from_binary(&chunk)?);

    }

    Ok(updates)
}


#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum UpdateOp {
    Assign,
    PlusEquals,
    MinusEquals,
    TimesEquals,
    Append,
    Prepend,
}

impl UpdateOp {
    fn from_str(s: &str) -> Result<Self, EzError> {
        // println!("calling: UpdateOp::from_str()");

        match s {
            "=" => Ok(UpdateOp::Assign),
            "+=" => Ok(UpdateOp::PlusEquals),
            "-=" => Ok(UpdateOp::MinusEquals),
            "*=" => Ok(UpdateOp::TimesEquals),
            "append" => Ok(UpdateOp::Append),
            "assign" => Ok(UpdateOp::Assign),
            "prepend" => Ok(UpdateOp::Prepend),
            _ => Err(EzError{tag: ErrorTag::Query, text: format!("'{}' is not a valid UpdateOp", s)}),
        }
    }

    pub fn to_keystring(&self) -> KeyString {
        match self {
            UpdateOp::Assign => KeyString::from("Assign"),
            UpdateOp::PlusEquals => KeyString::from("PlusEquals"),
            UpdateOp::MinusEquals => KeyString::from("MinusEquals"),
            UpdateOp::TimesEquals => KeyString::from("TimesEquals"),
            UpdateOp::Append => KeyString::from("Append"),
            UpdateOp::Prepend => KeyString::from("Prepend"),
        }
    }

    pub fn to_binary(&self) -> [u8;8] {
        match self {
            UpdateOp::Assign => (1 as u64).to_le_bytes(),
            UpdateOp::PlusEquals => (2 as u64).to_le_bytes(),
            UpdateOp::MinusEquals => (3 as u64).to_le_bytes(),
            UpdateOp::TimesEquals => (4 as u64).to_le_bytes(),
            UpdateOp::Append => (5 as u64).to_le_bytes(),
            UpdateOp::Prepend => (6 as u64).to_le_bytes(),
        }
    }

    pub fn from_binary(binary: &[u8]) -> Result<UpdateOp, EzError> {
        match u64_from_le_slice(binary) {
            1 => Ok(UpdateOp::Assign),
            2 => Ok(UpdateOp::PlusEquals),
            3 => Ok(UpdateOp::MinusEquals),
            4 => Ok(UpdateOp::TimesEquals),
            5 => Ok(UpdateOp::Append),
            6 => Ok(UpdateOp::Prepend),
            other => return Err(EzError { tag: ErrorTag::Deserialization, text: format!("Unknown value: '{other}' encountered as UpdateOp") })
        }
    }
}




/// This enum represents the possible ways to list primary keys to test. 
/// See EZQL spec for details (handlers.rs).
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RangeOrListOrAll {
    Range(KeyString, KeyString),
    List(Vec<KeyString>),
    All,
}

impl Display for RangeOrListOrAll {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // println!("calling: RangeOrListOrAll::fmt()");

        let mut printer = String::new();
        match &self {
            RangeOrListOrAll::Range(start, stop) => printer.push_str(&format!("{}..{}", start, stop)),
            RangeOrListOrAll::List(list) => {
                printer.push('(');
                printer.push_str(&print_sep_list(list, ", "));
                printer.push(')');
            },
            RangeOrListOrAll::All => printer.push('*'),
        };
        write!(f, "{}", printer)
    }
}

impl RangeOrListOrAll {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut binary = Vec::new();
        match self {
            RangeOrListOrAll::Range(from, to) => {
                binary.extend_from_slice(KeyString::from("RANGE").raw());
                binary.extend_from_slice(from.raw());
                binary.extend_from_slice(to.raw());
            },
            RangeOrListOrAll::List(vec) => {
                binary.extend_from_slice(KeyString::from("LIST").raw());
                binary.extend_from_slice(&vec.len().to_le_bytes());
                for s in vec {
                    binary.extend_from_slice(s.raw());

                }
            },
            RangeOrListOrAll::All => {
                binary.extend_from_slice(KeyString::from("ALL").raw());
            },
        };
        binary
    }

    pub fn from_binary(binary: &[u8]) -> Result<Self, EzError> {
        if binary.len() < 64 {
            return Err(EzError{tag: ErrorTag::Query, text: format!("RangeOrListOrAll is always at least 64 bytes. Input binary is only '{}'", binary.len())})
        }
        let first = KeyString::try_from(&binary[0..64]).unwrap();
        match first.as_str() {
            "RANGE" => {
                if binary.len() != 192 {
                    return Err(EzError{tag: ErrorTag::Query, text: format!("Range is always 192 bytes. Input binary is '{}'", binary.len())})
                }
                let from = KeyString::try_from(&binary[64..128]).unwrap();
                let to = KeyString::try_from(&binary[128..192]).unwrap();
                Ok(RangeOrListOrAll::Range(from, to))
            }
            "LIST" => {
                if (binary.len()-8) % 64 != 0 {
                    return Err(EzError{tag: ErrorTag::Query, text: format!("List is always a multiple of 64 bytes. Input binary is {}", binary.len())})
                }
                let mut list = Vec::new();
                let list_len = u64_from_le_slice(&binary[64..72]) as usize;
                for chunk in binary[72..72+64*list_len].chunks(64) {
                    list.push(KeyString::try_from(chunk).unwrap());
                }
                Ok(RangeOrListOrAll::List(list))
            }
            "ALL" => {
                Ok(RangeOrListOrAll::All)
            }
            _ => return Err(EzError{tag: ErrorTag::Query, text: format!("'{}' is neither 'RANGE' nor 'LIST' nor 'ALL'", first)})
        }
    }
}

/// Represents the condition a item must pass to be included in the result
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub struct Condition {
    pub attribute: KeyString,
    pub test: Test,
}

impl Display for Condition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // println!("calling: Condition::fmt()");

        write!(f, "{} - {}", self.attribute, self.test)
    }
}

impl Condition {

    pub fn new(attribute: &str, test: Test) -> Result<Self, EzError> {
        // println!("calling: Condition::new()");

        Ok(Condition {
            attribute: KeyString::from(attribute),
            test,
        })
    }


    pub fn from_binary(binary: &[u8]) -> Result<Self, EzError> {
        let attribute = KeyString::try_from(&binary[0..64])?;
        let test = Test::from_binary(&binary[64..192])?;
        Ok( Condition {attribute, test} )
    }

    pub fn blank() -> Self {
        // println!("calling: Condition::blank()");

        Condition {
            attribute: KeyString::from(""),
            test: Test::Equals(DbValue::Int(0)),
        }
    }
}



#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Operator {
    AND,
    OR,
}

impl Operator {
    pub fn to_keystring(&self) -> KeyString {
        match self {
            Operator::AND => KeyString::from("AND"),
            Operator::OR => KeyString::from("OR"),
        }
    }
}

#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum OpOrCond {
    Cond(Condition),
    Op(Operator),
}

impl Display for OpOrCond {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // println!("calling: OpOrCond::fmt()");

        match self {
            OpOrCond::Cond(cond) => write!(f, "({} {})", cond.attribute, cond.test),
            OpOrCond::Op(op) => match op {
                Operator::AND => write!(f, "AND"),
                Operator::OR => write!(f, "OR"),
            },
        }
    }
}

impl OpOrCond {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut binary = Vec::new();
        match self {
            OpOrCond::Cond(condition) => {
                binary.extend_from_slice(condition.attribute.raw());
                binary.extend_from_slice(&condition.test.to_binary());
            },
            OpOrCond::Op(operator) => binary.extend_from_slice(operator.to_keystring().raw()),
        }
        binary
    }

    pub fn from_binary(binary: &[u8]) -> Result<OpOrCond, EzError> {
        if binary.len() < 64 {
            return Err(EzError{tag: ErrorTag::Query, text: format!("OpOrCond is at least 64 bytes. Input binary is {}", binary.len())})
        }

        let first = KeyString::try_from(&binary[0..64])?;
        match first.as_str() {
            "AND" => Ok(OpOrCond::Op(Operator::AND)),
            "OR" => Ok(OpOrCond::Op(Operator::OR)),
            _ => {
                if binary.len() < 128 {
                    return Err(EzError{tag: ErrorTag::Query, text: format!("Cond is at least 128 bytes. Input binary is {}", binary.len())})
                }
                let second = Test::from_binary(&binary[64..])?;
                Ok(OpOrCond::Cond(Condition{attribute: first, test: second}))
            }
        }

    }
}


pub fn conditions_from_binary(binary: &[u8]) -> Result<Vec<OpOrCond>, EzError> {
    if binary.is_empty() {
        return Ok(Vec::new())
    }
    
    if binary.len() < 136 {
        return Err(EzError{tag: ErrorTag::Query, text: format!("Condition is at least 136 bytes. Input binary is {}", binary.len())})

    }
    let mut conditions = Vec::new();

    let mut offset = 0;
    let mut i = 1;
    while offset < binary.len() {
        if i % 2 == 0 {
            conditions.push(OpOrCond::from_binary(&binary[offset..offset+64])?);
            offset += 64;
        } else {
            conditions.push(OpOrCond::from_binary(&binary[offset..offset+200])?);
            offset += 200;
        }
        i += 1;
    }

    Ok(conditions)
}

/// Represents the currenlty implemented tests
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum Test {
    Equals(DbValue),
    NotEquals(DbValue),
    Less(DbValue),
    Greater(DbValue),
    Starts(DbValue),
    Ends(DbValue),
    Contains(DbValue),
    //Closure,   could you imagine?
}

impl Display for Test {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // println!("calling: Test::fmt()");

        match self {
            Test::Equals(value) => write!(f, "equals {}", value),
            Test::NotEquals(value) => write!(f, "not_equals {}", value),
            Test::Less(value) => write!(f, "less_than {}", value),
            Test::Greater(value) => write!(f, "greater_than {}", value),
            Test::Starts(value) => write!(f, "starts_with {}", value),
            Test::Ends(value) => write!(f, "ends_with {}", value),
            Test::Contains(value) => write!(f, "contains {}", value),
        }
    }
}

impl Test {
    pub fn new(input: &str, bar: DbValue) -> Self {
        // println!("calling: Test::new()");

        match input.to_lowercase().as_str() {
            "Equals" | "equals"  => Test::Equals(bar),
            "NotEquals" | "not_equals" => Test::NotEquals(bar),
            "Less" | "less_than" => Test::Less(bar),
            "Greater" | "greater_than" => Test::Greater(bar),
            "Starts" | "starts_with" => Test::Starts(bar),
            "Ends" | "ends_with" => Test::Ends(bar),
            "Contains" | "contains"=> Test::Contains(bar),
            _ => todo!(),
        }
    }

    pub fn to_binary(&self) -> [u8;136] {
        let mut binary = [0u8;136];
        match self {
            Test::Equals(val) => {
                binary[0..64].copy_from_slice(KeyString::from("EQUALS").raw());
                binary[64..136].copy_from_slice(&val.to_binary());
            },
            Test::NotEquals(val) => {
                binary[0..64].copy_from_slice(KeyString::from("NOT_EQUALS").raw());
                binary[64..136].copy_from_slice(&val.to_binary());    
            },
            Test::Less(val) => {
                binary[0..64].copy_from_slice(KeyString::from("LESS").raw());
                binary[64..136].copy_from_slice(&val.to_binary());    
            },
            Test::Greater(val) => {
                binary[0..64].copy_from_slice(KeyString::from("GREATER").raw());
                binary[64..136].copy_from_slice(&val.to_binary());    
            },
            Test::Starts(val) => {
                binary[0..64].copy_from_slice(KeyString::from("STARTS").raw());
                binary[64..136].copy_from_slice(&val.to_binary());    
            },
            Test::Ends(val) => {
                binary[0..64].copy_from_slice(KeyString::from("ENDS").raw());
                binary[64..136].copy_from_slice(&val.to_binary());    
            },
            Test::Contains(val) => {
                binary[0..64].copy_from_slice(KeyString::from("CONTAINS").raw());
                binary[64..136].copy_from_slice(&val.to_binary());    
            },
        }
        binary
    }

    pub fn from_binary(binary: &[u8]) -> Result<Self, EzError> {
        let t = KeyString::try_from(&binary[0..64])?;
        let v = DbValue::from_binary(&binary[64..])?;
        let x = match t.as_str() {
            "EQUALS" => Test::Equals(v),
            "NOT_EQUALS" => Test::NotEquals(v),
            "LESS" => Test::Less(v),
            "GREATER" => Test::Greater(v),
            "STARTS" => Test::Starts(v),
            "ENDS" => Test::Ends(v),
            "CONTAINS" => Test::Contains(v),
            _ => return Err(EzError{tag: ErrorTag::Query, text: format!("Test: '{}' is not supported", t)})
        };
        Ok(x)
    }
}

pub enum ConditionBranch<'a> {
    Branch(Vec<&'a ConditionBranch<'a>>),
    Leaf(Condition),
}


pub struct ParserState {
    depth: u8,
    stack: Vec<u8>,
    word_buffer: Vec<u8>,

}


pub fn subsplitter(s: &str) -> Vec<Vec<&str>> {
    // println!("calling: subsplitter()");


    let mut temp = Vec::new();
    for line in s.split(';') {
        temp.push(line.split(',').collect::<Vec<&str>>());
    }

    temp

}

#[inline]
pub fn is_even(x: usize) -> bool {
    // println!("calling: is_even()");

    0 == (x & 1)
}


pub fn parse_contained_token(s: &str, container_open: char, container_close: char) -> Option<&str> {
    // println!("calling: parse_contained_token()");

    let mut start = std::usize::MAX;
    let mut stop = 0;
    let mut inside = false;
    for (index, c) in s.chars().enumerate() {
        // println!("start: {}\tstop: {}\tindex: {}", start, stop, index);
        stop += 1;
        match c {
            x if x == container_open => {
                match inside {
                    true => {
                        if container_open == container_close {
                            stop = index;
                            break;
                        } else {
                            continue;
                        }
                    },
                    false => {
                        inside = true;
                        start = index + 1;
                    }
                };
            },
            x if x == container_close => {
                match inside {
                    true => {
                        stop = index;
                        break;
                    },
                    false => {
                        continue;
                    }
                };
            },
            _ => continue,
        };
    }

    if stop < start {
        return None
    }

    Some(&s[start..stop])
}

pub fn execute_kv_queries(kv_queries: Vec<KvQuery>, database: Arc<Database>) -> Vec<Result<Option<Value>, EzError>> {

    let mut result_values = Vec::new();

    for query in kv_queries {
        match query {
            KvQuery::Create(key_string, vec) => {
                let value = Value{
                    name: key_string,
                    body: vec,
                };
                match database.buffer_pool.add_value(value) {
                    Ok(_) => continue,
                    Err(e) => result_values.push(Err(e))
                };
                result_values.push(Ok(None));
            },
            KvQuery::Read(key_string) => {
                match database.buffer_pool.values.read().unwrap().get(&key_string) {
                    Some(v) => {
                        result_values.push(Ok(Some(v.clone())));
                    },
                    None => result_values.push(Err(EzError{tag: ErrorTag::Query, text: format!("No value corresponds to key: '{}'", key_string)}))
                };
            },
            KvQuery::Update(key_string, vec) => {
                let value = Value{
                    name: key_string,
                    body: vec,
                };

                let read_lock = database.buffer_pool.values.read().unwrap();
                if read_lock.contains_key(&key_string) {
                    drop(read_lock);
                    let mut write_lock = database.buffer_pool.values.write().unwrap();
                    write_lock.insert(key_string, value);
                    result_values.push(Ok(None));
                } else {
                    result_values.push(Err(EzError{tag: ErrorTag::Query, text: format!("No value corresponds to key: '{}'", key_string)}))
                }

            },
            KvQuery::Delete(key_string) => {
                match database.buffer_pool.values.write().unwrap().remove(&key_string) {
                    Some(v) => {
                        result_values.push(Ok(Some(v.clone())));
                    },
                    None => result_values.push(Err(EzError{tag: ErrorTag::Query, text: format!("No value corresponds to key: '{}'", key_string)}))
                };
            },
        }
    }

    result_values

}

#[allow(non_snake_case)]
pub fn execute_EZQL_queries(queries: Vec<Query>, database: Arc<Database>) -> Result<Option<ColumnTable>, EzError> {
    // println!("calling: execute_EZQL_queries()");


    let mut result_table = None;
    for query in queries.into_iter() {

        match &query {
            Query::DELETE{ primary_keys: _, table_name, conditions: _ } => {
                match result_table {
                    Some(mut table) => result_table = execute_delete_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(table_name).unwrap().write().unwrap();
                        result_table = execute_delete_query(query, &mut table)?;
                        database.buffer_pool.table_naughty_list.write().unwrap().insert(table.name);
                    },
                }
                
            },
            Query::SELECT{ table_name, primary_keys: _, columns: _, conditions: _ } => {
                match result_table {
                    Some(mut table) => result_table = execute_select_query(query, &mut table)?,
                    None => {
                        println!("table name: {}", table_name);
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let table = tables.get(table_name).unwrap().read().unwrap();
                        result_table = execute_select_query(query, &table)?;
                    },
                }
            },
            Query::LEFT_JOIN{ left_table_name, right_table_name, match_columns: _, primary_keys: _ } => {
                match result_table {
                    Some(table) => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let right_table = tables.get(right_table_name).unwrap().read().unwrap();
                        result_table = execute_left_join_query(query, &table, &right_table)?;
                    },
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let left_table = tables.get(left_table_name).unwrap().read().unwrap();
                        let right_table = tables.get(right_table_name).unwrap().read().unwrap();
                        execute_left_join_query(query, &left_table, &right_table)?;
                    },
                }
                
            },
            Query::INNER_JOIN => {
                unimplemented!("Inner joins are not yet implemented");
                // execute_inner_join_query(query, database);
            },
            Query::RIGHT_JOIN => {
                unimplemented!("Right joins are not yet implemented");

                // execute_right_join_query(query, database);
            },
            Query::FULL_JOIN => {
                unimplemented!("Full joins are not yet implemented");

                // execute_full_join_query(query, database);
            },
            Query::UPDATE{ table_name, primary_keys: _, conditions: _, updates: _ } => {
                match result_table {
                    Some(mut table) => result_table = execute_update_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(table_name).unwrap().write().unwrap();
                        result_table = execute_update_query(query, &mut table)?;
                        database.buffer_pool.table_naughty_list.write().unwrap().insert(table.name);
                    },
                }
            },
            Query::INSERT{ table_name, inserts: _ } => {
                match result_table {
                    Some(mut table) => result_table = execute_insert_query(query, &mut table)?,
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let mut table = tables.get(table_name).unwrap().write().unwrap();
                        result_table = execute_insert_query(query, &mut table)?;
                        database.buffer_pool.table_naughty_list.write().unwrap().insert(table.name);
                    },
                }
            },
            
            Query::SUMMARY { table_name, columns } => {
                match result_table {
                    Some(table) => {
                        let result = execute_summary_query(&query, &table)?;
                        match result {
                            Some(s) => return Ok(Some(s)),
                            None => todo!(),
                        };
                    },
                    None => {
                        let tables = database.buffer_pool.tables.read().unwrap();
                        let table = tables.get(table_name).unwrap().read().unwrap();
                        let result = execute_summary_query(&query, &table)?;
                        match result {
                            Some(s) => return Ok(Some(s)),
                            None => todo!(),
                        };
                    },
                }
            }
            Query::CREATE { table } => {
                match database.buffer_pool.add_table(table.clone()) {
                    Ok(_) => {
                        result_table = None;
                    },
                    Err(e) => return Err(e),
                }
            },
            Query::DROP { table_name } => {
                match database.buffer_pool.remove_table(*table_name) {
                    Ok(_) => {
                        result_table = None;
                    },
                    Err(e) => return Err(e),
                }
            },
        }
    }

    match result_table {
        Some(table) => Ok(Some(table)),
        None => Ok(None),
    }
}


pub fn execute_delete_query(query: Query, table: &mut ColumnTable) -> Result<Option<ColumnTable>, EzError> {
    // println!("calling: execute_delete_query()");
    
    match query {
        Query::DELETE { primary_keys, table_name: _, conditions } => {
            let keepers = filter_keepers(&conditions, &primary_keys, table)?;
            table.delete_by_indexes(&keepers);
        
            Ok(
                None
            )
        },
        other_query => return Err(EzError{tag: ErrorTag::Query, text: format!("Wrong type of query passed to execute_delete_query() function.\nReceived query: {}", other_query)}),
    }

}

pub fn execute_left_join_query(query: Query, left_table: &ColumnTable, right_table: &ColumnTable) -> Result<Option<ColumnTable>, EzError> {
    // println!("calling: execute_left_join_query()");
    
    match query {
        Query::LEFT_JOIN { left_table_name: _, right_table_name: _, match_columns, primary_keys } => {
            let filtered_indexes = keys_to_indexes(left_table, &primary_keys)?;
            let mut filtered_table = left_table.subtable_from_indexes(&filtered_indexes, &KeyString::from("__RESULT__"));
        
            filtered_table.alt_left_join(right_table, &match_columns.0)?;
        
            Ok(Some(filtered_table))
        },
        other_query => return Err(EzError{tag: ErrorTag::Query, text: format!("Wrong type of query passed to execute_left_join_query() function.\nReceived query: {}", other_query)}),
    }    
}


#[inline]
pub fn update_i32(keepers: &[usize], column: &mut [i32], op: UpdateOp, value: &DbValue) -> Result<(), EzError> {
    let new_value = match value {
        DbValue::Int(x) => x,
        _ => return Err(EzError { tag: ErrorTag::Query, text: format!("an int can only be updated by an int") })
    };
    match op {
        UpdateOp::Assign => {
            for keeper in keepers {
                column[*keeper] = *new_value;
            }

        },
        UpdateOp::PlusEquals => {
            for keeper in keepers {
                column[*keeper] += new_value;
            }
        },
        UpdateOp::MinusEquals => {
            for keeper in keepers {
                column[*keeper] -= new_value;
            }
        },
        UpdateOp::TimesEquals => {
            for keeper in keepers {
                column[*keeper] *= new_value;
            }
        },
        UpdateOp::Append => {
            return Err(EzError{tag: ErrorTag::Query, text: "'append' operator can only be performed on text data".to_owned()})
        },
        UpdateOp::Prepend => {
            return Err(EzError{tag: ErrorTag::Query, text: "'prepend' operator can only be performed on text data".to_owned()})
        },
    }
    Ok(())
}

#[inline]
pub fn update_f32(keepers: &[usize], column: &mut [f32], op: UpdateOp, value: &DbValue) -> Result<(), EzError> {
    let new_value = match value {
        DbValue::Float(x) => x,
        _ => return Err(EzError { tag: ErrorTag::Query, text: format!("a float can only be updated by a float") })
    };
    match op {
        UpdateOp::Assign => {
            for keeper in keepers {
                column[*keeper] = *new_value;
            }

        },
        UpdateOp::PlusEquals => {
            for keeper in keepers {
                column[*keeper] += new_value;
            }
        },
        UpdateOp::MinusEquals => {
            for keeper in keepers {
                column[*keeper] -= new_value;
            }
        },
        UpdateOp::TimesEquals => {
            for keeper in keepers {
                column[*keeper] *= new_value;
            }
        },
        UpdateOp::Append => {
            return Err(EzError{tag: ErrorTag::Query, text: "'append' operator can only be performed on text data".to_owned()})
        },
        UpdateOp::Prepend => {
            return Err(EzError{tag: ErrorTag::Query, text: "'prepend' operator can only be performed on text data".to_owned()})
        },
    }
    Ok(())
}

#[inline]
pub fn update_keystrings(keepers: &[usize], column: &mut [KeyString], op: UpdateOp, value: &DbValue) -> Result<(), EzError> {
    let new_value = match value {
        DbValue::Text(x) => x,
        _ => return Err(EzError { tag: ErrorTag::Query, text: format!("an int can only be updated by an int") })
    };
    match op {
        UpdateOp::Assign => {
            for keeper in keepers {
                column[*keeper] = *new_value;
            }
        },
        UpdateOp::PlusEquals => return Err(EzError{tag: ErrorTag::Query, text: "Can't do math on text".to_owned()}),
        UpdateOp::MinusEquals => return Err(EzError{tag: ErrorTag::Query, text: "Can't do math on text".to_owned()}),
        UpdateOp::TimesEquals => return Err(EzError{tag: ErrorTag::Query, text: "Can't do math on text".to_owned()}),
        UpdateOp::Append => {
            for keeper in keepers {
                column[*keeper].push(new_value.as_str());
            }
        },
        UpdateOp::Prepend => {
            for keeper in keepers {
                let mut temp = column[*keeper];
                temp.push(new_value.as_str());
                column[*keeper].push(temp.as_str());
            }
        },
    }
    Ok(())
}

pub fn execute_update_query(query: Query, table: &mut ColumnTable) -> Result<Option<ColumnTable>, EzError> {
    match query {
        Query::UPDATE { table_name: _, primary_keys, conditions, mut updates } => {
            let keepers = filter_keepers(&conditions, &primary_keys, table)?;

            updates.sort_by(|a, b| a.attribute.cmp(&b.attribute));

            for update in &updates{

                let active_column = match table.columns.get_mut(&update.attribute) {
                    Some(x) => x,
                    None => return Err(EzError{tag: ErrorTag::Query, text: format!("Table does not contain column {}", update.attribute)})
                };

                match active_column {
                    DbColumn::Ints(vec) => update_i32(&keepers, vec.as_mut_slice(), update.operator, &update.value)?,
                    DbColumn::Texts(vec) => update_keystrings(&keepers, vec.as_mut_slice(), update.operator, &update.value)?,
                    DbColumn::Floats(vec) => update_f32(&keepers, vec.as_mut_slice(), update.operator, &update.value)?,
                }
            }
            
            Ok(
                None    
            )
        },
        other_query => return Err(EzError{tag: ErrorTag::Query, text: format!("Wrong type of query passed to execute_update_query() function.\nReceived query: {}", other_query)}),
    }
}

pub fn execute_insert_query(query: Query, table: &mut ColumnTable) -> Result<Option<ColumnTable>, EzError> {
    // println!("calling: execute_insert_query()");

    match query {
        Query::INSERT { table_name: _, inserts } => {
            table.insert(inserts)?;
        
            Ok(
                None
            )
        },
        other_query => return Err(EzError{tag: ErrorTag::Query, text: format!("Wrong type of query passed to execute_insert_query() function.\nReceived query: {}", other_query)}),

    }
}

pub fn execute_select_query(query: Query, table: &ColumnTable) -> Result<Option<ColumnTable>, EzError> {
    // println!("calling: execute_select_query()");

    match query {
        Query::SELECT { table_name: _, primary_keys, columns, conditions } => {
            let keepers = filter_keepers(&conditions, &primary_keys, table)?;
        
            Ok(
                Some(
                    table
                        .subtable_from_indexes(&keepers, &KeyString::from("RESULT"))
                        .subtable_from_columns(&columns, "RESULT")?
                    )
            )
        },
        other_query => return Err(EzError{tag: ErrorTag::Query, text: format!("Wrong type of query passed to execute_select_query() function.\nReceived query: {}", other_query)}),
    }
}


pub fn execute_summary_query(query: &Query, table: &ColumnTable) -> Result<Option<ColumnTable>, EzError> {
    match query {
        Query::SUMMARY { table_name: _, columns } => {
            let mut result = ColumnTable::blank(&BTreeSet::new(), KeyString::from("RESULT"), "QUERY");

            result.add_column(ksf("Statistic"), DbColumn::Texts(vec![
                ksf("SUM"),
                ksf("MEAN"),
                ksf("MEDIAN"),
                ksf("MODE"),
                ksf("STDEV"),
            ]))?;

            for stat in columns {
                let requested_column = match table.columns.get(&stat.column) {
                    Some(x) => x,
                    None => return Err(EzError{tag: ErrorTag::Query, text: format!("No column named {} in table {}", stat.column, table.name)}),
                };

                match requested_column {
                    DbColumn::Ints(vec) => {
                        let mut temp = [0i32; 5].to_vec();
                        for action in &stat.actions {
                            match action {
                                StatOp::SUM => temp[0] = sum_i32_slice(&vec),
                                StatOp::MEAN => temp[1] = mean_i32_slice(&vec) as i32,
                                StatOp::MEDIAN => temp[2] = median_i32_slice(&vec) as i32,
                                StatOp::MODE => temp[3] = mode_i32_slice(&vec),
                                StatOp::STDEV => temp[4] = stdev_i32_slice(&vec) as i32,
                            }
                        }
                        result.add_column(stat.column, DbColumn::Ints(temp))?;
                    },
                    DbColumn::Texts(vec) => {
                        let mut temp = [ksf(""); 5].to_vec();
                        for action in &stat.actions {
                            match action {
                                StatOp::SUM => temp[0] = ksf("can't sum text"),
                                StatOp::MEAN => temp[1] = ksf("can't mean text"),
                                StatOp::MEDIAN => temp[2] = ksf("can't median text"),
                                StatOp::MODE => temp[3] = mode_string_slice(&vec),
                                StatOp::STDEV => temp[4] = ksf("can't stdev text"),
                            }
                        }
                        result.add_column(stat.column, DbColumn::Texts(temp))?;
                    },
                    DbColumn::Floats(vec) => {
                        let mut temp = [0f32; 5].to_vec();
                        for action in &stat.actions {
                            match action {
                                StatOp::SUM => temp[0] = sum_f32_slice(&vec),
                                StatOp::MEAN => temp[1] = mean_f32_slice(&vec),
                                StatOp::MEDIAN => temp[2] = median_f32_slice(&vec),
                                StatOp::MODE => temp[3] = 0.0,
                                StatOp::STDEV => temp[4] = stdev_f32_slice(&vec),
                            }
                        }
                        result.add_column(stat.column, DbColumn::Floats(temp))?;
                    },
                }
            }

            Ok(Some(result))

        },
        other_query => return Err(EzError{tag: ErrorTag::Query, text: format!("Wrong type of query passed to execute_select_query() function.\nReceived query: {}", other_query)}),
    }
}

#[allow(unused)]
pub fn execute_inner_join_query(query: Query, database: Arc<Database>) -> Result<Option<ColumnTable>, EzError> {
    // println!("calling: execute_inner_join_query()");
    
    // let tables = database.buffer_pool.tables.read().unwrap();
    // let table = tables.get(&query.table).unwrap().read().unwrap();
    // let keepers = filter_keepers(&query, &table)?;

    Err(EzError{tag: ErrorTag::Unimplemented, text: "inner joins are not yet implemented".to_owned()})
}

#[allow(unused)]
pub fn execute_right_join_query(query: Query, database: Arc<Database>) -> Result<Option<ColumnTable>, EzError> {
    // println!("calling: execute_right_join_query()");

    // let tables = database.buffer_pool.tables.read().unwrap();
    // let table = tables.get(&query.table).unwrap().read().unwrap();
    // let keepers = filter_keepers(&query, &table)?;

    Err(EzError{tag: ErrorTag::Unimplemented, text: "right joins are not yet implemented".to_owned()})
}
#[allow(unused)]
pub fn execute_full_join_query(query: Query, database: Arc<Database>) -> Result<Option<ColumnTable>, EzError> {
    // println!("calling: execute_full_join_query()");

    // let tables = database.buffer_pool.tables.read().unwrap();
    // let table = tables.get(&query.table).unwrap().read().unwrap();
    // let keepers = filter_keepers(&query, &table)?;

    Err(EzError{tag: ErrorTag::Unimplemented, text: "full joins are not yet implemented".to_owned()})
}

pub fn keys_to_indexes(table: &ColumnTable, keys: &RangeOrListOrAll) -> Result<Vec<usize>, EzError> {
    // println!("calling: keys_to_indexes()");

    let mut indexes = Vec::new();

    match keys {
        RangeOrListOrAll::Range(ref start, ref stop) => {
            match &table.columns[&table.get_primary_key_col_index()] {
                DbColumn::Ints(column) => {
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
                DbColumn::Texts(column) => {
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
                DbColumn::Floats(_n) => unreachable!("There should never be a float primary key"),
            }
        },
        RangeOrListOrAll::List(ref keys) => {
            match &table.columns[&table.get_primary_key_col_index()] {
                DbColumn::Ints(column) => {
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
                DbColumn::Texts(column) => {
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
                DbColumn::Floats(_) => unreachable!("There should never be a float primary key"),
            }
        },
        RangeOrListOrAll::All => indexes = (0..table.len()).collect(),
    };

    Ok(indexes)
}


pub fn filter_keepers(conditions: &Vec<OpOrCond>, primary_keys: &RangeOrListOrAll, table: &ColumnTable) -> Result<Vec<usize>, EzError> {
    // println!("calling: filter_keepers()");

    let indexes = keys_to_indexes(table, primary_keys)?;
    
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
                                    DbColumn::Ints(col) => if col[*index] == bar.to_i32() {keepers.push(*index)},
                                    DbColumn::Floats(col) => if col[*index] == bar.to_f32() {keepers.push(*index)},
                                    DbColumn::Texts(col) => if col[*index] == bar.to_keystring() {keepers.push(*index)},
                                }
                            },
                            Test::NotEquals(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*index] != bar.to_i32() {keepers.push(*index)},
                                    DbColumn::Floats(col) => if col[*index] != bar.to_f32() {keepers.push(*index)},
                                    DbColumn::Texts(col) => if col[*index] != bar.to_keystring() {keepers.push(*index)},
                                }
                            },
                            Test::Less(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*index] < bar.to_i32() {keepers.push(*index)},
                                    DbColumn::Floats(col) => if col[*index] < bar.to_f32() {keepers.push(*index)},
                                    DbColumn::Texts(col) => if col[*index] < bar.to_keystring() {keepers.push(*index)},
                                }
                            },
                            Test::Greater(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*index] > bar.to_i32() {keepers.push(*index)},
                                    DbColumn::Floats(col) => if col[*index] > bar.to_f32() {keepers.push(*index)},
                                    DbColumn::Texts(col) => if col[*index] > bar.to_keystring() {keepers.push(*index)},
                                }
                            },
                            Test::Starts(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().starts_with(bar.to_keystring().as_str()) {keepers.push(*index)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'starts_with' on text values".to_owned()}),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().ends_with(bar.to_keystring().as_str()) {keepers.push(*index)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'ends_with' on text values".to_owned()}),
                                }
                            },
                            Test::Contains(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*index].as_str().contains(bar.to_keystring().as_str()) {keepers.push(*index)},
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
                                    DbColumn::Ints(col) => if col[*keeper] == bar.to_i32() {losers.push(*keeper)},
                                    DbColumn::Floats(col) => if col[*keeper] == bar.to_f32() {losers.push(*keeper)},
                                    DbColumn::Texts(col) => if col[*keeper] == bar.to_keystring() {losers.push(*keeper)},
                                }
                            },
                            Test::NotEquals(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*keeper] != bar.to_i32() {losers.push(*keeper)},
                                    DbColumn::Floats(col) => if col[*keeper] != bar.to_f32() {losers.push(*keeper)},
                                    DbColumn::Texts(col) => if col[*keeper] != bar.to_keystring() {losers.push(*keeper)},
                                }
                            },
                            Test::Less(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*keeper] < bar.to_i32() {losers.push(*keeper)},
                                    DbColumn::Floats(col) => if col[*keeper] < bar.to_f32() {losers.push(*keeper)},
                                    DbColumn::Texts(col) => if col[*keeper] < bar.to_keystring() {losers.push(*keeper)},
                                }
                            },
                            Test::Greater(bar) => {
                                match column {
                                    DbColumn::Ints(col) => if col[*keeper] > bar.to_i32() {losers.push(*keeper)},
                                    DbColumn::Floats(col) => if col[*keeper] > bar.to_f32() {losers.push(*keeper)},
                                    DbColumn::Texts(col) => if col[*keeper] > bar.to_keystring() {losers.push(*keeper)},
                                }
                            },
                            Test::Starts(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().starts_with(bar.to_keystring().as_str()) {losers.push(*keeper)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'starts_with' on text values".to_owned()}),
                                }
                            },
                            Test::Ends(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().ends_with(bar.to_keystring().as_str()) {losers.push(*keeper)},
                                    _ => return Err(EzError{tag: ErrorTag::Query, text: "Can only filter by 'ends_with' on text values".to_owned()}),
                                }
                            },
                            Test::Contains(bar) => {
                                match column {
                                    DbColumn::Texts(col) => if col[*keeper].as_str().contains(bar.to_keystring().as_str()) {losers.push(*keeper)},
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


#[allow(non_snake_case)]
#[allow(unused)]
#[cfg(test)]
mod tests {

    // INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
    // SELECT(primary_keys: *, table_name: products, conditions: ((price greater_than 500) AND (stock less_than 1000)))
    // UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts_with 011)), updates: ((price += 100), (stock -= 100)))
    // DELETE(primary_keys: *, table_name: products, conditions: ((price greater_than 500) AND (stock less_than 1000)))
    // LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)
    // SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))


    use std::default;

    use rand::Rng;

    use crate::{testing_tools::{random_column_table, random_kv_query, random_query}, utilities::ksf};

    use super::*;


    #[test]
    fn test_parse_contained_token() {
        let text = "hello. (this part is contained). \"This one is not\"";
        let output= parse_contained_token(text, '(', ')').unwrap();
        assert_eq!(output, "this part is contained");
        let second = parse_contained_token(text, '"', '"').unwrap();
        assert_eq!(second, "This one is not");

    }


    #[test]
    fn test_queries_from_binary() {
        for _ in 0..100 {
            let i = rand::thread_rng().gen_range(1..10);
            if i == 1 {
                let query = random_query();
                let bin_query = query.to_binary();
                let parsed_query = Query::from_binary(&bin_query).unwrap();
                assert_eq!(query, parsed_query);
            } else {
                
                let mut queries = Vec::new();
                for _ in 0..i {
                    let query = random_query();
                    queries.push(query);
                }
                let binary = queries_to_binary(&queries);
                
                let parsed_queries = parse_queries_from_binary(&binary).unwrap();
                assert_eq!(queries, parsed_queries);
            }
            
        }

    }

    #[test]
    fn test_base_query() {
        let query = Query::SELECT { 
            table_name: ksf("good_table"),
            primary_keys: RangeOrListOrAll::All,
            columns: vec![ksf("id"), ksf("name"), ksf("price")],
            conditions: vec![
                OpOrCond::Cond(Condition{attribute: ksf("id"), test: Test::Equals(DbValue::Int(4))}),
                OpOrCond::Op(Operator::AND),
                OpOrCond::Cond(Condition{attribute: ksf("name"), test: Test::Equals(DbValue::Text(ksf("four")))}),
                
            ],
        };
        let binary = query.to_binary();
        println!("query len = {}", binary.len());
        println!("{:?}", binary);
        let parsed = Query::from_binary(&binary).unwrap();
        assert_eq!(query, parsed);
    }

    #[test]
    fn test_CREATE_query_binary() {
        for i in 0..100 {
            let query = random_query();
            let binary_query = query.to_binary();
            let parsed_query = Query::from_binary(&binary_query).unwrap();
            assert_eq!(query, parsed_query);
        }
    }

    #[test]
    fn test_base_kv_query() {
        let kv_query = KvQuery::Create(ksf("test"), vec![0,1,2,3,4,5,6,7,8,9]);
        let bin_query = kv_query.to_binary();
        let parsed_query = KvQuery::from_binary(&bin_query).unwrap();

        assert_eq!(kv_query, parsed_query);
    }

    #[test]
    fn test_kv_queries() {
        let mut kv_queries = Vec::new();

        for _ in 0..100 {
            kv_queries.push(random_kv_query());
        }

        let mut binary = Vec::new();

        for query in &kv_queries {
            binary.extend_from_slice(&query.to_binary());
        }

        let parsed_queries = parse_kv_queries_from_binary(&binary).unwrap();

        assert_eq!(kv_queries, parsed_queries);

    }


}