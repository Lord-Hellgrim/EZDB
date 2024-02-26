use std::{collections::HashSet, fmt::{self, Display, Debug}, io::Write};

use smartstring::{SmartString, LazyCompact};

use crate::networking_utilities::*;

use crate::PATH_SEP;

pub type KeyString = SmartString<LazyCompact>;

#[derive(Debug, PartialEq)]
pub enum StrictError {
    MoreItemsThanHeader(usize),
    FewerItemsThanHeader(usize),
    RepeatingHeader(usize, usize),
    FloatPrimaryKey,
    Empty,
    Update(String),
    Io(std::io::ErrorKind),
    MissingType,
    TooManyHeaderFields,
    WrongType,
    Parse(usize),
    TooManyPrimaryKeys,
    WrongKey,
    NonUniquePrimaryKey(usize),
    BinaryRead(String),
    Query(String),
}

impl fmt::Display for StrictError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StrictError::MoreItemsThanHeader(n) => write!(f, "There are more items in line {} than in the header.\n", n),
            StrictError::FewerItemsThanHeader(n) => write!(f, "There are less items in line {} than in the header.\n", n),
            StrictError::RepeatingHeader(n, m) => write!(f, "Item {} and {} are repeated in the header.\n", n, m),
            StrictError::FloatPrimaryKey => write!(f, "Primary key can't be a floating point number. Must be an integer or string."),
            StrictError::Empty => write!(f, "Don't pass an empty string."),
            StrictError::Update(s) => write!(f, "Failed to update because:\n{s}"),
            StrictError::Io(e) => write!(f, "Failed to write to disk because: \n--> {e}"),
            StrictError::MissingType => write!(f, "Missing type from header"),
            StrictError::TooManyHeaderFields => write!(f, "Too many fields in header"),
            StrictError::WrongType => write!(f, "Wrong type specified in header"),
            StrictError::Parse(i) => write!(f, "Item in line {i} cannot be parsed"),
            StrictError::TooManyPrimaryKeys => write!(f, "There can only be one primary key column"),
            StrictError::WrongKey => write!(f, "The type of the primary key is wrong"),
            StrictError::NonUniquePrimaryKey(i) => write!(f, "The primary key at position {i} in the sorted table is repeated"),
            StrictError::BinaryRead(s) => write!(f, "{}", s),
            StrictError::Query(s) => write!(f, "Query item {s} is incorrectly formatted"),
        }
    }
}

impl From<std::io::ErrorKind> for StrictError {
    fn from(e: std::io::ErrorKind) -> Self {
        StrictError::Io(e)
    }
}

// This struct is here to future proof the StrictTable. More metadata will be added in future.
#[derive(PartialEq, Clone, Debug)]
pub struct Metadata {
    pub last_access: u64,
    pub times_accessed: u64,
    pub created_by: KeyString,
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut printer = String::new();

        printer.push_str(&format!("last_access:{}\n", self.last_access));
        printer.push_str(&format!("times_accessed:{}\n", self.times_accessed));
        printer.push_str(&format!("created_by:{}", self.created_by));
        writeln!(f, "{}", printer)
    }
}

impl Metadata {
    pub fn new(client: &str) -> Metadata{
        Metadata{
            last_access: get_current_time(),
            times_accessed: 0,
            created_by: KeyString::from(client),
        }
    }
}


#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum DbEntry {
    Int(i64),
    Float(f64),
    Text(String),
    Empty,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum DbType {
    Int,
    Float,
    Text,
}

#[derive(Clone, Debug)]
pub enum DbVec {
    Ints(Vec<i32>),
    Floats(Vec<f32>),
    Texts(Vec<KeyString>),
}


#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HeaderItem {
    name: KeyString,
    kind: DbType,
    key: TableKey,
}

impl HeaderItem {
    pub fn new() -> HeaderItem {
        HeaderItem{
            name: KeyString::from("default_name"),
            kind: DbType::Text,
            key: TableKey::None,

        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum TableKey {
    Primary,
    None,
    Foreign,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]

pub struct Query {
    pub primary_keys: RangeOrListorAll,
    pub conditions: Vec<Condition>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum RangeOrListorAll{
    Range([KeyString;2]),
    List(Vec<KeyString>),
    All,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Condition {
    pub attribute: KeyString,
    pub test: Test,
    pub bar: KeyString,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Test {
    Equals,
    Less,
    Greater,
    Starts,
    Ends,
    Contains,
    //Closure,   could you imagine?
}

#[derive(Clone, Debug)]
pub struct ColumnTable {
    pub metadata: Metadata,
    pub name: KeyString,
    pub header: Vec<HeaderItem>,
    pub table: Vec<DbVec>,
}

impl Display for ColumnTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut printer = String::new();

        for item in &self.header {
            printer.push_str(&item.name);
            printer.push(',');
            match item.kind {
                DbType::Float => printer.push('f'),
                DbType::Int => printer.push('i'),
                DbType::Text => printer.push('t'),
            }
            match &item.key {
                TableKey::Primary => printer.push_str("-P"),
                TableKey::Foreign => printer.push_str("-F"),
                TableKey::None => printer.push_str("-N")
            }
            printer.push(';');
        }
        printer.pop();
        printer.push('\n');

        for i in 0..(self.len()) {

            for vec in &self.table {
                match vec {
                    DbVec::Floats(col) => {
                        // println!("float: col.len(): {}", col.len());
                        printer.push_str(&col[i].to_string());
                        printer.push(';');
                    },
                    DbVec::Ints(col) => {
                        // println!("int: col.len(): {}", col.len());
                        printer.push_str(&col[i].to_string());
                        printer.push(';');
                    },
                    DbVec::Texts(col) => {
                        // println!("text: col.len(): {}", col.len());
                        printer.push_str(&col[i]);
                        printer.push(';');
                    },
                }
            }
            printer.pop();
            printer.push('\n');
        }
        printer.pop();

        write!(f, "{}", printer)
    }
}

impl ColumnTable {
    pub fn from_csv_string(s: &str, table_name: &str, created_by: &str) -> Result<ColumnTable, StrictError> {


        /* 
        EZ CSV FORMAT:
        Table names shall be no more that 254 characters.

        Header is formatted like this:
        name1,type-key;name2,type-key;...;nameN,type-key        

        The name can be:
        Any string of characters except these three (;  ,  -) and of course newlines

        The type can be:
        I, Int, int, or i for integer data (i32)
        F, Float, float, or f for floating point data (f32)
        T, Text, text, or t for text data (String, ax length 255)

        The key should be one of the three:
        P - This column will be treated as the primary key. There can be only one P column
        FTableName - This column will be treated as a foreign key. The first character F denotes that this is a foreign key. If they foreign key references it's own table, that is an error.
        N - This column is neither a primary nor foreign key. It simply contains data

        The body is formatted like this:
        Given a header:
        id,i-P;name,Text-N;product_group,t-FProductGroup

        The body can be formatted like this:

        123;sample;samples
        234;plunger;toiletries
        567;racecar;toys

        If a value needs to contain a ";" character, you can enclose the calue in triple quotes """value"""
        Values will not be trimmed. Any whitespace will be included. Take care that the triple quotes are included in the 255 character limit for text values
        if you need to store text values longer than 255 characters, reference them by foreign keys to key_value storage
        */ 


        if s.is_empty() {
            return Err(StrictError::Empty)
        }

        let mut header = Vec::new();
        let mut primary_key_set = false;

        let first_line: Vec<&str> = s.split('\n').next().expect("confirmed to exist because of earlier check").split(';').collect();
        for item in first_line {
            let temp: Vec<&str> = item.split(',').collect();
            let mut header_item = HeaderItem::new();
            if temp.is_empty() {
                return Err(StrictError::MissingType)
            } else if temp.len() == 1{
                header_item.kind = DbType::Text;
            } else if temp.len() > 2 {
                return Err(StrictError::TooManyHeaderFields)
            } else {
                header_item.name = KeyString::from(temp[0].trim());
                let mut t = temp[1].trim().split('-');
                match t.next().unwrap() {
                    "I" | "Int" | "int" | "i" => header_item.kind = DbType::Int,
                    "F" | "Float" | "float" | "f" => header_item.kind = DbType::Float,
                    "T" | "Text" | "text" | "t" => header_item.kind = DbType::Text,
                    _ => return Err(StrictError::WrongType),
                }
                match t.next().unwrap() {
                    "P" => {
                        if primary_key_set {
                            return Err(StrictError::TooManyPrimaryKeys)
                        }
                        header_item.key = TableKey::Primary;
                        primary_key_set = true;
                    },
                    "N" => header_item.key = TableKey::None,
                    "F" => header_item.key = TableKey::Foreign,
                    _ => return Err(StrictError::WrongKey),
                }
            }
            header.push(header_item);
        }

        if !primary_key_set {
            match header[0].kind {
                DbType::Int => header[0].key = TableKey::Primary,
                DbType::Text => header[0].key = TableKey::Primary,
                _ => unreachable!("Should already have a primary key or have been rejected for float primary key")
            };
        }

        let mut line_index = 0;
        let mut data: Vec<Vec<&str>> = Vec::new();
        for line in s.lines() {
            // println!("line: {}", line);
            if line_index == 0 {
                line_index += 1;
                continue
            }
            for (row_index,cell) in line.split(';').enumerate() {
                if line_index == 1 {
                    data.push(Vec::from([cell]));
                } else {
                    data[row_index].push(cell);
                }

            }
            line_index += 1;
        }

        let mut result = Vec::new();
        let mut i = 0;
        for col in data {
            
            let db_vec = match header[i].kind {
                DbType::Float => {
                    let mut outvec = Vec::with_capacity(col.len());
                    let mut index = 0;
                    for cell in col {
                        let temp = match cell.parse::<f32>() {
                            Ok(x) => x,
                            Err(_) => {
                                println!("failed to parse float: {:x?}", cell.as_bytes());
                                return Err(StrictError::Parse(index))
                            },
                        };
                        outvec.push(temp);
                        index += 1;
                    }
                    DbVec::Floats(outvec)
                },
                DbType::Int => {
                    let mut outvec = Vec::with_capacity(col.len());
                    let mut index = 0;
                    for cell in col {
                        // println!("index: {} - cell: {}",index, cell);
                        let temp = match cell.parse::<i32>() {
                            Ok(x) => x,
                            Err(_) => {
                                return Err(StrictError::Parse(index))
                            },
                        };
                        outvec.push(temp);
                        index += 1;
                    }
                    DbVec::Ints(outvec)
                },
                DbType::Text => {
                    let mut outvec = Vec::with_capacity(col.len());
                    for cell in col {
                        outvec.push(KeyString::from(cell));
                    }
                    DbVec::Texts(outvec)
                },
            };
            
            result.push(db_vec);
            i += 1;
        }

        let mut primary_key_index = 0;
        for (index, item) in header.iter().enumerate() {
            if item.key == TableKey::Primary {
                primary_key_index = index;
            }
        };
        match &result[primary_key_index] {
            DbVec::Ints(col) => {
                let mut i = 1;
                while i < col.len() {
                    if col[i] == col[i-1] {
                        return Err(StrictError::NonUniquePrimaryKey(i))
                    }
                    i += 1;
                }
            },
            DbVec::Texts(col) => {
                let mut i = 1;
                while i < col.len() {
                    if col[i] == col[i-1] {
                        return Err(StrictError::NonUniquePrimaryKey(i))
                    }
                    i += 1;
                }
            },
            DbVec::Floats(_) => unreachable!("Should never have a float primary key"),
        }

        let mut output = ColumnTable { 
            metadata: Metadata::new(created_by), 
            name: KeyString::from(table_name), 
            header: header, 
            table: result 
        };
        output.sort();
        Ok(
            output
        )
    }

    pub fn update_from_csv(&mut self, input_csv: &str) -> Result<(), StrictError> {

        let insert_table = ColumnTable::from_csv_string(input_csv, "insert", "system")?;

        self.update(&insert_table)?;

        Ok(())
    }

    pub fn get_primary_key_col_index(&self) -> usize {
        let mut self_primary_key_index = 0;
        
        let mut i = 0;
        for item in &self.header {
            if item.key == TableKey::Primary {
                self_primary_key_index = i;
            }
            i+= 1;
        }

        self_primary_key_index
    }

    pub fn update(&mut self, other_table: &ColumnTable) -> Result<(), StrictError> {

        if self.header != other_table.header {
            return Err(StrictError::Update("Headers don't match".to_owned()));
        }

        let self_primary_key_index = self.get_primary_key_col_index();

        let minlen = std::cmp::min(self.table.len(), other_table.table.len());

        let record_vec: Vec<u8>;
        match &mut self.table[self_primary_key_index] {
            DbVec::Ints(col) => {
                match &other_table.table[self_primary_key_index] {
                    DbVec::Ints(other_col) => {
                        (*col, record_vec) = merge_sorted(col, other_col);
                    },
                    _ => unreachable!("Should always have the same primary key column")
                }
            },
            DbVec::Texts(col) => {
                match &other_table.table[self_primary_key_index] {
                    DbVec::Texts(other_col) => {
                        (*col, record_vec) = merge_sorted(col, other_col);
                    },
                    _ => unreachable!("Should always have the same primary key column")
                }
            },
            DbVec::Floats(_) => unreachable!("Should never have a float primary key column"),
        }
        for i in 0..minlen {

            if i == self_primary_key_index {
                continue;
            }

            match &mut self.table[i] {
                DbVec::Ints(col) => {
                    match &other_table.table[i] {
                        DbVec::Ints(other_col) => {
                            *col = merge_in_order(col, other_col, &record_vec);
                        },
                        _ => unreachable!("Should always have the same type column")
                    }
                },
                DbVec::Texts(col) => {
                    match &other_table.table[i] {
                        DbVec::Texts(other_col) => {
                            *col = merge_in_order(col, other_col, &record_vec);
                        },
                        _ => unreachable!("Should always have the same type column")
                    }
                },
                DbVec::Floats(col) => {
                    match &other_table.table[i] {
                        DbVec::Floats(other_col) => {
                            *col = merge_in_order(col, other_col, &record_vec);
                        },
                        _ => unreachable!("Should always have the same type column")
                    }
                },
            }
        }
        Ok(())
    }

    pub fn len(&self) -> usize {
        let len: usize;
        match &self.table[0] {
            DbVec::Floats(col) => len = col.len(),
            DbVec::Ints(col) => len = col.len(),
            DbVec::Texts(col) => len = col.len(),
        }
        len
    }

    pub fn sort(&mut self) {

        let len = self.len();

        let mut indexer: Vec<usize> = (0..len).collect();
        
        let primary_index = self.get_primary_key_col_index();

        let vec = &mut self.table[primary_index];
        match vec {
            DbVec::Ints(col) => {
                indexer.sort_unstable_by_key(|&i|col[i] );
            },
            DbVec::Texts(col) => {
                indexer.sort_unstable_by_key(|&i|&col[i] );
            },
            DbVec::Floats(_) => {
                unreachable!("There should never be a float primary key");
            },
        }

        self.table.iter_mut().for_each(|vec| {
            match vec {
            DbVec::Floats(col) => {
                // println!("float!");
                rearrange_by_index(col, &indexer);
            },
            DbVec::Ints(col) => {
                // println!("int!");
                rearrange_by_index(col, &indexer);
            },
            DbVec::Texts(col) => {
                // println!("text!");
                rearrange_by_index(col, &indexer);
            },
            }
        });
    }

    pub fn get_line(&self, index: usize) -> Result<String, StrictError> {
        
        if index > self.len() {
            return Err(StrictError::Query("Index larger than data".to_owned()))
        }

        let mut output = String::new();
        for v in &self.table {
            match v {
                DbVec::Floats(col) => {
                    let item = col[index];
                    output.push_str(&item.to_string());
                },
                DbVec::Ints(col) => {
                    let item = col[index];
                    output.push_str(&item.to_string());
                },
                DbVec::Texts(col) => {
                    let item = &col[index];
                    output.push_str(item);
                },
            }

            output.push(';');
        }
        output.pop();

        Ok(output)
    }

    pub fn query_list(&self, mut key_list: Vec<&str>) -> Result<String, StrictError> {
        let mut printer = String::new();
        let primary_index = self.get_primary_key_col_index();
        key_list.sort();

        let mut indexes = Vec::new();
        for item in key_list {
            match &self.table[primary_index] {
                DbVec::Floats(_) => return Err(StrictError::FloatPrimaryKey),
                DbVec::Ints(col) => {
                    let key: i32;
                    match item.parse::<i32>() {
                        Ok(num) => key = num,
                        Err(_) => continue,
                    };
                    let index: usize;
                    match col.binary_search(&key) {
                        Ok(num) => index = num,
                        Err(_) => continue,
                    } 
                    indexes.push(index);
                },
                DbVec::Texts(col) => {
                    let index: usize;
                    match col.binary_search(&KeyString::from(item)) {
                        Ok(num) => index = num,
                        Err(_) => continue,
                    } 
                    indexes.push(index);
                }
            }
        }

        for index in indexes {
            for v in &self.table {
                match v {
                    DbVec::Floats(col) => printer.push_str(&col[index].to_string()),
                    DbVec::Ints(col) => printer.push_str(&col[index].to_string()),
                    DbVec::Texts(col) => printer.push_str(&col[index]),
                }
                printer.push(';');
            }
            printer.pop();
            printer.push('\n');
        }
        printer.pop();
        
        Ok(printer)
    }

    pub fn query_range(&self, range: (&str, &str)) -> Result<String, StrictError> {
        let mut printer = String::new();

        if range.1 < range.0 {
            return Err(StrictError::Empty)
        }

        if range.0 == range.1 {
            return self.query(range.0);
        }

        let primary_index = self.get_primary_key_col_index();

        let mut indexes: [usize;2] = [0,0];
        match &self.table[primary_index] {
            DbVec::Floats(_) => return Err(StrictError::FloatPrimaryKey),
            DbVec::Ints(col) => {
                let key = match range.0.parse::<i32>() {
                    Ok(num) => num,
                    Err(_) => return Err(StrictError::Empty),
                };
                let index: usize = col.partition_point(|n| n < &key);
                indexes[0] = index;

                if range.1 == "" {
                    indexes[1] = col.len();
                } else {
                    let key2 = match range.1.parse::<i32>() {
                        Ok(num) => num,
                        Err(_) => return Err(StrictError::WrongKey),
                    };
                    // // println!("key2: {}", key2);
                    let index: usize = col.partition_point(|n| n < &key2);
                    if col[index] == key2 {
                        indexes[1] = index;
                    } else {
                        indexes[1] = index - 1;
                    }
                }

            },
            DbVec::Texts(col) => {
                let index: usize = col.partition_point(|n| n < &KeyString::from(range.0));
                indexes[0] = index;

                if range.1 == "" {
                    indexes[1] = col.len();
                }

                let index: usize = col.partition_point(|n| n < &KeyString::from(range.1));

                if col[index] == range.1 {
                    indexes[1] = index;
                } else {
                    indexes[1] = index - 1;
                }

                indexes[1] = index;
            }
        }

        let mut i = indexes[0];
        while i <= indexes[1] {
            for v in &self.table {
                match v {
                    DbVec::Floats(col) => printer.push_str(&col[i].to_string()),
                    DbVec::Ints(col) => printer.push_str(&col[i].to_string()),
                    DbVec::Texts(col) => printer.push_str(&col[i]),
                }
                printer.push(';');
            }
            printer.pop();
            printer.push('\n');
            i += 1;
        }
        printer.pop();

        Ok(printer)
    }

    pub fn query(&self, query: &str) -> Result<String, StrictError> {
        self.query_list(Vec::from([query]))
    }

    pub fn complex_query(&self, mut query: Query) -> Result<String, StrictError> {

        let pk_index = self.get_primary_key_col_index();

        let mut indexes = Vec::new();

        match &self.table[pk_index] {
            DbVec::Ints(col) => {
                match query.primary_keys {
                    RangeOrListorAll::List(list) => {

                        let mut int_list = Vec::with_capacity(list.len());
                        for item in &list {
                            let temp = match item.parse::<i32>() {
                                Ok(x) => x,
                                Err(_) => return Err(StrictError::Query(item.to_string())),
                            };
                            int_list.push(temp);
                        }
                        int_list.sort();

                        for item in int_list {
                            match col.binary_search(&item) {
                                Ok(i) => indexes.push(i),
                                Err(_) => break,
                            };
                        }
                    },
                    RangeOrListorAll::Range(range) => {
                        let range0 = match range[0].parse::<i32>() {
                            Ok(x) => x,
                            Err(e) => return Err(StrictError::Query(format!("Could not parse '{}' as a i32", range[0]))),
                        };
                        let range1 = match range[1].parse::<i32>() {
                            Ok(x) => x,
                            Err(e) => return Err(StrictError::Query(format!("Could not parse '{}' as a i32", range[1]))),
                        };
                        let start = col.binary_search(&range0).unwrap_or(0);
                        let stop = col.binary_search(&range1).unwrap_or(col.len());

                        indexes = (start..stop).collect();
                        
                    },
                    RangeOrListorAll::All => indexes = (0..col.len()).collect(),
                };
            },
            DbVec::Texts(col) => {
                match query.primary_keys {
                    RangeOrListorAll::List(mut list) => {
                        
                        list.sort();

                        for item in list {
                            match col.binary_search(&item) {
                                Ok(i) => indexes.push(i),
                                Err(_) => break,
                            };
                        }
                    },
                    RangeOrListorAll::Range(range) => {
                        
                        let start = col.binary_search(&range[0]).unwrap_or(0);
                        let stop = col.binary_search(&range[1]).unwrap_or(col.len());
                        
                        indexes = (start..stop).collect();
                        
                    },
                    RangeOrListorAll::All => indexes = (0..col.len()).collect(),
                };
            },
            DbVec::Floats(_) => unreachable!("Should never have a float primary key"),
        }

        let mut keepers = Vec::with_capacity(indexes.len());

        for index in indexes {
            for condition in &query.conditions {
                let col_index = match self.header.iter().position(|x| x.name == condition.attribute) {
                    Some(x) => x,
                    None => return Err(StrictError::Query(format!("Queried table does not have attribute {}", condition.attribute)))
                };

                match condition.test {

                    Test::Contains => {
                        match &self.table[col_index] {
                            DbVec::Floats(_) => {
                                return Err(StrictError::Query(format!("Cannot apply a 'contains' test on floats")))
                            },
                            DbVec::Ints(_) => {
                                return Err(StrictError::Query(format!("Cannot apply a 'contains' test on ints")))
                            },
                            DbVec::Texts(col) => {
                                let bar = &condition.bar;
                                if col[index].contains(&bar.to_string()) {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                        }
                    },

                    Test::Ends => {
                        match &self.table[col_index] {
                            DbVec::Floats(_) => {
                                return Err(StrictError::Query(format!("Cannot apply a 'ends' test on floats")))
                            },
                            DbVec::Ints(_) => {
                                return Err(StrictError::Query(format!("Cannot apply a 'ends' test on ints")))
                            },
                            DbVec::Texts(col) => {
                                let bar = &condition.bar;
                                if col[index].ends_with(&bar.to_string()) {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                        }
                    },

                    Test::Equals => {
                        match &self.table[col_index] {
                            DbVec::Floats(col) => {
                                let bar = match condition.bar.parse::<f32>() {
                                    Ok(n) => n,
                                    Err(_) => return Err(StrictError::Query(format!("Could not parse '{}' as an f32", condition.bar).to_owned()))
                                };
                                if col[index] == bar {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                            DbVec::Ints(col) => {
                                let bar = match condition.bar.parse::<i32>() {
                                    Ok(n) => n,
                                    Err(_) => return Err(StrictError::Query(format!("Could not parse '{}' as an i32", condition.bar).to_owned()))
                                };
                                if col[index] == bar {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                            DbVec::Texts(col) => {
                                let bar = &condition.bar;
                                if col[index] == *bar {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                        }
                    },

                    Test::Greater => {
                        match &self.table[col_index] {
                            DbVec::Floats(col) => {
                                let bar = match condition.bar.parse::<f32>() {
                                    Ok(n) => n,
                                    Err(_) => return Err(StrictError::Query(format!("Could not parse '{}' as an f32", condition.bar).to_owned()))
                                };
                                if col[index] > bar {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                            DbVec::Ints(col) => {
                                let bar = match condition.bar.parse::<i32>() {
                                    Ok(n) => n,
                                    Err(_) => return Err(StrictError::Query(format!("Could not parse '{}' as an i32", condition.bar).to_owned()))
                                };
                                if col[index] > bar {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                            DbVec::Texts(col) => {
                                let bar = &condition.bar;
                                if col[index] > *bar {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                        }
                    },

                    Test::Less => {match &self.table[col_index] {
                        DbVec::Floats(col) => {
                            let bar = match condition.bar.parse::<f32>() {
                                Ok(n) => n,
                                Err(_) => return Err(StrictError::Query(format!("Could not parse '{}' as an f32", condition.bar).to_owned()))
                            };
                            if col[index] < bar {
                                keepers.push(index);
                            } else {
                                continue;
                            }
                        },
                        DbVec::Ints(col) => {
                            let bar = match condition.bar.parse::<i32>() {
                                Ok(n) => n,
                                Err(_) => return Err(StrictError::Query(format!("Could not parse '{}' as an i32", condition.bar).to_owned()))
                            };
                            if col[index] < bar {
                                keepers.push(index);
                            } else {
                                continue;
                            }
                        },
                        DbVec::Texts(col) => {
                            let bar = &condition.bar;
                            if col[index] < *bar {
                                keepers.push(index);
                            } else {
                                continue;
                            }
                        },
                    }},

                    Test::Starts => {
                        match &self.table[col_index] {
                            DbVec::Floats(_) => {
                                return Err(StrictError::Query(format!("Cannot apply a 'ends' test on floats")))
                            },
                            DbVec::Ints(_) => {
                                return Err(StrictError::Query(format!("Cannot apply a 'ends' test on ints")))
                            },
                            DbVec::Texts(col) => {
                                let bar = &condition.bar;
                                if col[index].starts_with(&bar.to_string()) {
                                    keepers.push(index);
                                } else {
                                    continue;
                                }
                            },
                        }
                    },
                }; // match condition.test
            } // for condition in query.conditions {
        }  // Generating keepers

        let mut output = String::new();
        for keeper in keepers {
            assert!(keeper < self.len());
            output.push_str(&self.get_line(keeper).unwrap()); // should be safe to unwrap since the keepers are generated from self
            output.push('\n');
        }

        Ok(output)
    }

    pub fn delete_range(&mut self, range: (&str, &str)) -> Result<(), StrictError> {

        // UP TO BUT NOT INCLUDING!!!!

        if range.1 < range.0 {
            return Err(StrictError::Empty)
        }

        if range.0 == range.1 {
            return self.delete(range.0);
        }

        let primary_index = self.get_primary_key_col_index();

        let mut indexes: [usize;2] = [0,0];
        match &self.table[primary_index] {
            DbVec::Floats(_) => return Err(StrictError::FloatPrimaryKey),
            DbVec::Ints(col) => {
                let key = match range.0.parse::<i32>() {
                    Ok(num) => num,
                    Err(_) => return Err(StrictError::Empty),
                };
                let index: usize = col.partition_point(|n| *n < key);
                indexes[0] = index;

                if range.1 == "" {
                    indexes[1] = col.len();
                } else {
                    let key2 = match range.1.parse::<i32>() {
                        Ok(num) => num,
                        Err(_) => return Err(StrictError::WrongKey),
                    };
                    // // println!("key2: {}", key2);
                    let index: usize = col.partition_point(|n| n < &key2);
                    indexes[1] = index;
                }

            },
            DbVec::Texts(col) => {
                let index: usize = col.partition_point(|n| n < &KeyString::from(range.0));
                indexes[0] = index;

                if range.1 == "" {
                    indexes[1] = col.len();
                }

                let index: usize = col.partition_point(|n| n < &KeyString::from(range.1));

                if col[index] == range.1 {
                    indexes[1] = index;
                } else {
                    indexes[1] = index - 1;
                }

                indexes[1] = index;
            }
        }

        for col in self.table.iter_mut() {
            match col {
                DbVec::Floats(v) => {
                    v.drain(indexes[0]..indexes[1]);
                },
                DbVec::Ints(v) => {
                    v.drain(indexes[0]..indexes[1]);
                },
                DbVec::Texts(v) => {
                    v.drain(indexes[0]..indexes[1]);
                },
            };
        }

        Ok(())
    }

    pub fn delete_list(&mut self, mut key_list: Vec<&str>) -> Result<(), StrictError> {
        let primary_index = self.get_primary_key_col_index();
        key_list.sort();

        let mut indexes = Vec::new();
        for item in key_list {
            match &self.table[primary_index] {
                DbVec::Floats(_) => return Err(StrictError::FloatPrimaryKey),
                DbVec::Ints(col) => {
                    let key: i32;
                    match item.parse::<i32>() {
                        Ok(num) => key = num,
                        Err(_) => continue,
                    };
                    let index: usize;
                    match col.binary_search(&key) {
                        Ok(num) => index = num,
                        Err(_) => continue,
                    } 
                    indexes.push(index);
                },
                DbVec::Texts(col) => {
                    let index: usize;
                    match col.binary_search(&KeyString::from(item)) {
                        Ok(num) => index = num,
                        Err(_) => continue,
                    } 
                    indexes.push(index);
                }
            }
        }

        let imut = self.table.iter_mut();
        for col in imut {
            match col {
                DbVec::Floats(v) => {
                    remove_indices(v, &indexes);
                }
                DbVec::Ints(v) => {
                    remove_indices(v, &indexes);
                }
                DbVec::Texts(v) => {
                    remove_indices(v, &indexes);
                }
            };
        }

        Ok(())
    }

    fn delete(&mut self, query: &str) -> Result<(), StrictError> {
        self.delete_list(Vec::from([query]))
    }

    pub fn save_to_disk_csv(&self, path: &str) -> Result<(), StrictError> {
        let file_name = &self.name;

        let metadata = &self.metadata.to_string();

        let table = &self.to_string();


        let mut table_file = match std::fs::File::create(format!("{}raw_tables/{}",path, file_name)) {
            Ok(f) => f,
            Err(e) => return Err(StrictError::Io(e.kind())),
        };

        let mut meta_file = match std::fs::File::create(format!("{}raw_tables-metadata/{}",path, file_name)) {
            Ok(f) => f,
            Err(e) => return Err(StrictError::Io(e.kind())),
        };

        match table_file.write_all(table.as_bytes()) {
            Ok(_) => (),
            Err(e) => println!("Error while writing to disk. Error was:\n{}", e),
        };
        match meta_file.write_all(metadata.as_bytes()) {
            Ok(_) => (),
            Err(e) => println!("Error while writing to disk. Error was:\n{}", e),
        };

        Ok(())
    }

    #[cfg(target_feature="sse")]
    pub fn write_to_raw_binary(&self) -> Vec<u8> {

        let mut total_bytes = 0;
        for item in &self.header {
            total_bytes += item.name.as_bytes().len() + 6;
        }
        
        let length = self.len();
        // println!("length: {}", length);
        for item in &self.table {
            match item {
                DbVec::Texts(col) => {
                    for thing in col {
                        total_bytes += thing.as_bytes().len() + 1;
                    }
                },
                _ => {
                    total_bytes += length * 4 + 1;
                }
            };
        }

        let mut output: Vec<u8> = Vec::with_capacity(total_bytes);

        for item in &self.header {
            let kind = match item.kind {
                DbType::Int => b'i',
                DbType::Float => b'f',
                DbType::Text => b't',
            };
            output.push(kind);
            let name = item.name.as_bytes();
            output.extend_from_slice(name);
            match &item.key {
                TableKey::Primary => output.push(b'P'),
                TableKey::None => output.push(b'N'),
                TableKey::Foreign => output.push(b'F'),
            }
            output.push(b';');

        }
        output.pop();
        output.push(b'\n');
        output.extend_from_slice(&(self.len() as u32).to_le_bytes());

        for column in &self.table {
            match &column {
                DbVec::Floats(col) => {
                    for item in col {
                        output.extend_from_slice(&item.to_le_bytes());
                    }
                },
                &DbVec::Ints(col) => {
                    for item in col {
                        // println!("item: {}", item);
                        output.extend_from_slice(&item.to_le_bytes());
                    }
                },
                DbVec::Texts(col) => {
                    for item in col {
                        output.extend_from_slice(&item.as_bytes());
                        output.push(b';');
                    }
                },
            };
        }
        output
    }

    pub fn read_raw_binary(binary: &[u8]) -> Result<ColumnTable, StrictError> {

        let mut binter = binary.iter();
        let first_newline = binter.position(|n| *n == b'\n').unwrap();
        let bin_header = &binary[0..first_newline];
        let bin_length = &binary[first_newline + 1..first_newline + 5];
        let bin_body = &binary[first_newline + 5..];

        let bin_length = u32_from_le_slice(bin_length) as usize;
        // println!("bin_length: {}", bin_length);

        let mut header = Vec::new();

        for item in bin_header.split(|n| n == &b';') {
            let kind = match item.first().unwrap() {
                b'i' => DbType::Int,
                b'f' => DbType::Float,
                b't' => DbType::Text,
                x => unreachable!("the first byte of a header item should never be {:x?}", x),
            };
            let key = match item.last().unwrap() {
                b'P' => TableKey::Primary,
                b'N' => TableKey::None,
                b'F' => TableKey::Foreign,
                x => unreachable!("The last byte of a header item should never be {:x?}", x),
            };
            let name = match std::str::from_utf8(&item[1..item.len()-1]) {
                Ok(n) => n,
                Err(e) => return Err(StrictError::BinaryRead(format!("Utf8 error while parsing header item name.\nError body: {}", e))),
            };
            let header_item = HeaderItem{
                kind: kind,
                name: KeyString::from(name),
                key: key,
            };

            header.push(header_item);
        }

        // dbg!(&header);

        let mut table: Vec<DbVec> = Vec::with_capacity(header.len());

        let mut total = 0;
        let mut index = 0;
        while index < header.len() {
            // println!("total: {}", total);
            match header[index].kind {
                DbType::Int => {
                    let blob = &bin_body[total..total+(bin_length*4)];
                    // println!("blob: {:x?}", blob);
                    let v = blob.chunks(4).map(|n| i32_from_le_slice(n)).collect();
                    // for x in &v {
                        // println!("x: {}", x);
                    // }
                    total += bin_length*4;
                    index += 1;
                    table.push(DbVec::Ints(v));
                },
                DbType::Float => {
                    let blob = &bin_body[total..total+(bin_length*4)];
                    let v: Vec<f32> = blob.chunks(4).map(|n| f32_from_le_slice(n)).collect();
                    total += bin_length*4;
                    index += 1;
                    table.push(DbVec::Floats(v));

                },
                DbType::Text => {
                    let mut pos = 0;
                    let mut v = Vec::with_capacity(bin_length);
                    let mut strbuf = Vec::with_capacity(24);
                    while pos < bin_length {
                        if bin_body[total] == b';' {
                            match String::from_utf8(strbuf.clone()) {
                                Ok(s) => v.push(KeyString::from(s)),
                                Err(e) => return Err(StrictError::BinaryRead(format!("Invalid utf-8 in text column.\nError body: {}", e))),
                            };
                            strbuf.clear();
                            pos += 1;
                            total += 1;
                            continue
                        }
                        
                        strbuf.push(bin_body[total]);

                        total += 1;    
                    }
                    table.push(DbVec::Texts(v));
                    index += 1;
                },
            }
        }

        Ok(ColumnTable {
            metadata: Metadata::new("test"),
            name: KeyString::from("test"),
            header: header,
            table: table,
        })   
    }
}



#[inline]
fn rearrange_by_index<T: Clone>(col: &mut Vec<T>, indexer: &[usize]) {
    let mut temp = Vec::with_capacity(col.len());
    for i in 0..col.len() {
        temp.push(col[indexer[i]].clone());
    }
    *col = temp;
} 


fn remove_indices<T>(vec: &mut Vec<T>, indices: &[usize]) {
    let indices_set: HashSet<_> = indices.iter().cloned().collect();
    let mut shift = 0;

    for i in 0..vec.len() {
        if indices_set.contains(&i) {
            shift += 1;
        } else if shift > 0 {
            vec.swap(i - shift, i);
        }
    }

    vec.truncate(vec.len() - shift);
}


fn merge_sorted<T: Ord + Clone + Display + Debug>(one: &[T], two: &[T]) -> (Vec<T>, Vec<u8>) {
    let mut output: Vec<T> = Vec::with_capacity(one.len() + two.len());
    let mut record_vec: Vec<u8> = Vec::with_capacity(one.len() + two.len());
    let mut one_pointer = 0;
    let mut two_pointer = 0;

    // println!("RUNNING merge_sorted()!!!--------------------------------");
    loop {
        // println!("one[{one_pointer}]: {}\t\ttwo[{two_pointer}]: {}", one[one_pointer], two[two_pointer]);

        match one[one_pointer].cmp(&two[two_pointer]) {
            std::cmp::Ordering::Less => {
                output.push(one[one_pointer].clone());
                record_vec.push(1);
                one_pointer += 1;
            },
            std::cmp::Ordering::Equal => {
                output.push(two[two_pointer].clone());
                record_vec.push(3);
                two_pointer += 1;
                one_pointer += 1;
            },
            std::cmp::Ordering::Greater => {
                output.push(two[two_pointer].clone());
                record_vec.push(2);
                two_pointer += 1;
            }
        }
        // if one[one_pointer] < two[two_pointer] {
        //     new_vec.push(one[one_pointer].clone());
        //     record_vec.push(1);
        //     one_pointer += 1;
        // } else if one[one_pointer] > two[two_pointer] {
        //     new_vec.push(two[two_pointer].clone());
        //     record_vec.push(2);
        //     two_pointer += 1;
        // } else if one[one_pointer] == two[two_pointer]{
        //     new_vec.push(two[two_pointer].clone());
        //     record_vec.push(3);
        //     two_pointer += 1;
        //     one_pointer += 1;
        // } else {
        //     unreachable!();
        // }
        if one_pointer >= one.len() {
            output.extend_from_slice(&two[two_pointer..two.len()]);
            while two_pointer < two.len() {
                record_vec.push(2);
                two_pointer += 1;
            }
            break;
        } else if two_pointer >= two.len() {
            output.extend_from_slice(&one[one_pointer..one.len()]);
            while one_pointer < one.len() {
                record_vec.push(1);
                one_pointer += 1;
            }
            
            break;
        }
    }
    // println!("new_vec.len(): {}\nnew_vec\n{:?}", new_vec.len(), new_vec);
    // println!("record_vec.len(): {}\nrecord_vec: \n{:?}", record_vec.len(), record_vec);
    // println!("merge_sorted() FINISHED !!!!!!######################################");
    // println!("\n\n");

    (output, record_vec)
}

fn merge_in_order<T: Clone + Display>(one: &[T], two: &[T], record_vec: &[u8]) -> Vec<T> {
    let mut output = Vec::with_capacity(one.len() + two.len());
    let mut one_pointer = 0;
    let mut two_pointer = 0;
    // // println!("record_vec.len(): {}", record_vec.len());
    // // println!("one.len():   {}", one.len());
    // // println!("two.len():   {}", two.len());
    // println!("record_vec: {:?}", record_vec);
    for index in record_vec {
        // //println!("one_p: {}\tone[one_p]: {}\ntwo_p: {}\ttwo[two_p]: {}", one_pointer, one[one_pointer], two_pointer, two[two_pointer]);
        match index {
            1 => {
                output.push(one[one_pointer].clone());
                one_pointer += 1;
            },
            2 => {
                output.push(two[two_pointer].clone());
                two_pointer += 1;
            },
            3 => {
                output.push(two[two_pointer].clone());
                one_pointer += 1;
                two_pointer += 1;
            }
            _ => unreachable!("Should always be 1, 2, or 3"),
        }
    }
    output
}


#[derive(PartialEq, Clone, Debug)]
pub struct Value {
    pub body: Vec<u8>,
    pub metadata: Metadata,
}

impl Value {
    pub fn new(creator: &str, body: &[u8]) -> Value {
        let mut body = Vec::from(body);
        body.shrink_to_fit();
        Value {
            body: body,
            metadata: Metadata::new(creator),
        }
    }

    pub fn save_to_disk_raw(&self, key: &str, path: &str) -> Result<(), StrictError> {
        let file_name = key;

        let metadata = &self.metadata.to_string();

        let mut value_file = match std::fs::File::create(&format!("{}key_value/{}",path, file_name)) {
            Ok(f) => f,
            Err(e) => return Err(StrictError::Io(e.kind())),
        };

        let mut meta_file = match std::fs::File::create(&format!("{}key_value-metadata/{}",path, file_name)) {
            Ok(f) => f,
            Err(e) => return Err(StrictError::Io(e.kind())),
        };

        match value_file.write_all(&self.body) {
            Ok(_) => (),
            Err(e) => println!("Error while writing to disk. Error was:\n{}", e),
        };
        match meta_file.write_all(metadata.as_bytes()) {
            Ok(_) => (),
            Err(e) => println!("Error while writing to disk. Error was:\n{}", e),
        };

        Ok(())
    }
}


#[cfg(test)]
mod tests {
    #![allow(unused)]

    use rand::Rng;

    use super::*;

    #[test]
    fn test_columntable_from_to_string() {
        let input = "vnr,i-P;heiti,t-N;magn,i-N\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        // println!("t: {}", t.to_string());
        assert_eq!(input, t.to_string());
    }

    #[test]
    fn test_columntable_combine_sorted() {
        let mut i = 0;
        let mut printer = String::from("vnr,text-P;heiti,text-N;magn,int-N;lengd,float-N\n");
        let mut printer2 = String::from("vnr,text-P;heiti,text-N;magn,int-N;lengd,float-N\n");
        let mut printer22 = String::new();
        loop {
            if i > 50 {
                break;
            }
            let random_number: i32 = rand::thread_rng().gen();
            let random_float: f32 = rand::thread_rng().gen();
            let mut random_string = String::new();
            for _ in 0..8 {
                random_string.push(rand::thread_rng().gen_range(97..122) as u8 as char);
            }
            printer.push_str(&format!("a{i};{random_string};{random_number};{random_float}\n"));
            printer2.push_str(&format!("b{i};{random_string};{random_number};{random_float}\n"));
            printer22.push_str(&format!("b{i};{random_string};{random_number};{random_float}\n"));
            
            i+= 1;
        }

        let mut printer3 = String::new();
        printer3.push_str(&printer);
        printer3.push_str(&printer22);
        // // println!("{}", printer3);

        let mut a = ColumnTable::from_csv_string(&printer, "a", "test").unwrap();
        let b = ColumnTable::from_csv_string(&printer2, "b", "test").unwrap();
        a.update(&b).unwrap();
        let c = ColumnTable::from_csv_string(&printer3, "c", "test").unwrap();

        assert_eq!(a.to_string(), c.to_string());
    }


    #[test]
    fn test_columntable_combine_unsorted_csv() {
        let unsorted1 = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_unsorted.csv")).unwrap();
        let unsorted2 = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets2_unsorted.csv")).unwrap();
        let sorted_combined = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();

        let mut a = ColumnTable::from_csv_string(&unsorted1, "a", "test").unwrap();
        let b = ColumnTable::from_csv_string(&unsorted2, "b", "test").unwrap();
        let c = ColumnTable::from_csv_string(&sorted_combined, "c", "test").unwrap();
        a.update(&b).unwrap();
        let mut file = std::fs::File::create("combined.csv").unwrap();
        file.write_all(a.to_string().as_bytes());

        assert_eq!(a.to_string(), c.to_string());
    }


    #[test]
    fn test_columntable_query_list() {
        let input = "vnr,i-P;heiti,t-N;magn,i-N\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        // println!("t: {}", t.to_string());
        let x = t.query_list(Vec::from(["113035"])).unwrap();
        assert_eq!(x, "113035;undirlegg;200");
    }

    #[test]
    fn test_columntable_query_single() {
        let input = "vnr,i-P;heiti,t-N;magn,i-N\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        // println!("t: {}", t.to_string());
        let x = t.query("113035").unwrap();
        assert_eq!(x, "113035;undirlegg;200");
    }

    #[test]
    fn test_columntable_query_range() {
        let input = "vnr,i-P;heiti,t-N;magn,i-N\n113035;undirlegg;200\n113050;annad undirlegg;500\n18572054;flísalím;42\n113446;harlech;250";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        let x = t.query_range(("113035", "113060")).unwrap();

        assert_eq!(x, "113035;undirlegg;200\n113050;annad undirlegg;500")
    }

    #[test]
    fn test_raw_binary() {
        // let input = "vnr,i-P;heiti,t;magn,i\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let input = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
        let t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();
        let bint_t = t.write_to_raw_binary();
        let string_t = t.to_string();
        println!("bin_t lent: {}", bint_t.len());
        println!("string_t lent: {}", string_t.len());
        let translated_t = ColumnTable::read_raw_binary(&bint_t).unwrap();
        
        let string_transl_t = translated_t.to_string();
        assert_eq!(string_t, string_transl_t);
    }


    // TEST QUERIES ###############################################################################################################################################################################

    #[test]
    fn test_delete_range() {
        let input = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
        let test_input = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted_test_range.csv")).unwrap();
        let mut t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();
        
        let test_t = ColumnTable::from_csv_string(&test_input, "test", "test").unwrap();
        // println!("{}", t);
        t.delete_range(("262", "673"));
        // println!("{}", t);
        assert_eq!(t.to_string(), test_t.to_string());
    }

    #[test]
    fn test_delete_list() {
        let input = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
        let test_input = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted_test_range.csv")).unwrap();
        let mut t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();
        
        let test_t = ColumnTable::from_csv_string(&test_input, "test", "test").unwrap();
        // println!("{}", t);
        t.delete_list(vec!["262","264","353","544","656"]);
        // println!("{}", t);
        assert_eq!(t.to_string(), test_t.to_string());
    }

    #[test]
    fn test_complex_query() {
        let input = std::fs::read_to_string("test_files/test_csv_from_google_sheets_combined_sorted_test_range.csv").unwrap();
        let mut t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();
        let query = Query {
            primary_keys: RangeOrListorAll::List(vec![KeyString::from("178"), KeyString::from("673"), KeyString::from("803")]),
            conditions: vec![
                Condition {
                    attribute: KeyString::from("heiti"),
                    test: Test::Equals,
                    bar: KeyString::from("name39"),
                },
            ]
        };

        let output = t.complex_query(query).unwrap();
        println!("output: {}", output);

        let query = Query {
            primary_keys: RangeOrListorAll::All,
            conditions: vec![
                Condition {
                    attribute: KeyString::from("magn"),
                    test: Test::Less,
                    bar: KeyString::from("50"),
                },
            ]
        };

        let output = t.complex_query(query).unwrap();
        println!("output: {}", output);

    }
}