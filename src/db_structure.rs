use std::{fmt::{self, Display, Debug}, io::Write, collections::BTreeMap};

use smartstring::{SmartString, LazyCompact};

use rayon::prelude::*;

use crate::networking_utilities::get_current_time;

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


#[derive(PartialEq, Clone, Debug)]
pub struct StrictTable {
    pub metadata: Metadata,
    pub name: String,
    pub header: Vec<DbEntry>,
    pub table: BTreeMap<String, Vec<DbEntry>>,
}

impl StrictTable {
    pub fn from_csv_string(s: &str, name: &str) -> Result<StrictTable, StrictError> {
        if s.len() < 1 {
            return Err(StrictError::Empty)
        }
        
        let mut header = Vec::new();

        {    /* Checking for unique header */
            let mut rownum = 0;
            for item in s.lines().next().unwrap().split(';') { // Safe since we know s is at least one line
                if rownum == 0 {
                    header.push(DbEntry::Text(item.to_owned()));
                    rownum += 1;
                    continue;
                }
                match item.parse::<i64>() {
                    Ok(value) => {
                        header.push(DbEntry::Int(value));
                        continue;
                    },
                    Err(_) => (),
                };
                
                match item.parse::<f64>() {
                    Ok(value) => {
                        header.push(DbEntry::Float(value));
                        continue;
                    },
                    Err(_) => (),
                };

                header.push(DbEntry::Text(item.to_owned()));
                
                
                rownum += 1;
            }
            let mut index1: usize = 0;
            let mut index2: usize = 0;
        
            loop {
                loop{
                    if index1 == header.len()-1 {
                        break;
                    } else if index1 == index2 {
                        index1 += 1;
                        continue;
                    } else if header[index1] == header[index2]{
                        return Err(StrictError::RepeatingHeader(index2, index1))
                    } else {
                        index1 += 1;
                    }
                }
                if index2 == header.len()-1 {
                    break;
                }
                index2 += 1;
            }
        }
        
        { // Checking that all rows have same number of items as header
            let mut linenum = 0;
            for line in s.lines() {
                if line.split(';').count() < header.len() {
                    return Err(StrictError::FewerItemsThanHeader(linenum));
                } else if line.split(';').count() > header.len() {
                    return Err(StrictError::MoreItemsThanHeader(linenum));
                } else {
                    linenum += 1;
                }
            }
        } // Finished checking
        
        // println!("one run");

        let mut output = BTreeMap::new();
        let mut rownum: usize = 0;
        for row in s.lines() {
            // This if statement is there to skip the header
            if rownum == 0 {
                rownum += 1;
                continue;
            }
            let mut temp = Vec::with_capacity(header.len());
            for col in row.split(";") {
                if col.len() == 0 { continue }
                if col.len() == 0 { 
                    temp.push(DbEntry::Empty);
                }
                if col.as_bytes()[0] == 0x30 {
                    temp.push(DbEntry::Text(col.to_owned()));
                    continue;
                }
                match col.parse::<i64>() {
                    Ok(value) => {
                        temp.push(DbEntry::Int(value));
                        continue;
                    },
                    Err(_) => (),
                };

                match col.parse::<f64>() {
                    Ok(value) => {
                        temp.push(DbEntry::Float(value));
                        continue;
                    },
                    Err(_) => (),
                };

                temp.push(DbEntry::Text(col.to_owned()));
                
                rownum += 1;
            }
            if temp.len() == 0 { continue }
            match &temp[0] {
                DbEntry::Text(value) => output.insert(value.to_owned(), temp),
                DbEntry::Int(value) => output.insert(value.to_string(), temp),
                _ => panic!("This is not supposed to happen"),
            };
        }


        let r = StrictTable {
            metadata: Metadata::new(name),
            header: header,
            name: String::from(name),
            table: output,
        };

        Ok(r)
    }


    pub fn to_csv_string(&self) -> String {
        let mut printer = String::from("");
        let map = &self.table;
        let header = &self.header;

        for item in header {
            match item {
                DbEntry::Float(value) => printer.push_str(&value.to_string()),
                DbEntry::Int(value) => printer.push_str(&value.to_string()),
                DbEntry::Text(value) => printer.push_str(value),
                DbEntry::Empty => (),
            }
            printer.push(';');
        }
        printer.pop().unwrap(); // safe since we know there is always a ; character there to be popped
        printer.push('\n');

        for (_, line) in map.iter() {
            for item in line {
                match item {
                    DbEntry::Float(value) => printer.push_str(&value.to_string()),
                    DbEntry::Int(value) => printer.push_str(&value.to_string()),
                    DbEntry::Text(value) => printer.push_str(value),
                    DbEntry::Empty => (),
                }
                printer.push(';')
            }
            printer.pop().unwrap();  // safe since we know there is always a ; character there to be popped
            printer.push('\n');
        }

        printer.pop();
        printer = printer.to_owned();
        printer
    }


    pub fn update(&mut self, csv: &str) -> Result<(), StrictError>{

        let mapped_csv = StrictTable::from_csv_string(csv, "update")?;

        if mapped_csv.header != self.header {
            {return Err(StrictError::Update("Headers don't match".to_owned()));}
        }

        for (key, value) in mapped_csv.table {
            self.table.insert(key, value);
        }

        self.metadata.last_access = get_current_time();
        self.metadata.times_accessed += 1;

        Ok(())
    }

    pub fn query_range(&self, range: (&str, &str)) -> Result<String, StrictError> {
        let min = range.0.to_owned();
        let max = range.1.to_owned();
        let output = self.table.range(min..=max);
        
        let mut printer = String::new();
        for (_, line) in output {
            for item in line {
                match item {
                    DbEntry::Float(value) => printer.push_str(&value.to_string()),
                    DbEntry::Int(value) => printer.push_str(&value.to_string()),
                    DbEntry::Text(value) => printer.push_str(value),
                    DbEntry::Empty => (),
                }
                printer.push(';')
            }
            printer.pop().unwrap();  // safe since we know there is always a ; character there to be popped
            printer.push('\n');
        }
        printer.pop();

        Ok(printer)
    }

    pub fn query_list(&self, key_list: Vec<&str>) -> Result<String, StrictError> {
        let mut printer = String::new();

        for item in key_list {
            for entry in &self.table[item] {
                match entry {
                    DbEntry::Float(value) => printer.push_str(&value.to_string()),
                    DbEntry::Int(value) => printer.push_str(&value.to_string()),
                    DbEntry::Text(value) => printer.push_str(value),
                    DbEntry::Empty => (),
                }
                printer.push(';')
            }
            printer.pop().unwrap(); // safe since we know there is always a ; character there to be popped
            printer.push('\n');

        }
        printer.pop();

        Ok(printer)
    }


    pub fn save_to_disk_raw(&self, path: &str) -> Result<(), StrictError> {
        let file_name = &self.name;

        let metadata = &self.metadata.to_string();

        let table = &self.to_csv_string();


        let mut table_file = match std::fs::File::create(&format!("{}raw_tables/{}",path, file_name)) {
            Ok(f) => f,
            Err(e) => return Err(StrictError::Io(e.kind())),
        };

        let mut meta_file = match std::fs::File::create(&format!("{}raw_tables-metadata/{}",path, file_name)) {
            Ok(f) => f,
            Err(e) => return Err(StrictError::Io(e.kind())),
        };

        table_file.write_all(table.as_bytes());
        meta_file.write_all(metadata.as_bytes());

        // pub struct Metadata {
        //     pub last_access: u64,
        //     pub times_accessed: u64,
        //     pub created_by: String,
        //     pub accessed_by: HashMap<String, Actions>,
        // }
        
        // pub struct Actions {
        //     pub uploaded: bool,
        //     pub downloaded: u64,
        //     pub updated: u64,
        //     pub queried: u64,
        // }
        
        // pub enum DbEntry {
        //     Int(i64),
        //     Float(f64),
        //     Text(String),
        //     Empty,
        // }
        
        // pub struct StrictTable {
        //     pub metadata: Metadata,
        //     pub name: String,
        //     pub header: Vec<DbEntry>,
        //     pub table: BTreeMap<String, Vec<DbEntry>>,
        // }


        Ok(())
    }

}


pub fn create_StrictTable_from_csv(s: &str, name: &str) -> Result<StrictTable, StrictError> {    

    StrictTable::from_csv_string(s, name)
    
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
    Ints{ name: KeyString, col: Vec<i64> },
    Floats{ name: KeyString, col: Vec<f64> },
    Texts{ name: KeyString, col: Vec<KeyString> },
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct HeaderItem {
    name: KeyString,
    kind: DbType,
    primary_key: bool,
}

impl HeaderItem {
    pub fn new() -> HeaderItem {
        HeaderItem{
            name: KeyString::from("default_name"),
            kind: DbType::Text,
            primary_key: false,
        }
    }
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
            if item.primary_key {
                printer.push_str("-p");
            }
            printer.push(';');
        }
        printer.pop();
        printer.push('\n');

        for i in 0..(self.len()) {

            for vec in &self.table {
                match vec {
                    DbVec::Floats { name: _, col } => {
                        // println!("float: col.len(): {}", col.len());
                        printer.push_str(&col[i].to_string());
                        printer.push_str(";");
                    },
                    DbVec::Ints { name: _, col } => {
                        // println!("int: col.len(): {}", col.len());
                        printer.push_str(&col[i].to_string());
                        printer.push_str(";");
                    },
                    DbVec::Texts { name: _, col } => {
                        // println!("text: col.len(): {}", col.len());
                        printer.push_str(&col[i]);
                        printer.push_str(";");
                    },
                }
            }
            printer.pop();
            printer.push_str("\n");
        }
        printer.pop();

        write!(f, "{}", printer)
    }
}

impl ColumnTable {
    pub fn from_csv_string(s: &str, table_name: &str, created_by: &str) -> Result<ColumnTable, StrictError> {

        if s.len() < 1 {
            return Err(StrictError::Empty)
        }

        let mut header = Vec::new();
        let mut primary_key_set = false;

        let first_line: Vec<&str> = s.split('\n').next().expect("confirmed to exist because of earlier check").split(';').collect();
        for item in first_line {
            let temp: Vec<&str> = item.split(',').collect();
            let mut header_item = HeaderItem::new();
            if temp.len() < 1 {
                return Err(StrictError::MissingType)
            } else if temp.len() == 1{
                header_item.kind = DbType::Text;
            } else if temp.len() > 2 {
                return Err(StrictError::TooManyHeaderFields)
            } else {
                header_item.name = KeyString::from(temp[0].trim());
                let t = temp[1].trim();
                match t {
                    "I" | "Int" | "int" | "i" => header_item.kind = DbType::Int,
                    "F" | "Float" | "float" | "f" => header_item.kind = DbType::Float,
                    "T" | "Text" | "text" | "t" => header_item.kind = DbType::Text,
                    "I-p" | "Int-p" | "int-p" | "i-p" => {
                        if primary_key_set {
                            return Err(StrictError::TooManyPrimaryKeys)
                        } else {
                            header_item.kind = DbType::Int;
                            header_item.primary_key = true;
                            primary_key_set = true;
                        }
                    },
                    "T-p" | "Text-p" | "text-p" | "t-p" => {
                        if primary_key_set {
                            return Err(StrictError::TooManyPrimaryKeys)
                        } else {
                            header_item.kind = DbType::Text;
                            header_item.primary_key = true;
                            primary_key_set = true;
                        }
                    },
                    "F-p" | "Float-p" | "float-p" | "f-p" => return Err(StrictError::FloatPrimaryKey),
                    _ => return Err(StrictError::WrongType),
                }
            }
            header.push(header_item);
        }

        if !primary_key_set {
            match header[0].kind {
                DbType::Int => header[0].primary_key = true,
                DbType::Text => header[0].primary_key = true,
                _ => unreachable!("Should already have a primary key or have been rejected for float primary key")
            };
        }

        let mut line_index = 0;
        let mut data: Vec<Vec<&str>> = Vec::new();
        for line in s.lines() {
            if line_index == 0 {
                line_index += 1;
                continue
            }
            let mut row_index = 0;
            for cell in line.split(';') {
                if line_index == 1 {
                    data.push(Vec::from([cell]));
                } else {
                    data[row_index].push(cell);
                }
                row_index += 1;

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
                        let temp = match cell.parse::<f64>() {
                            Ok(x) => x,
                            Err(_) => {
                                // println!("failed to parse: {}", cell);
                                return Err(StrictError::Parse(index))
                            },
                        };
                        outvec.push(temp);
                        index += 1;
                    }
                    DbVec::Floats { name: header[i].name.clone(), col: outvec }
                },
                DbType::Int => {
                    let mut outvec = Vec::with_capacity(col.len());
                    let mut index = 0;
                    for cell in col {
                        let temp = match cell.parse::<i64>() {
                            Ok(x) => x,
                            Err(_) => {
                                // println!("failed to parse: {}", cell);
                                return Err(StrictError::Parse(index))
                            },
                        };
                        outvec.push(temp);
                        index += 1;
                    }
                    DbVec::Ints { name: header[i].name.clone(), col: outvec }
                },
                DbType::Text => {
                    let mut outvec = Vec::with_capacity(col.len());
                    for cell in col {
                        outvec.push(KeyString::from(cell));
                    }
                    DbVec::Texts { name: header[i].name.clone(), col: outvec }
                },
            };
            
            result.push(db_vec);
            i += 1;
        }

        let mut primary_key_index = 0;
        for (index, item) in header.iter().enumerate() {
            if item.primary_key {
                primary_key_index = index;
            }
        };
        match &result[primary_key_index] {
            DbVec::Ints { name: _, col } => {
                let mut i = 1;
                while i < col.len() {
                    if col[i] == col[i-1] {
                        return Err(StrictError::NonUniquePrimaryKey(i))
                    }
                    i += 1;
                }
            },
            DbVec::Texts { name: _, col } => {
                let mut i = 1;
                while i < col.len() {
                    if col[i] == col[i-1] {
                        return Err(StrictError::NonUniquePrimaryKey(i))
                    }
                    i += 1;
                }
            },
            DbVec::Floats { name: _, col: _ } => unreachable!("Should never have a float primary key"),
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
            if item.primary_key {
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
            DbVec::Ints { name: _, col } => {
                match &other_table.table[self_primary_key_index] {
                    DbVec::Ints { name: _, col: other_col } => {
                        (*col, record_vec) = merge_sorted(col, other_col);
                    },
                    _ => unreachable!("Should always have the same primary key column")
                }
            },
            DbVec::Texts { name: _, col } => {
                match &other_table.table[self_primary_key_index] {
                    DbVec::Texts { name: _, col: other_col } => {
                        (*col, record_vec) = merge_sorted(col, other_col);
                    },
                    _ => unreachable!("Should always have the same primary key column")
                }
            },
            DbVec::Floats { name: _, col: _ } => unreachable!("Should never have a float primary key column"),
        }
        for i in 0..minlen {

            if i == self_primary_key_index {
                continue;
            }

            match &mut self.table[i] {
                DbVec::Ints { name: _, col } => {
                    match &other_table.table[i] {
                        DbVec::Ints { name: _, col: other_col } => {
                            *col = merge_in_order(col, other_col, &record_vec);
                        },
                        _ => unreachable!("Should always have the same type column")
                    }
                },
                DbVec::Texts { name: _, col } => {
                    match &other_table.table[i] {
                        DbVec::Texts { name: _, col: other_col } => {
                            *col = merge_in_order(col, other_col, &record_vec);
                        },
                        _ => unreachable!("Should always have the same type column")
                    }
                },
                DbVec::Floats { name: _, col } => {
                    match &other_table.table[i] {
                        DbVec::Floats { name: _, col: other_col } => {
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
            DbVec::Floats { name: _, col } => len = col.len(),
            DbVec::Ints { name: _, col } => len = col.len(),
            DbVec::Texts { name: _, col } => len = col.len(),
        }
        len
    }

    pub fn sort(&mut self) {

        let len = self.len();

        let mut indexer: Vec<usize> = (0..len).collect();
        
        let primary_index = self.get_primary_key_col_index();

        let vec = &mut self.table[primary_index];
        match vec {
            DbVec::Ints { name: _, col } => {
                indexer.sort_unstable_by_key(|&i|col[i] );
            },
            DbVec::Texts { name: _, col } => {
                indexer.sort_unstable_by_key(|&i|&col[i] );
            },
            DbVec::Floats { name: _, col: _ } => {
                unreachable!("There should never be a float primary key");
            },
        }

        self.table.par_iter_mut().for_each(|vec| {
            match vec {
            DbVec::Floats { name: _, col } => {
                rearrange_by_index(col, &indexer);
            },
            DbVec::Ints { name: _, col } => {
                rearrange_by_index(col, &indexer);
            },
            DbVec::Texts { name: _, col } => {
                rearrange_by_index(col, &indexer);
            },
            }
        });
    }

    pub fn query_list(&self, mut key_list: Vec<&str>) -> Result<String, StrictError> {
        let mut printer = String::new();
        let primary_index = self.get_primary_key_col_index();
        key_list.sort();

        let mut indexes = Vec::new();
        for item in key_list {
            match &self.table[primary_index] {
                DbVec::Floats { name: _, col: _ } => return Err(StrictError::FloatPrimaryKey),
                DbVec::Ints { name: _, col } => {
                    let key: i64;
                    match item.parse::<i64>() {
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
                DbVec::Texts { name: _, col } => {
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
                    DbVec::Floats { name: _, col } => printer.push_str(&col[index].to_string()),
                    DbVec::Ints { name: _, col } => printer.push_str(&col[index].to_string()),
                    DbVec::Texts { name: _, col } => printer.push_str(&col[index]),
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
            DbVec::Floats { name: _, col: _ } => return Err(StrictError::FloatPrimaryKey),
            DbVec::Ints { name: _, col } => {
                let key: i64;
                match range.0.parse::<i64>() {
                    Ok(num) => key = num,
                    Err(_) => return Err(StrictError::Empty),
                };
                let index: usize = col.partition_point(|n| n < &key);
                indexes[0] = index;

                if range.1 == "" {
                    indexes[1] = col.len();
                } else {
                    let key2: i64;
                    match range.1.parse::<i64>() {
                        Ok(num) => key2 = num,
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
            DbVec::Texts { name: _, col } => {
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
                    DbVec::Floats { name: _, col } => printer.push_str(&col[i].to_string()),
                    DbVec::Ints { name: _, col } => printer.push_str(&col[i].to_string()),
                    DbVec::Texts { name: _, col } => printer.push_str(&col[i]),
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

    pub fn save_to_disk_raw(&self, path: &str) -> Result<(), StrictError> {
        let file_name = &self.name;

        let metadata = &self.metadata.to_string();

        let table = &self.to_string();


        let mut table_file = match std::fs::File::create(&format!("{}raw_tables/{}",path, file_name)) {
            Ok(f) => f,
            Err(e) => return Err(StrictError::Io(e.kind())),
        };

        let mut meta_file = match std::fs::File::create(&format!("{}raw_tables-metadata/{}",path, file_name)) {
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

    
}

#[inline]
fn rearrange_by_index<T: Clone>(col: &mut Vec<T>, indexer: &Vec<usize>) {

    let mut temp = Vec::with_capacity(col.len());
    for i in 0..col.len() {
        temp.push(col[indexer[i]].clone());
    }
    *col = temp;

} 

fn merge_sorted<T: Ord + Clone + Display + Debug>(one: &Vec<T>, two: &Vec<T>) -> (Vec<T>, Vec<u8>) {
    let mut new_vec: Vec<T> = Vec::with_capacity(one.len() + two.len());
    let mut record_vec: Vec<u8> = Vec::with_capacity(one.len() + two.len());
    let mut one_pointer = 0;
    let mut two_pointer = 0;

    // println!("RUNNING merge_sorted()!!!--------------------------------");
    loop {
        // println!("one[{one_pointer}]: {}\t\ttwo[{two_pointer}]: {}", one[one_pointer], two[two_pointer]);
        if one[one_pointer] < two[two_pointer] {
            new_vec.push(one[one_pointer].clone());
            record_vec.push(1);
            one_pointer += 1;
        } else if one[one_pointer] > two[two_pointer] {
            new_vec.push(two[two_pointer].clone());
            record_vec.push(2);
            two_pointer += 1;
        } else if one[one_pointer] == two[two_pointer]{
            new_vec.push(two[two_pointer].clone());
            record_vec.push(3);
            two_pointer += 1;
            one_pointer += 1;
        } else {
            unreachable!();
        }
        if one_pointer >= one.len() {
            new_vec.extend_from_slice(&two[two_pointer..two.len()]);
            while two_pointer < two.len() {
                record_vec.push(2);
                two_pointer += 1;
            }
            break;
        } else if two_pointer >= two.len() {
            new_vec.extend_from_slice(&one[one_pointer..one.len()]);
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

    (new_vec, record_vec)
}

fn merge_in_order<T: Clone + Display>(one: &Vec<T>, two: &Vec<T>, record_vec: &Vec<u8>) -> Vec<T> {
    let mut new_vec = Vec::with_capacity(one.len() + two.len());
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
                new_vec.push(one[one_pointer].clone());
                one_pointer += 1;
            },
            2 => {
                new_vec.push(two[two_pointer].clone());
                two_pointer += 1;
            },
            3 => {
                new_vec.push(two[two_pointer].clone());
                one_pointer += 1;
                two_pointer += 1;
            }
            _ => unreachable!("Should always be 1, 2, or 3"),
        }
    }

    new_vec
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

    use rand::Rng;

    use super::*;

    #[test]
    fn test_columntable_from_to_string() {
        let input = "vnr,i-p;heiti,t;magn,i\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        // println!("t: {}", t.to_string());
        assert_eq!(input, t.to_string());

    }

    #[test]
    fn test_columntable_combine_sorted() {
        let mut i = 0;
        let mut printer = String::from("vnr,text-p;heiti,text;magn,int;lengd,float\n");
        let mut printer2 = String::from("vnr,text-p;heiti,text;magn,int;lengd,float\n");
        let mut printer22 = String::new();
        loop {
            if i > 50 {
                break;
            }
            let random_number: i64 = rand::thread_rng().gen();
            let random_float: f64 = rand::thread_rng().gen();
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
        let unsorted1 = std::fs::read_to_string("test_csv_from_google_sheets_unsorted.csv").unwrap();
        let unsorted2 = std::fs::read_to_string("test_csv_from_google_sheets2_unsorted.csv").unwrap();
        let sorted_combined = std::fs::read_to_string("test_csv_from_google_sheets_combined_sorted.csv").unwrap();

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
        let input = "vnr,i-p;heiti,t;magn,i\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        // println!("t: {}", t.to_string());
        let x = t.query_list(Vec::from(["113035"])).unwrap();
        assert_eq!(x, "113035;undirlegg;200");
    }

    #[test]
    fn test_columntable_query_single() {
        let input = "vnr,i-p;heiti,t;magn,i\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        // println!("t: {}", t.to_string());
        let x = t.query("113035").unwrap();
        assert_eq!(x, "113035;undirlegg;200");
    }

    #[test]
    fn test_columntable_query_range() {
        let input = "vnr,i-p;heiti,t;magn,i\n113035;undirlegg;200\n113050;annad undirlegg;500\n18572054;flísalím;42\n113446;harlech;250";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        let x = t.query_range(("113035", "113060")).unwrap();

        assert_eq!(x, "113035;undirlegg;200\n113050;annad undirlegg;500")
    }


}