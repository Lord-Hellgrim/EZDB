use std::{fmt::{self, Display, Debug}, collections::BTreeMap, io::Write};

use crate::logger::get_current_time;
use crate::networking_utilities::*;

use smartstring::{SmartString, LazyCompact};

use rayon::prelude::*;

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

        }
    }
}

impl From<std::io::ErrorKind> for StrictError {
    fn from(e: std::io::ErrorKind) -> Self{
        StrictError::Io(e)
    }
}

// This struct is here to future proof the StrictTable. More metadata will be added in future.
#[derive(PartialEq, Clone, Debug)]
pub struct Metadata {
    pub last_access: u64,
    pub times_accessed: u64,
    pub created_by: String,
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
            created_by: String::from(client),
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

#[derive(Clone, Debug)]
pub enum DbTypes {
    Int,
    Float,
    Text,
    IntPrimaryKey,
    TextPrimaryKey,
}

#[derive(Clone, Debug)]
pub enum DbVec {
    Ints{ name: KeyString, primary_key: bool, col: Vec<i64> },
    Floats{ name: KeyString, primary_key: bool, col: Vec<f64> },
    Texts{ name: KeyString, primary_key: bool, col: Vec<KeyString> },
}

#[derive(Clone, Debug)]
pub struct ColumnTable {
    metadata: Metadata,
    header: Vec<KeyString>,
    table: Vec<DbVec>,
}

impl ColumnTable {
    pub fn from_csv_string(s: &str, name: &str) -> Result<ColumnTable, StrictError> {

        if s.len() < 1 {
            return Err(StrictError::Empty)
        }

        let mut header_names = Vec::new();
        let mut header_types = Vec::new();
        let mut primary_key_set = false;

        let header: Vec<&str> = s.split('\n').next().expect("confirmed to exist because of earlier check").split(';').collect();
        for item in header {
            let temp: Vec<&str> = item.split(',').collect();
            if temp.len() < 2 {
                return Err(StrictError::MissingType)
            } else if temp.len() > 2 {
                return Err(StrictError::TooManyHeaderFields)
            } else {
                header_names.push(KeyString::from(temp[0].trim()));
                let t = temp[1].trim();
                match t {
                    "I" | "Int" | "int" | "i" => header_types.push(DbTypes::Int),
                    "F" | "Float" | "float" | "f" => header_types.push(DbTypes::Float),
                    "T" | "Text" | "text" | "t" => header_types.push(DbTypes::Text),
                    "I-p" | "Int-p" | "int-p" | "i-p" => {
                        if primary_key_set {
                            return Err(StrictError::TooManyPrimaryKeys)
                        } else {
                            header_types.push(DbTypes::IntPrimaryKey);
                            primary_key_set = true;
                        }
                    },
                    "F-p" | "Float-p" | "float-p" | "f-p" => return Err(StrictError::FloatPrimaryKey),
                    "T-p" | "Text-p" | "text-p" | "t-p" => {
                        if primary_key_set {
                            return Err(StrictError::TooManyPrimaryKeys)
                        } else {
                            header_types.push(DbTypes::TextPrimaryKey);
                            primary_key_set = true;
                        }
                    },
                    _ => return Err(StrictError::WrongType),
                }
            }
        }

        if !primary_key_set {
            match header_types[0] {
                DbTypes::Int => header_types[0] = DbTypes::IntPrimaryKey,
                DbTypes::Text => header_types[0] = DbTypes::TextPrimaryKey,
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
            
            let db_vec = match header_types[i] {
                DbTypes::Float => {
                    let mut outvec = Vec::with_capacity(col.len());
                    let mut index = 0;
                    for cell in col {
                        let temp = match cell.parse::<f64>() {
                            Ok(x) => x,
                            Err(_) => {
                                println!("failed to parse: {}", cell);
                                return Err(StrictError::Parse(index))
                            },
                        };
                        outvec.push(temp);
                        index += 1;
                    }
                    DbVec::Floats { name: header_names[i].clone(), primary_key: false, col: outvec }
                },
                DbTypes::Int => {
                    let mut outvec = Vec::with_capacity(col.len());
                    let mut index = 0;
                    for cell in col {
                        let temp = match cell.parse::<i64>() {
                            Ok(x) => x,
                            Err(_) => {
                                println!("failed to parse: {}", cell);
                                return Err(StrictError::Parse(index))
                            },
                        };
                        outvec.push(temp);
                        index += 1;
                    }
                    DbVec::Ints { name: header_names[i].clone(), primary_key: false, col: outvec }
                },
                DbTypes::Text => {
                    let mut outvec = Vec::with_capacity(col.len());
                    for cell in col {
                        outvec.push(KeyString::from(cell));
                    }
                    DbVec::Texts { name: header_names[i].clone(), primary_key: false, col: outvec }
                },
                DbTypes::IntPrimaryKey => {
                    let mut outvec = Vec::with_capacity(col.len());
                    let mut index = 0;
                    for cell in col {
                        let temp = match cell.parse::<i64>() {
                            Ok(x) => x,
                            Err(_) => {
                                println!("failed to parse: {}", cell);
                                return Err(StrictError::Parse(index))
                            },
                        };
                        outvec.push(temp);
                        index += 1;
                    }
                    DbVec::Ints { name: header_names[i].clone(), primary_key: true, col: outvec }
                },
                DbTypes::TextPrimaryKey => {
                    let mut outvec = Vec::with_capacity(col.len());
                    for cell in col {
                        outvec.push(KeyString::from(cell));
                    }
                    DbVec::Texts { name: header_names[i].clone(), primary_key: true, col: outvec }
                },
            };
            
            result.push(db_vec);
            i += 1;
        }

        let mut output = ColumnTable { metadata: Metadata::new(name), header: header_names, table: result };
        output.sort();
        Ok(
            output
        )

    }

    pub fn insert_csv(&mut self, input_csv: &str) -> Result<(), StrictError> {

        let insert_table = ColumnTable::from_csv_string(input_csv, "insert")?;

        self.update(insert_table)?;

        Ok(())
    }

    pub fn get_primary_key_col_index(&self) -> usize {
        let mut self_primary_key_index = 0;
        for i in 0..self.table.len() {
            match self.table[i] {
                DbVec::Ints { name: _, primary_key, col: _ } => {
                    if primary_key {
                        self_primary_key_index = i;
                        break
                }
            },
                DbVec::Texts { name: _, primary_key, col: _ } => {
                    if primary_key {
                        self_primary_key_index = i;
                        break
                    }
                },
                DbVec::Floats { name: _, primary_key, col: _ } => {
                    if primary_key {
                        unreachable!("There should never be a float primary key");
                    }
                },
            }
        }

        self_primary_key_index
    }

    pub fn update(&mut self, other_table: ColumnTable) -> Result<(), StrictError> {

        if self.header != other_table.header {
            return Err(StrictError::Update("Headers don't match".to_owned()));
        }

        let self_primary_key_index = self.get_primary_key_col_index();

        let minlen = std::cmp::min(self.table.len(), other_table.table.len());

        let mut record_vec: Vec<u8>;
        match &mut self.table[self_primary_key_index] {
            DbVec::Ints { name: _, primary_key: _, col } => {
                match &other_table.table[self_primary_key_index] {
                    DbVec::Ints { name: _, primary_key: _, col: other_col } => {
                        (*col, record_vec) = merge_sorted(col, other_col);
                    },
                    _ => unreachable!("Should always have the same primary key column")
                }
            },
            DbVec::Texts { name: _, primary_key: _, col } => {
                match &other_table.table[self_primary_key_index] {
                    DbVec::Texts { name: _, primary_key: _, col: other_col } => {
                        (*col, record_vec) = merge_sorted(col, other_col);
                    },
                    _ => unreachable!("Should always have the same primary key column")
                }
            },
            DbVec::Floats { name: _, primary_key: _, col: _ } => unreachable!("Should never have a float primary key column"),
        }
        for i in 0..minlen {

            if i == self_primary_key_index {
                continue;
            }

            match &mut self.table[i] {
                DbVec::Ints { name: _, primary_key: _, col } => {
                    match &other_table.table[i] {
                        DbVec::Ints { name: _, primary_key: _, col: other_col } => {
                            *col = merge_in_order(col, other_col, &record_vec);
                        },
                        _ => unreachable!("Should always have the same type column")
                    }
                },
                DbVec::Texts { name: _, primary_key: _, col } => {
                    match &other_table.table[i] {
                        DbVec::Texts { name: _, primary_key: _, col: other_col } => {
                            *col = merge_in_order(col, other_col, &record_vec);
                        },
                        _ => unreachable!("Should always have the same type column")
                    }
                },
                DbVec::Floats { name: _, primary_key: _, col } => {
                    match &other_table.table[i] {
                        DbVec::Floats { name: _, primary_key: _, col: other_col } => {
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
            DbVec::Floats { name: _, primary_key: _, col } => len = col.len(),
            DbVec::Ints { name: _, primary_key: _, col } => len = col.len(),
            DbVec::Texts { name: _, primary_key: _, col } => len = col.len(),
        }
        len
    }

    pub fn sort(&mut self) {

        let len = self.len();

        let outer_instant = std::time::Instant::now();
        let mut indexer: Vec<usize> = (0..len).collect();
        
        let mut primary_key_exists = false;
        for vec in self.table.iter_mut() {
            match vec {
                DbVec::Ints { name: _, primary_key, col } => {
                    if *primary_key {
                    let instant = std::time::Instant::now();
                    indexer.sort_unstable_by_key(|&i|col[i] );
                    let time = instant.elapsed().as_millis();
                    println!("time to sort indexer with int PK: {} millis", time);
                    primary_key_exists = true;
                }
            },
            DbVec::Texts { name: _, primary_key, col } => {
                if *primary_key {
                    let instant = std::time::Instant::now();
                    indexer.sort_unstable_by_key(|&i|&col[i] );
                    let time = instant.elapsed().as_millis();
                    println!("time to sort indexer with text PK: {} millis", time);
                    primary_key_exists = true;
                }
            },
            DbVec::Floats { name: _, primary_key, col: _ } => {
                if *primary_key {
                    unreachable!("There should never be a float primary key");
                }
            },
            }
        }
        let outer_time = outer_instant.elapsed().as_millis();
        println!("total indexer sorting time: {} millis", outer_time);


        if !primary_key_exists {
            unreachable!("There should always be a primary key on every table")
        }


        let instant = std::time::Instant::now();
        self.table.par_iter_mut().for_each(|vec| {
            match vec {
            DbVec::Floats { name: _, primary_key: _, col } => {
                rearrange_by_index(col, &indexer);
            },
            DbVec::Ints { name: _, primary_key: _, col } => {
                rearrange_by_index(col, &indexer);
            },
            DbVec::Texts { name: _, primary_key: _, col } => {
                rearrange_by_index(col, &indexer);
            },
            }
        });

        let time = instant.elapsed().as_millis();
        println!("time to rearrange columns: {} millis", time);

    }

    pub fn to_string(&self) -> String {
        let mut printer = String::new();
        for i in 0..(self.len()-1) {

            for vec in &self.table {
                match vec {
                    DbVec::Floats { name: _, primary_key: _, col } => {
                        println!("float: col.len(): {}", col.len());
                        printer.push_str(&col[i].to_string());
                        printer.push_str(";");
                    },
                    DbVec::Ints { name: _, primary_key: _, col } => {
                        println!("int: col.len(): {}", col.len());
                        printer.push_str(&col[i].to_string());
                        printer.push_str(";");
                    },
                    DbVec::Texts { name: _, primary_key: _, col } => {
                        println!("text: col.len(): {}", col.len());
                        printer.push_str(&col[i]);
                        printer.push_str(";");
                    },
                }
                printer.pop();
            }
            printer.push_str("\n");
        }
        printer.pop();

        printer
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

    println!("RUNNING merge_sorted()!!!--------------------------------");
    loop {
        println!("one[{one_pointer}]: {}\t\ttwo[{two_pointer}]: {}", one[one_pointer], two[two_pointer]);
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
            let cap = two.len() - two_pointer;
            while two_pointer < cap + 1 {
                record_vec.push(2);
                two_pointer += 1;
            }
            break;
        } else if two_pointer >= two.len() {
            new_vec.extend_from_slice(&one[one_pointer..one.len()]);
            let cap = one.len() - one_pointer;
            while one_pointer < cap + 1 {
                record_vec.push(1);
                one_pointer += 1;
            }
            
            break;
        }
    }
    println!("new_vec.len(): {}\nnew_vec\n{:?}", new_vec.len(), new_vec);
    println!("record_vec.len(): {}\nrecord_vec: \n{:?}", record_vec.len(), record_vec);
    println!("merge_sorted() FINISHED !!!!!!######################################");
    println!("\n\n");

    (new_vec, record_vec)
}

fn merge_in_order<T: Clone>(one: &Vec<T>, two: &Vec<T>, record_vec: &Vec<u8>) -> Vec<T> {
    let mut new_vec = Vec::with_capacity(one.len() + two.len());
    let mut one_pointer = 0;
    let mut two_pointer = 0;
    // println!("record_vec.len(): {}", record_vec.len());
    // println!("one.len():   {}", one.len());
    // println!("two.len():   {}", two.len());
    println!("record_vec: {:?}", record_vec);
    for index in record_vec {
        println!("one_p: {}\ttwo_p: {}", one_pointer, two_pointer);
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
                if count_char(line.as_bytes(), 59) < header.len()-1 {
                    return Err(StrictError::FewerItemsThanHeader(linenum));
                } else if count_char(line.as_bytes(), 59) > header.len()-1 {
                    return Err(StrictError::MoreItemsThanHeader(linenum));
                } else {
                    linenum += 1;
                }
            }
        } // Finished checking


        let mut output = BTreeMap::new();
        let mut rownum: usize = 0;
        for row in fast_split(s, "\n".as_bytes()[0]) {
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

        value_file.write_all(&self.body);
        meta_file.write_all(metadata.as_bytes());

        Ok(())

    }
}


#[cfg(test)]
mod tests {

    use rand::Rng;

    use super::*;

    #[test]
    fn test_StrictError_fewer() {
        let s = "here baby;1;2\n3;4".to_owned();
        let out: StrictTable;
        match create_StrictTable_from_csv(&s, "test") {
            Ok(o) => out = o,
            Err(e) => {
                println!("{}", e);
                assert_eq!(e, StrictError::FewerItemsThanHeader(1));
            },
        };
        
    }

    #[test]
    fn test_StrictError_more() {
        let s = "here baby;1;2\n3;4;5;6".to_owned();
        let out: StrictTable;
        match create_StrictTable_from_csv(&s, "test") {
            Ok(o) => out = o,
            Err(e) => {
                println!("{}", e);
                assert_eq!(e, StrictError::MoreItemsThanHeader(1));
            },
        };
        
    }

    #[test]
    fn test_StrictError_repeating_header() {
        let s = "here baby;1;1\n3;4;5".to_owned();
        let out: StrictTable;
        match create_StrictTable_from_csv(&s, "test") {
            Ok(o) => out = o,
            Err(e) => {
                println!("{}", e);
                assert_eq!(e, StrictError::RepeatingHeader(1, 2));
            },
        };
        
    }

    #[test]
    fn test_method_equals_function() {
        let s = "here baby;1;2\n3;4;5".to_owned();
        let out1: StrictTable;
        match StrictTable::from_csv_string(&s, "test") {
            Ok(o) => out1 = o,
            Err(e) => {
                println!("{}", e);
                return;
            },
        };
        let out2: StrictTable;
        match create_StrictTable_from_csv(&s, "test") {
            Ok(o) => out2 = o,
            Err(e) => {
                println!("{}", e);
                return;
            },
        };

        assert_eq!(out1, out2);
        
    }

    #[test]
    fn test_StrictTable_to_csv_string() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let t = StrictTable::from_csv_string(&csv, "test").unwrap();
        println!("{:?}", t.header);
        println!("{:?}", t.table);
        let x = t.to_csv_string();
        println!("{}", x);
        assert_eq!(x, "vnr;heiti;magn\n0113000;undirlegg2;100\n0113035;undirlegg;200\n18572054;flísalím;42");
    }

    #[test]
    fn test_update_StrictTable() {
        let s = std::fs::read_to_string("good_csv.txt").unwrap();
        let mut t = StrictTable::from_csv_string(&s, "test").unwrap();
        println!("{:?}", t.table);
        let update_csv = "vnr;heiti;magn\n0113030;Flotsement;50";
        t.update(update_csv);
        assert_eq!(t.to_csv_string(), "vnr;heiti;magn\n0113000;undirlegg2;100\n0113030;Flotsement;50\n0113035;undirlegg;200\n18572054;flísalím;42")

    }

    #[test]
    fn test_query_range() {
        let s = std::fs::read_to_string("good_csv.txt").unwrap();
        let mut t = StrictTable::from_csv_string(&s, "test").unwrap();
        let update_csv = "vnr;heiti;magn\n0113030;Flotsement;50";
        t.update(update_csv);
        let queried_table = t.query_range(("0113000", "0113035")).unwrap();
        assert_eq!(queried_table, "0113000;undirlegg2;100\n0113030;Flotsement;50\n0113035;undirlegg;200");
    }

    #[test]
    fn test_query_list() {
        let s = std::fs::read_to_string("good_csv.txt").unwrap();
        let mut t = StrictTable::from_csv_string(&s, "test").unwrap();
        let update_csv = "vnr;heiti;magn\n0113030;Flotsement;50";
        t.update(update_csv);
        let queried_table = t.query_list(vec!("0113000", "18572054", "0113035")).unwrap();

        assert_eq!(queried_table, "0113000;undirlegg2;100\n18572054;flísalím;42\n0113035;undirlegg;200");
    }

    #[test]
    fn test_save_raw_table() {
        let csv = std::fs::read_to_string("good_csv.txt").unwrap();
        let t = StrictTable::from_csv_string(&csv, "test").unwrap();
        println!("{:?}", t.header);
        println!("{:?}", t.table);
        t.save_to_disk_raw("EZconfig/").unwrap();
    }

    #[test]
    fn test_columntable() {

        let mut i = 0;
        let mut printer = String::from("vnr,text-p;heiti,text;magn,int;lengd,float\n");
        let mut printer2 = String::from("vnr,text-p;heiti,text;magn,int;lengd,float\n");
        loop {
            if i > 50 {
                break;
            }
            let random_number: i64 = rand::thread_rng().gen();
            let random_float: f64 = rand::thread_rng().gen();
            let random_key: u32 = rand::thread_rng().gen();
            let random_key2: u32 = rand::thread_rng().gen();
            let mut random_string = String::new();
            for _ in 0..8 {
                random_string.push(rand::thread_rng().gen_range(97..122) as u8 as char);
            }
            printer.push_str(&format!("a{random_key};{random_string};{random_number};{random_float}\n"));
            printer2.push_str(&format!("b{random_key};{random_string};{random_number};{random_float}\n"));
            
            i+= 1;
        }
        let mut file = std::fs::File::create("large.csv").unwrap();
        file.write_all(printer.as_bytes()).unwrap();
        let mut file = std::fs::File::create("large2.csv").unwrap();
        file.write_all(printer2.as_bytes()).unwrap();


        let csv = std::fs::read_to_string("large.csv").unwrap();
        let csv2 = std::fs::read_to_string("large2.csv").unwrap();
        let instant = std::time::Instant::now();
        // let mut t: ColumnTable = ColumnTable::from_csv_string(&csv, "init").unwrap();
        let mut t = ColumnTable::from_csv_string(&csv, "test").unwrap();
        let el = instant.elapsed().as_millis();
        println!("TIME to parse! {}", el);
        
        let r = ColumnTable::from_csv_string(&csv2, "test").unwrap();
        t.update(r);
        println!("t: {}", t.to_string());

    }

}