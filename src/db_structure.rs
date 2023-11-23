use std::{fmt, collections::{BTreeMap, HashMap}, path::{Display, self, Path}, io::{ErrorKind, Write}};

use crate::logger::get_current_time;
use crate::networking_utilities::*;

use smartstring::{SmartString, LazyCompact};

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
}

#[derive(Clone, Debug)]
pub enum DbVec {
    Ints{ name: KeyString, col: Vec<i64> },
    Floats{ name: KeyString, col: Vec<f64> },
    Texts{ name: KeyString, col: Vec<KeyString> },
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
                    _ => return Err(StrictError::WrongType),
                }
            }
        }

        let mut line_index = 0;
        

        
        let mut result = Vec::new();

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

        let mut i = 0;
        for col in data {
            result.push(parse_column(header_names[i].clone(), header_types[i].clone(), col).unwrap());
            i += 1;
        }


        Ok(
            ColumnTable { metadata: Metadata::new("test"), header: header_names, table: result }
        )
    }
}

pub fn parse_column(col_name: KeyString, type_name: DbTypes, col: Vec<&str>) -> Result<DbVec, StrictError> {
    let result = match type_name {
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
            DbVec::Floats { name: col_name, col: outvec }
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
            DbVec::Ints { name: col_name, col: outvec }
        },
        DbTypes::Text => {
            let mut outvec = Vec::with_capacity(col.len());
            for cell in col {
                outvec.push(KeyString::from(cell));
            }
            DbVec::Texts { name: col_name, col: outvec }
        },
    };

    Ok(result)

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
    use std::str::from_utf8;

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

        // let mut i = 0;
        // let mut printer = String::from("vnr,int;heiti,text;magn,int;lengd,float\n");
        // loop {
        //     if i > 10_000_000 {
        //         break;
        //     }
        //     let random_number: i64 = rand::thread_rng().gen();
        //     let random_float: f64 = rand::thread_rng().gen();
        //     printer.push_str(&format!("i{};product name;{random_number};{random_float}\n", i));
        //     i+= 1;
        // }
        // let mut file = std::fs::File::create("large.csv").unwrap();
        // file.write_all(printer.as_bytes()).unwrap();

        let csv = std::fs::read_to_string("large.csv").unwrap();
        let instant = std::time::Instant::now();
        for i in 0..4 {
            let t = ColumnTable::from_csv_string(&csv, "test").unwrap();
        }
        println!("TIME! {}", instant.elapsed().as_millis()/20)
    }

}