use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet}, fmt::{self, Debug, Display}, num::{ParseFloatError, ParseIntError}, sync::atomic::{AtomicU64, Ordering}
};

// use smartstring::{LazyCompact, SmartString, };

use ezcbor::cbor::{byteslice_from_cbor, byteslice_to_cbor, expected_data_item, Cbor, CborError, DataItem};

use crate::utilities::*;
#[allow(unused)]
use crate::PATH_SEP;

/// Alias for SmartString
// pub type KeyString = SmartString<LazyCompact>;


/// The struct that carries metadata relevant to a given table. More metadata will probably be added later.
#[derive(Debug)]
pub struct Metadata {
    pub last_access: AtomicU64,
    pub times_accessed: AtomicU64,
    pub created_by: KeyString,
}

impl Clone for Metadata {
    fn clone(&self) -> Self {
        Self { 
            last_access: AtomicU64::new(self.last_access.load(Ordering::Relaxed)),
            times_accessed: AtomicU64::new(self.times_accessed.load(Ordering::Relaxed).clone()),
            created_by: self.created_by.clone(),
        }
    }
}

impl fmt::Display for Metadata {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let mut printer = String::new();

        printer.push_str(&format!("last_access:{}\n", self.last_access.load(Ordering::Relaxed)));
        printer.push_str(&format!("times_accessed:{}\n", self.times_accessed.load(Ordering::Relaxed)));
        printer.push_str(&format!("created_by:{}", self.created_by));
        writeln!(f, "{}", printer)
    }
}

impl Cbor for Metadata {
    fn to_cbor_bytes(&self) -> Vec<u8> {
        

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.last_access.load(Ordering::Relaxed).to_cbor_bytes());
        bytes.extend_from_slice(&self.times_accessed.load(Ordering::Relaxed).to_cbor_bytes());
        bytes.extend_from_slice(&self.created_by.to_cbor_bytes());
        bytes
    }

    fn from_cbor_bytes(bytes: &[u8]) -> Result<(Self, usize), ezcbor::cbor::CborError>
        where 
            Self: Sized 
    {
        

        let mut i = 0;
        let (last_access, bytes_read) = <u64 as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        let (times_accessed, bytes_read) = <u64 as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        let (created_by, bytes_read) = <KeyString as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        Ok((Self {
            last_access: AtomicU64::from(last_access),
            times_accessed: AtomicU64::from(times_accessed),
            created_by,
        }, i))
    }
}

impl Metadata {
    pub fn new(client: &str) -> Metadata {
        
        Metadata {
            last_access: AtomicU64::new(get_current_time()),
            times_accessed: AtomicU64::new(0),
            created_by: KeyString::from(client),
        }
    }
}


/// Identifies a type of a DbVec
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum DbType {
    Int,
    Float,
    Text,
}

impl Cbor for DbType {
    fn to_cbor_bytes(&self) -> Vec<u8> {
        

        let mut bytes = Vec::new();
        match self {
            DbType::Int => bytes.push(0xc6),
            DbType::Float => bytes.push(0xc6+1),
            DbType::Text => bytes.push(0xc6+2),
        };
        bytes
    }

    fn from_cbor_bytes(bytes: &[u8]) -> Result<(Self, usize), CborError>
        where 
            Self: Sized 
    {
        

        match expected_data_item(bytes[0]) {
            DataItem::Tag(byte) => match byte {
                0 => Ok((DbType::Int, 1)),
                1 => Ok((DbType::Float, 1)),
                2 => Ok((DbType::Text, 1)),
                _ => return Err(CborError::Unexpected(format!("Unexpected byte encountered while decoding a DbType. Should only allow 0x0, 0x1, or 0x2 but encounterd '{:x}'", byte))),

            },
            _ => return Err(CborError::Unexpected("Error originated from TableKey implementation".to_owned())),
        }
    }
}

/// A single column in a database table.
#[derive(Clone, Debug, PartialEq, PartialOrd)]
pub enum DbColumn {
    Ints(Vec<i32>),
    Texts(Vec<KeyString>),
    Floats(Vec<f32>),
}

impl Display for DbColumn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        

        match self {
            DbColumn::Ints(v) => write!(f, "{:?}", v),
            DbColumn::Floats(v) => write!(f, "{:?}", v),
            DbColumn::Texts(v) => write!(f, "{:?}", v),
        }
    }
}

impl Cbor for DbColumn {
    fn to_cbor_bytes(&self) -> Vec<u8> {
        

        let mut bytes = Vec::new();
        match self {
            DbColumn::Ints(col) => {
                bytes.push(0xc6);
                bytes.extend_from_slice(&col.to_cbor_bytes());
            },
            DbColumn::Texts(col) => {
                bytes.push(0xc6+1);
                bytes.extend_from_slice(&col.to_cbor_bytes());

            },
            DbColumn::Floats(col) => {
                bytes.push(0xc6+2);
                bytes.extend_from_slice(&col.to_cbor_bytes());

            },
        }
        bytes
    }

    fn from_cbor_bytes(bytes: &[u8]) -> Result<(Self, usize), CborError>
        where 
            Self: Sized 
    {
        

        match expected_data_item(bytes[0]) {
            DataItem::Tag(byte) => match byte {
                0 => {
                    let (thing, bytes_read) = <Vec<i32> as Cbor>::from_cbor_bytes(&bytes[1..])?;
                    Ok((DbColumn::Ints(thing), bytes_read+1))
                },
                1 => {
                    let (thing, bytes_read) = <Vec<KeyString> as Cbor>::from_cbor_bytes(&bytes[1..])?;
                    Ok((DbColumn::Texts(thing), bytes_read+1))
                },
                2 => {
                    let (thing, bytes_read) = <Vec<f32> as Cbor>::from_cbor_bytes(&bytes[1..])?;
                    Ok((DbColumn::Floats(thing), bytes_read+1))
                },
                _ => return Err(CborError::Unexpected(format!("Unexpected byte encountered while decoding a DbColumn. Should only allow 0x0, 0x1, or 0x2 but encounterd '{:x}'", byte))),
            },
            _ => return Err(CborError::Unexpected("Error originated from TableKey implementation".to_owned())),
        }
    }
}

impl DbColumn {
    pub fn len(&self) -> usize {
        match self {
            DbColumn::Floats(v) => v.len(),
            DbColumn::Ints(v) => v.len(),
            DbColumn::Texts(v) => v.len(),
        }
    }

    pub fn get_i32_col(&self) -> &Vec<i32> {
        match self {
            DbColumn::Ints(col) => col,
            _ => panic!("Never call this function unless you are sure it's an i32 column"),
        }
    }

    pub fn get_f32_col(&self) -> &Vec<f32> {
        match self {
            DbColumn::Floats(col) => col,
            _ => panic!("Never call this function unless you are sure it's an f32 column"),
        }
    }

    pub fn get_text_col(&self) -> &Vec<KeyString> {
        match self {
            DbColumn::Texts(col) => col,
            _ => panic!("Never call this function unless you are sure it's a KeyString column"),
        }
    }
}

/// The header of a database column. Identifies name, type, and whether it is the primary key,
/// a forreign key or just a regular ol' entry
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HeaderItem {
    pub name: KeyString,
    pub kind: DbType,
    pub key: TableKey,
}

impl Display for HeaderItem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        

        let mut printer = String::new();
        printer.push_str(self.name.as_str());
        printer.push(',');
        match self.kind {
            DbType::Float => printer.push('f'),
            DbType::Int => printer.push('i'),
            DbType::Text => printer.push('t'),
        }
        match &self.key {
            TableKey::Primary => printer.push_str("-P"),
            TableKey::Foreign => printer.push_str("-F"),
            TableKey::None => printer.push_str("-N"),
        }
        write!(f, "{}", printer)
    }
}

impl Default for HeaderItem {
    fn default() -> Self {
        Self::new()
    }
}

impl Cbor for HeaderItem {
    fn to_cbor_bytes(&self) -> Vec<u8> {
        

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.name.to_cbor_bytes());
        bytes.extend_from_slice(&self.kind.to_cbor_bytes());
        bytes.extend_from_slice(&self.key.to_cbor_bytes());
        bytes
    }

    fn from_cbor_bytes(bytes: &[u8]) -> Result<(Self, usize), CborError>
        where 
            Self: Sized 
    {
        

        let mut i = 0;
        let (name, bytes_read) = <KeyString as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        let (kind, bytes_read) = <DbType as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        let (key, bytes_read) = <TableKey as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        Ok(
            (
                Self { name, kind, key },
                i
            )
        )
    }
}

impl HeaderItem {
    pub fn new() -> HeaderItem {
        HeaderItem {
            name: KeyString::from("default_name"),
            kind: DbType::Text,
            key: TableKey::None,
        }
    }
}



/// The type of key a column can represent. Currently unused. I haven't implmented joins yet.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum TableKey {
    Primary,
    None,
    Foreign,
}

impl Cbor for TableKey {
    fn to_cbor_bytes(&self) -> Vec<u8> {
        

        let mut bytes = Vec::new();
        match self {
            TableKey::Primary => bytes.push(0xc6),
            TableKey::None => bytes.push(0xc6+1),
            TableKey::Foreign => bytes.push(0xc6+2),
        };
        bytes
    }

    fn from_cbor_bytes(bytes: &[u8]) -> Result<(Self, usize), CborError>
        where 
            Self: Sized 
    {
        

        match expected_data_item(bytes[0]) {
            DataItem::Tag(byte) => match byte {
                0 => Ok((TableKey::Primary, 1)),
                1 => Ok((TableKey::None, 1)),
                2 => Ok((TableKey::Foreign, 1)),
                _ => return Err(CborError::Unexpected(format!("Unexpected byte encountered while decoding a TableKey. Should only allow 0x0, 0x1, or 0x2 but encounterd '{:x}'", byte))),
            },
            _ => return Err(CborError::Unexpected("Error originated from TableKey implementation".to_owned())),
        }
    }
}


/// This is the main data structure of EZDB. It represents a table as a list of columns.
#[derive(Clone, Debug)]
pub struct ColumnTable {
    pub name: KeyString,
    pub header: BTreeSet<HeaderItem>,
    pub columns: BTreeMap<KeyString, DbColumn>,
}

impl PartialOrd for ColumnTable {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match self.name.partial_cmp(&other.name) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        match self.header.partial_cmp(&other.header) {
            Some(core::cmp::Ordering::Equal) => {}
            ord => return ord,
        }
        self.columns.partial_cmp(&other.columns)
    }
}

impl PartialEq for ColumnTable {
    fn eq(&self, other: &Self) -> bool {
        self.header == other.header && self.columns == other.columns
    }
}

impl Cbor for ColumnTable {
    fn to_cbor_bytes(&self) -> Vec<u8> {

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&self.name.to_cbor_bytes());
        bytes.extend_from_slice(&self.header.to_cbor_bytes());
        bytes.extend_from_slice(&self.columns.to_cbor_bytes());
        bytes
    }

    fn from_cbor_bytes(bytes: &[u8]) -> Result<(Self, usize), CborError>
        where 
            Self: Sized 
    {
        

        let mut i = 0;
        
        let (name, bytes_read) = <KeyString as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        let (header, bytes_read) = <BTreeSet<HeaderItem> as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        let (columns, bytes_read) = <BTreeMap<KeyString, DbColumn> as Cbor>::from_cbor_bytes(&bytes[i..])?;
        i += bytes_read;
        Ok(
            (
                Self { name, header, columns  },
                i
            )
        )
    }
}

/// Prints the ColumnTable as a csv (separated by semicolons ;)
impl Display for ColumnTable {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        

        let mut printer = String::new();

        for item in &self.header {
            printer.push_str(&item.to_string());
            printer.push(';');
        }
        printer.pop();
        printer.push('\n');

        for i in 0..(self.len()) {
            for vec in self.columns.values() {
                match vec {
                    DbColumn::Floats(col) => {
                        // println!("float: col.len(): {}", col.len());
                        printer.push_str(&col[i].to_string());
                        printer.push(';');
                    }
                    DbColumn::Ints(col) => {
                        // println!("int: col.len(): {}", col.len());
                        printer.push_str(&col[i].to_string());
                        printer.push(';');
                    }
                    DbColumn::Texts(col) => {
                        // println!("text: col.len(): {}", col.len());
                        printer.push_str(col[i].as_str());
                        printer.push(';');
                    }
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

    pub fn create_empty(name: &str, created_by: &str) -> ColumnTable {

        ColumnTable {
            name: ksf(name),
            header: BTreeSet::new(),
            columns: BTreeMap::new(),
        }
    }

    pub fn blank(header: &BTreeSet<HeaderItem>, name: KeyString, created_by: &str) -> ColumnTable {

        let mut columns = BTreeMap::new();

        for head in header {
            match head.kind {
                DbType::Int => columns.insert(head.name, DbColumn::Ints(Vec::new())),
                DbType::Float => columns.insert(head.name, DbColumn::Floats(Vec::new())),
                DbType::Text => columns.insert(head.name, DbColumn::Texts(Vec::new())),
            };
        }

        ColumnTable {
            name: name,
            header: header.clone(),
            columns,
        }

    }

    /// Parses a ColumnTable from a csv string. Ensures strictness. See EZ CSV FORMAT below.
    pub fn from_csv_string(
        s: &str,
        table_name: &str,
        created_by: &str,
    ) -> Result<ColumnTable, EzError> {
        

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
        id,i-P;name,Text-N;product_group,t-F

        The body can be formatted like this:

        123;sample;samples
        234;plunger;toiletries
        567;racecar;toys

        If a value needs to contain a ";" character, you can enclose the calue in triple quotes """value"""
        Values will not be trimmed. Any whitespace will be included. Take care that the triple quotes are included in the 255 character limit for text values
        if you need to store text values longer than 255 characters, reference them by foreign keys to key_value storage
        */

        if s.is_empty() {
            return Err(EzError{tag: ErrorTag::Deserialization, text: ("Input string is empty".to_owned())});
        }

        let mut header = Vec::new();
        let mut primary_key_set = false;

        let first_line: Vec<&str> = s
            .split('\n')
            .next()
            .expect("confirmed to exist because of earlier check")
            .split(';')
            .collect();
        for item in first_line {
            let temp: Vec<&str> = item.split(',').collect();
            let mut header_item = HeaderItem::new();
            if temp.is_empty() {
                return Err(EzError{tag: ErrorTag::Deserialization, text: ("Header is empty".to_owned())});
            } else if temp.len() == 1 {
                header_item.kind = DbType::Text;
            } else if temp.len() > 2 {
                return Err(EzError{tag: ErrorTag::Deserialization, text: ("Incorrectly formatted header".to_owned())});
            } else {
                header_item.name = KeyString::from(temp[0].trim());
                let mut t = temp[1].trim().split('-');
                let next = t.next().unwrap();
                match next {
                    "I" | "Int" | "int" | "i" => header_item.kind = DbType::Int,
                    "F" | "Float" | "float" | "f" => header_item.kind = DbType::Float,
                    "T" | "Text" | "text" | "t" => header_item.kind = DbType::Text,
                    _ => return Err(EzError{tag: ErrorTag::Deserialization, text: (format!("Unsupported type: {}", next))}),
                }
                match t.next().unwrap() {
                    "P" => {
                        if primary_key_set {
                            return Err(EzError{tag: ErrorTag::Deserialization, text: ("Too many primary keys specified".to_owned())});
                        }
                        header_item.key = TableKey::Primary;
                        primary_key_set = true;
                    }
                    "N" => header_item.key = TableKey::None,
                    "F" => header_item.key = TableKey::Foreign,
                    _ => return Err(EzError{tag: ErrorTag::Deserialization, text: ("Unsupported key type".to_owned())}),
                }
            }
            header.push(header_item);
        }

        if !primary_key_set {
            panic!("You need to specify a primary key")
        }

        let mut line_index = 0;
        let mut data: Vec<Vec<&str>> = Vec::new();
        for line in s.lines() {
            // println!("line: {}", line);
            if line_index == 0 {
                line_index += 1;
                continue;
            }
            for (row_index, cell) in line.split(';').enumerate() {
                if line_index == 1 {
                    data.push(Vec::from([cell]));
                } else {
                    data[row_index].push(cell);
                }
            }
            line_index += 1;
        }

        let mut result = BTreeMap::new();
        for (i, col) in data.into_iter().enumerate() {
            let db_vec = match header.iter().nth(i).unwrap().kind {
                DbType::Float => {
                    let mut outvec = Vec::with_capacity(col.len());
                    for (index, cell) in col.iter().enumerate() {
                        let temp = match cell.parse::<f32>() {
                            Ok(x) => x,
                            Err(_) => {
                                println!("failed to parse float: {:x?}", cell.as_bytes());
                                return Err(EzError{tag: ErrorTag::Deserialization, text: (format!("Could not parse item at position: {}", index))});
                            }
                        };
                        outvec.push(temp);
                    }
                    DbColumn::Floats(outvec)
                }
                DbType::Int => {

                    let mut outvec = Vec::with_capacity(col.len());
                    for (index, cell) in col.iter().enumerate() {
                        // println!("index: {} - cell: {}",index, cell);
                        let temp = match cell.parse::<i32>() {
                            Ok(x) => x,
                            Err(_) => {
                                println!("failes to parse int: {}", cell);
                                return Err(EzError{tag: ErrorTag::Deserialization, text: (format!("Could not parse item at position: {}", index))});
                            },
                        };
                        outvec.push(temp);
                    }
                    DbColumn::Ints(outvec)
                }
                DbType::Text => {
                    let mut outvec = Vec::with_capacity(col.len());
                    for cell in col {
                        outvec.push(KeyString::from(cell));
                    }
                    DbColumn::Texts(outvec)
                }
            };

            result.insert(header.iter().nth(i).unwrap().name, db_vec);
        }

        let mut primary_key_index = None;
        for item in header.iter() {
            if item.key == TableKey::Primary {
                primary_key_index = Some(item.name);
            }
        }

        let primary_key_index = match primary_key_index {
            Some(x) => x,
            None => return Err(EzError{tag: ErrorTag::Deserialization, text: "No primary key specified".to_owned()})
        };

        match &result[&primary_key_index] {
            DbColumn::Ints(col) => {
                let mut test_set = HashSet::new();
                for item in col.iter() {
                    if test_set.contains(item) {
                        return Err(EzError{tag: ErrorTag::Deserialization, text: format!("Primary key is not unique. Item {} is repeated", item)})
                    }
                    test_set.insert(item);
                }
            }
            DbColumn::Texts(col) => {
                let mut test_set = HashSet::new();
                for item in col.iter() {
                    if test_set.contains(item) {
                        return Err(EzError{tag: ErrorTag::Deserialization, text: format!("Primary key is not unique. Item {} is repeated", item)})
                    }
                    test_set.insert(item);
                }
            }
            DbColumn::Floats(_) => unreachable!("Should never have a float primary key. Something went wrong in the parsing csv code near column {} line{}. Abort and crash.", column!(), line!()),
        }

        let header: BTreeSet<HeaderItem> = header.iter().cloned().collect();

        let mut output = ColumnTable {
            name: KeyString::from(table_name),
            header: header,
            columns: result,
        };
        output.sort();
        Ok(output)
    }

    /// Helper function to update a ColumnTable with a csv
    pub fn update_from_csv(&mut self, input_csv: &str) -> Result<(), EzError> {
        

        let update_table = ColumnTable::from_csv_string(input_csv, "update", "system")?;

        self.update(&update_table)?;

        Ok(())
    }

    pub fn insert(&mut self, inserts: ColumnTable) -> Result<(), EzError> {
        


        let mut input_table = inserts;

        let mut losers = Vec::new();

        match &input_table.columns[&input_table.get_primary_key_col_index()] {
            DbColumn::Ints(column) => {
                for item in column {
                    if let Some(index) = self.contains_key_i32(*item) {
                        losers.push(index);
                    }
                }
            },
            DbColumn::Texts(column) => {
                for item in column {
                    if let Some(index) = self.contains_key_string(*item) {
                        losers.push(index);
                    }
                }
            },
            DbColumn::Floats(_column) => unreachable!("There should never be a float primary key"),
        }

        input_table.delete_by_indexes(&losers);

        self.update(&input_table)?;

        Ok(())
    }

    pub fn contains_key_i32(&self, key: i32) -> Option<usize> {
        


        match &self.columns[&self.get_primary_key_col_index()] {
            DbColumn::Ints(column) => {
                match column.binary_search(&key) {
                    Ok(x) => Some(x),
                    Err(_) => None,
                }
            },
           _ => unreachable!("Already checked the key type earlier")
        }
    }

    pub fn contains_key_string(&self, key: KeyString) -> Option<usize> {
        


        match &self.columns[&self.get_primary_key_col_index()] {
            DbColumn::Texts(column) => {
                match column.binary_search(&key) {
                    Ok(x) => Some(x),
                    Err(_) => None,
                }
            },
           _ => unreachable!("Already checked the key type earlier")
        }
    }

    

    pub fn byte_size(&self) -> usize {
        


        let mut total = 0;
        
        for item in &self.header {
            total += item.name.as_bytes().len();
            total += 16; // DbType and TableKey are both raw enums which are 8 bytes in memory for the tag.
        }
        for column in self.columns.values() {
            match column {
                DbColumn::Ints(c) => total += c.len() * 4,
                DbColumn::Floats(c) => total += c.len() * 4,
                DbColumn::Texts(c) => total += c.len() * 64,
            }
        }

        total
    }

    /// utility function to get the index of the column with the primary key
    pub fn get_primary_key_col_index(&self) -> KeyString {
        

        
        for item in &self.header {
            if item.key == TableKey::Primary {
                return item.name;
            }
        }

        unreachable!("There should always be a primary key")
    }

    pub fn get_primary_key_type(&self) -> DbType {
        

        match self.columns[&self.get_primary_key_col_index()] {
            DbColumn::Ints(_) => DbType::Int,
            DbColumn::Texts(_) => DbType::Text,
            DbColumn::Floats(_) => unreachable!("There should never be a float primary key"),
        }
    }

    /// Updates a ColumnTable. Overwrites existing keys and adds new ones in proper order
    pub fn update(&mut self, other_table: &ColumnTable) -> Result<(), EzError> {
        


        if other_table.len() == 0 {
            return Err(EzError{tag: ErrorTag::Query, text: "Can't update anything with an empty table".to_owned()})
        }

        if self.header != other_table.header {
            return Err(EzError{tag: ErrorTag::Query, text: "Headers don't match".to_owned()})
        }

        let self_primary_key_index = self.get_primary_key_col_index();

        let record_vec: Vec<u8>;
        match self.columns.get_mut(&self_primary_key_index).unwrap() {
            DbColumn::Ints(col) => match &other_table.columns[&self_primary_key_index] {
                DbColumn::Ints(other_col) => {
                    
                    (*col, record_vec) = merge_sorted(col, other_col);
                }
                _ => unreachable!("Should always have the same primary key column"),
            },
            DbColumn::Texts(col) => match &other_table.columns[&self_primary_key_index] {
                DbColumn::Texts(other_col) => {
                    
                    (*col, record_vec) = merge_sorted(col, other_col);
                }
                _ => unreachable!("Should always have the same primary key column"),
            },
            DbColumn::Floats(_) => unreachable!("Should never have a float primary key column"),
        }

        let pk = self.get_primary_key_col_index();
        for (key, column) in self.columns.iter_mut() {
           if key == &pk {
            continue
           }
            match column {
                DbColumn::Ints(col) => match &other_table.columns[key] {
                    DbColumn::Ints(other_col) => {
                        *col = merge_in_order(col, other_col, &record_vec);
                    }
                    _ => unreachable!("Should always have the same type column"),
                },
                DbColumn::Texts(col) => match &other_table.columns[key] {
                    DbColumn::Texts(other_col) => {
                        *col = merge_in_order(col, other_col, &record_vec);
                    }
                    _ => unreachable!("Should always have the same type column"),
                },
                DbColumn::Floats(col) => match &other_table.columns[key] {
                    DbColumn::Floats(other_col) => {
                        *col = merge_in_order(col, other_col, &record_vec);
                    }
                    _ => unreachable!("Should always have the same type column"),
                },
            }
        }

        Ok(())
    }

    pub fn key_index(&self, key: &KeyString) -> Option<usize> {
        

        match &self.columns[&self.get_primary_key_col_index()] {
            DbColumn::Ints(column) => {
                match column.binary_search(&key.to_i32()) {
                    Ok(x) => Some(x),
                    Err(_) => None
                }
            },
            DbColumn::Texts(column) => {
                match column.binary_search(key) {
                    Ok(x) => Some(x),
                    Err(_) => None
                }
            },
            DbColumn::Floats(_) => unreachable!("The should never be a primary key"),
        }
    }

    /// Utility function to get the length of the database columns.
    pub fn len(&self) -> usize {
        

        match &self.columns.values().next() {
            Some(column) => match column {
                DbColumn::Floats(col) => col.len(),
                DbColumn::Ints(col) => col.len(),
                DbColumn::Texts(col) => col.len(),
            },
            None => 0,
        }
    }

    /// Sorts all the columns in the table by the primary key. This was tricky to write.
    pub fn sort(&mut self) {
        

        let len = self.len();

        let mut indexer: Vec<usize> = (0..len).collect();

        let primary_index = self.get_primary_key_col_index();

        let vec = self.columns.get_mut(&primary_index).unwrap();
        match vec {
            DbColumn::Ints(col) => {
                indexer.sort_unstable_by_key(|&i| col[i]);
            }
            DbColumn::Texts(col) => {
                indexer.sort_unstable_by_key(|&i| &col[i]);
            }
            DbColumn::Floats(_) => {
                unreachable!("There should never be a float primary key");
            }
        }

        for column in self.columns.iter_mut() {
            match column.1 {
                DbColumn::Floats(col) => {
                    // println!("float!");
                    rearrange_by_index(col, &indexer);
                }
                DbColumn::Ints(col) => {
                    // println!("int!");
                    rearrange_by_index(col, &indexer);
                }
                DbColumn::Texts(col) => {
                    // println!("text!");
                    rearrange_by_index(col, &indexer);
                }
            }
        };
    }

    /// Gets a single line from the table as a csv String.
    pub fn get_line(&self, index: usize) -> Result<String, EzError> {
        

        if index > self.len() {
            return Err(EzError{tag: ErrorTag::Query, text: "Index larger than data".to_owned()})
        }

        let mut output = String::new();
        for v in self.columns.values() {
            match v {
                DbColumn::Floats(col) => {
                    let item = col[index];
                    output.push_str(&item.to_string());
                }
                DbColumn::Ints(col) => {
                    let item = col[index];
                    output.push_str(&item.to_string());
                }
                DbColumn::Texts(col) => {
                    let item = &col[index];
                    output.push_str(item.as_str());
                }
            }

            output.push(';');
        }
        output.pop();

        Ok(output)
    }
    
    pub fn get_column_int<'a>(&'a self, index: &KeyString) -> Result<&'a Vec<i32>, EzError> {
        match self.columns.get(index) {
            Some(dbcol) => match dbcol {
                DbColumn::Ints(column) => Ok(column),
                DbColumn::Texts(_) => Err(EzError{tag: ErrorTag::Structure, text: "Wrong column type".to_owned()}),
                DbColumn::Floats(_) => Err(EzError{tag: ErrorTag::Structure, text: "Wrong column type".to_owned()}),
            },
            None => Err(EzError{tag: ErrorTag::Structure, text: format!("No such column as {}", index)})
        }

    }

    pub fn get_column_text<'a>(&'a self, index: &KeyString) -> Result<&'a Vec<KeyString>, EzError> {
        match self.columns.get(index) {
            Some(dbcol) => match dbcol {
                DbColumn::Texts(column) => Ok(column),
                DbColumn::Ints(_) => Err(EzError{tag: ErrorTag::Structure, text: "Wrong column type".to_owned()}),
                DbColumn::Floats(_) => Err(EzError{tag: ErrorTag::Structure, text: "Wrong column type".to_owned()}),
            },
            None => Err(EzError{tag: ErrorTag::Structure, text: format!("No such column as {}", index)})
        }

    }

    pub fn get_column_float<'a>(&'a self, index: &KeyString) -> Result<&'a Vec<f32>, EzError> {
        

        match self.columns.get(index) {
            Some(dbcol) => match dbcol {
                DbColumn::Floats(column) => Ok(column),
                DbColumn::Texts(_) => Err(EzError{tag: ErrorTag::Structure, text: "Wrong column type".to_owned()}),
                DbColumn::Ints(_) => Err(EzError{tag: ErrorTag::Structure, text: "Wrong column type".to_owned()}),
            },
            None => Err(EzError{tag: ErrorTag::Structure, text: format!("No such column as {}", index)})
        }

    }

    /// Gets a list of items from the table and returns a csv string containing them
    pub fn query_list(&self, mut key_list: Vec<&str>) -> Result<String, EzError> {
        

        let mut printer = String::new();
        let primary_index = self.get_primary_key_col_index();
        key_list.sort();

        let mut indexes = Vec::new();
        for item in key_list {
            match &self.columns[&primary_index] {
                DbColumn::Floats(_) => return Err(EzError{tag: ErrorTag::Structure, text: "There should never be a float primary key".to_owned()}),
                DbColumn::Ints(col) => {
                    let key: i32 = match item.parse::<i32>() {
                        Ok(num) => num,
                        Err(_) => continue,
                    };
                    let index: usize = match col.binary_search(&key) {
                        Ok(num) => num,
                        Err(_) => continue,
                    };
                    indexes.push(index);
                }

                DbColumn::Texts(col) => {
                    let index: usize = match col.binary_search(&KeyString::from(item)) {
                        Ok(num) => num,
                        Err(_) => continue,
                    };
                    indexes.push(index);
                }
            }
        }

        for index in indexes {
            for v in self.columns.values() {
                match v {
                    DbColumn::Floats(col) => printer.push_str(&col[index].to_string()),
                    DbColumn::Ints(col) => printer.push_str(&col[index].to_string()),
                    DbColumn::Texts(col) => printer.push_str(col[index].as_str()),
                }
                printer.push(';');
            }
            printer.pop();
            printer.push('\n');
        }
        printer.pop();

        Ok(printer)
    }

    pub fn subtable_from_indexes(&self, indexes: &[usize], new_name: &KeyString) -> ColumnTable {
        

        let mut result_columns = BTreeMap::new();

        for (key, column) in self.columns.iter() {
            for index in indexes {
                assert!(*index < self.len());
                match column {
                    DbColumn::Ints(column) => {
                        let mut temp = Vec::with_capacity(indexes.len());
                        for index in indexes {
                            temp.push(column[*index]);
                        }
                        result_columns.insert(*key, DbColumn::Ints(temp));
                    },
                    DbColumn::Floats(column) => {
                        let mut temp = Vec::with_capacity(indexes.len());
                        for index in indexes {
                            temp.push(column[*index]);
                        }
                        result_columns.insert(*key, DbColumn::Floats(temp));
                    },
                    DbColumn::Texts(column) => {
                        let mut temp = Vec::with_capacity(indexes.len());
                        for index in indexes {
                            temp.push(column[*index]);
                        }
                        result_columns.insert(*key, DbColumn::Texts(temp));
                    },
                }
            }
        }

        ColumnTable {
            name: *new_name,
            header: self.header.clone(),
            columns: result_columns,
        }
    }

    pub fn subtable_from_columns(&self, columns: &[KeyString], new_name: &str) -> Result<ColumnTable, EzError> {
        

        let mut new_table_inner = BTreeMap::new();
        let mut new_table_header = BTreeSet::new();

        if columns.is_empty() {
            return Err(EzError{tag: ErrorTag::Query, text: "No columns specified. If you want all columns, us '*'".to_owned()})
        }

        if columns[0].as_str() == "*" || columns[0].as_str() == "*" {
            return Ok(
                ColumnTable {
                    name: KeyString::from(new_name),
                    header: self.header.clone(),
                    columns: self.columns.clone(),
                }
            )
        }

        for column in columns {
            match self.columns.get(column) {
                Some(col) => {
                    new_table_inner.insert(*column, col.clone());
                    let header_item = self.header
                        .iter()
                        .find(|&x| x.name==*column)
                        .expect("This is safe since the header must always have a corresponding entry to the column name")
                        .clone();
                    new_table_header.insert(header_item);
                },
                None => return Err(EzError{tag: ErrorTag::Query, text: format!("No such column as {}", column)})
            };
        }

        Ok(
            ColumnTable {
                name: KeyString::from(new_name),
                header: new_table_header,
                columns: new_table_inner,
            }
        )
    }

    // /// Gets a range of items from the table and returns a csv String containing them
    // pub fn query_range(&self, range: (&str, &str)) -> Result<String, EzError> {
        

    //     let mut printer = String::new();

    //     if range.1 < range.0 {
    //         return Err(EzError{tag: ErrorTag::Query, text: "Table is empty".to_owned())})
    //     }

    //     if range.0 == range.1 {
    //         return self.query(range.0);
    //     }

    //     let primary_index = self.get_primary_key_col_index();

    //     let mut indexes: [usize; 2] = [0, 0];
    //     match &self.columns[&primary_index] {
    //         DbColumn::Floats(_) => return Err(EzError{tag: ErrorTag::Structure, text: "There should never be a float primary key".to_owned()}),
    //         DbColumn::Ints(col) => {
    //             let key = match range.0.parse::<i32>() {
    //                 Ok(num) => num,
    //                 Err(_) => return Err(EzError{tag: ErrorTag::Query, text: "Table is empty".to_owned())})
    //             };
    //             let index: usize = col.partition_point(|n| n < &key);
    //             indexes[0] = index;

    //             if range.1.is_empty() {
    //                 indexes[1] = col.len();
    //             } else {
    //                 let key2 = match range.1.parse::<i32>() {
    //                     Ok(num) => num,
    //                     Err(_) => return Err(EzError::WrongKey),
    //                 };
    //                 // // println!("key2: {}", key2);
    //                 let index: usize = col.partition_point(|n| n < &key2);
    //                 if col[index] == key2 {
    //                     indexes[1] = index;
    //                 } else {
    //                     indexes[1] = index - 1;
    //                 }
    //             }
    //         }
    //         DbColumn::Texts(col) => {
    //             let index: usize = col.partition_point(|n| n < &KeyString::from(range.0));
    //             indexes[0] = index;

    //             if range.1.is_empty() {
    //                 indexes[1] = col.len();
    //             }

    //             let index: usize = col.partition_point(|n| n < &KeyString::from(range.1));

    //             if col[index] == KeyString::from(range.1) {
    //                 indexes[1] = index;
    //             } else {
    //                 indexes[1] = index - 1;
    //             }

    //             indexes[1] = index;
    //         }
    //     }

    //     let mut i = indexes[0];
    //     while i <= indexes[1] {
    //         for v in self.columns.values() {
    //             match v {
    //                 DbColumn::Floats(col) => printer.push_str(&col[i].to_string()),
    //                 DbColumn::Ints(col) => printer.push_str(&col[i].to_string()),
    //                 DbColumn::Texts(col) => printer.push_str(col[i].as_str()),
    //             }
    //             printer.push(';');
    //         }
    //         printer.pop();
    //         printer.push('\n');
    //         i += 1;
    //     }
    //     printer.pop();

    //     Ok(printer)
    // }

    /// Gets one item from the list. Same as get_line. I should get rid of this but right now I'm commenting...
    pub fn query(&self, query: &str) -> Result<String, EzError> {
        

        self.query_list(Vec::from([query]))
    }

    pub fn copy_lines(&self, target: &mut ColumnTable, line_keys: &DbColumn) -> Result<(), EzError> {
        if target.header != self.header {
            return Err(EzError{tag: ErrorTag::Query, text: "Target table header does not match source table header.".to_owned()})
        }

        let mut temp_table = ColumnTable {
            name: KeyString::from("none"),
            header: target.header.clone(),
            columns: BTreeMap::new(),
        };

        let mut temp_tree = BTreeMap::new();
        for item in &self.header {
            match item.kind {
                DbType::Int => temp_tree.insert(item.name, DbColumn::Ints(Vec::with_capacity(line_keys.len()))),
                DbType::Float => temp_tree.insert(item.name, DbColumn::Floats(Vec::with_capacity(line_keys.len()))),
                DbType::Text => temp_tree.insert(item.name, DbColumn::Texts(Vec::with_capacity(line_keys.len()))),
            };
        }

        temp_table.columns = temp_tree;

        let pk_index = self.get_primary_key_col_index();

        let mut indexes: Vec<usize> = Vec::with_capacity(line_keys.len());

        match line_keys {
            DbColumn::Ints(col) => {
                let source_col = match &self.columns[&pk_index] {
                    DbColumn::Ints(col) => col,
                    _ => return Err(EzError{tag: ErrorTag::Structure, text: "Source and target table do not have matching primary key types".to_owned()}),
                };
                for key in col {
                    match source_col.binary_search(key) {
                        Ok(i) => indexes.push(i),
                        Err(_) => continue,
                    }
                }
            },
            DbColumn::Texts(col) => {
                let source_col = match &self.columns[&pk_index] {
                    DbColumn::Texts(col) => col,
                    _ => return Err(EzError{tag: ErrorTag::Structure, text: "Source and target table do not have matching primary key types".to_owned()}),
                };
                for key in col {
                    match source_col.binary_search(key) {
                        Ok(i) => indexes.push(i),
                        Err(_) => continue,
                    }
                }
            },
            _ => unreachable!("Should never have a float primary key."),
        }

        for (key, column) in self.columns.iter() {
            match column {
                DbColumn::Floats(col) => {
                    for index in &indexes {
                        match temp_table.columns.get_mut(key).unwrap() {
                            DbColumn::Floats(temp) => temp.push(col[*index]),
                            _ => unreachable!("Source and target column should always have the same type"),
                        }
                    }
                },
                DbColumn::Ints(col) => {
                    for index in &indexes {
                        match temp_table.columns.get_mut(key).unwrap() {
                            DbColumn::Ints(temp) => temp.push(col[*index]),
                            _ => unreachable!("Source and target column should always have the same type"),
                        }
                    }
                },
                DbColumn::Texts(col) => {
                    for index in &indexes {
                        match temp_table.columns.get_mut(key).unwrap() {
                            DbColumn::Texts(temp) => temp.push(col[*index]),
                            _ => unreachable!("Source and target column should always have the same type"),
                        }
                    }
                },
            }
        }

        // println!("Source:\n{}", self);
        // println!();
        // println!();
        // println!("temp_table:\n{}", temp_table);
        // println!();
        // println!();
        // println!("Target:\n{}", target);
        // println!();
        // println!();
        target.update(&temp_table)?;
        // println!("Updated Target:\n{}", target);


        Ok(())
    }

    pub fn create_subtable_from_index_range(&self, start: usize, mut stop: usize) -> ColumnTable {
        


        if stop >= self.len() {
            stop = self.len();
        }
        assert!(stop >= start);

        let mut subtable = BTreeMap::new();

        for (key, v) in self.columns.iter() {
            match v {
                DbColumn::Ints(column) => {
                    subtable.insert(*key, DbColumn::Ints(column[start..stop].to_vec()));
                },
                DbColumn::Floats(column) => {
                    subtable.insert(*key, DbColumn::Floats(column[start..stop].to_vec()));
                },
                DbColumn::Texts(column) => {
                    subtable.insert(*key, DbColumn::Texts(column[start..stop].to_vec()));
                },
            }
        }
        
        ColumnTable {
            name: KeyString::from("subtable"),
            header: self.header.clone(),
            columns: subtable,
        }

    }

    /// Deletes a range of rows by primary key from the table
    pub fn delete_range(&mut self, range: (&str, &str)) -> Result<(), EzError> {
        

        // Up to but not including.
        // Up to but not including!!
        // UP TO BUT NOT INCLUDING!!!

        if range.1 < range.0 {
            return Err(EzError{tag: ErrorTag::Query, text: "Range is invalid. Start is higher than stop".to_owned()})
        }

        if range.0 == range.1 {
            return self.delete(range.0);
        }

        let primary_index = self.get_primary_key_col_index();

        let mut indexes: [usize; 2] = [0, 0];
        match &self.columns[&primary_index] {
            DbColumn::Floats(_) => return Err(EzError{tag: ErrorTag::Structure, text: "There should never be a float primary key".to_owned()}),
            DbColumn::Ints(col) => {
                let key = match range.0.parse::<i32>() {
                    Ok(num) => num,
                    Err(_) => return Err(EzError{tag: ErrorTag::Structure, text: format!("start: '{}' could not be parsed as i32", range.0)}),
                };
                let index: usize = col.partition_point(|n| *n < key);
                indexes[0] = index;

                if range.1.is_empty() {
                    indexes[1] = col.len();
                } else {
                    let key2 = match range.1.parse::<i32>() {
                        Ok(num) => num,
                        Err(_) => return Err(EzError{tag: ErrorTag::Structure, text: format!("start: '{}' could not be parsed as i32", range.1)}),
                    };
                    // // println!("key2: {}", key2);
                    let index: usize = col.partition_point(|n| n < &key2);
                    indexes[1] = index;
                }
            }
            DbColumn::Texts(col) => {
                let index: usize = col.partition_point(|n| n < &KeyString::from(range.0));
                indexes[0] = index;

                if range.1.is_empty() {
                    indexes[1] = col.len();
                }

                let index: usize = col.partition_point(|n| n < &KeyString::from(range.1));

                if col[index] == KeyString::from(range.1) {
                    indexes[1] = index;
                } else {
                    indexes[1] = index - 1;
                }

                indexes[1] = index;
            }
        }

        for col in self.columns.values_mut() {
            match col {
                DbColumn::Floats(v) => {
                    v.drain(indexes[0]..indexes[1]);
                }
                DbColumn::Ints(v) => {
                    v.drain(indexes[0]..indexes[1]);
                }
                DbColumn::Texts(v) => {
                    v.drain(indexes[0]..indexes[1]);
                }
            };
        }

        Ok(())
    }

    /// Deletes a list of rows by primary key from the database
    pub fn delete_list(&mut self, mut key_list: Vec<&str>) -> Result<(), EzError> {
        

        let primary_index = self.get_primary_key_col_index();
        key_list.sort();

        let mut indexes = Vec::new();
        for item in key_list {
            match &self.columns[&primary_index] {
                DbColumn::Floats(_) => return Err(EzError{tag: ErrorTag::Structure, text: "There should never be a float primary key".to_owned()}),
                DbColumn::Ints(col) => {
                    let key: i32 = match item.parse::<i32>() {
                        Ok(num) => num,
                        Err(_) => continue,
                    };

                    let index: usize = match col.binary_search(&key) {
                        Ok(num) => num,
                        Err(_) => continue,
                    };
                    indexes.push(index);
                }

                DbColumn::Texts(col) => {
                    let index: usize = match col.binary_search(&KeyString::from(item)) {
                        Ok(num) => num,
                        Err(_) => continue,
                    };
                    indexes.push(index);
                }
            }
        }

        let imut = self.columns.values_mut();
        for col in imut {
            match col {
                DbColumn::Floats(v) => {
                    remove_indices(v, &indexes);
                }
                DbColumn::Ints(v) => {
                    remove_indices(v, &indexes);
                }
                DbColumn::Texts(v) => {
                    remove_indices(v, &indexes);
                }
            };
        }

        Ok(())
    }


    pub fn delete_by_vec(&mut self, key_list: DbColumn) -> Result<(), EzError> {
        

        let primary_index = self.get_primary_key_col_index();

        let mut indexes = Vec::with_capacity(key_list.len());
        match key_list {
            DbColumn::Ints(mut column) => {
                column.sort();
                for item in column {
                    match &self.columns[&primary_index] {
                        DbColumn::Ints(col) => {

                            let index: usize = match col.binary_search(&item) {
                                Ok(num) => num,
                                Err(_) => continue,
                            };
                            indexes.push(index);
                        },
                        _ => unreachable!(
                            "If we ever get here then the table is invalid. Crash immediately.\n###################\nTable name: {}\n##########################"
                            , self.name
                        ),
                    }
                }
            },
            DbColumn::Texts(mut column) => {
                column.sort();
                for item in column {
                    match &self.columns[&primary_index] {
                        DbColumn::Texts(col) => {
                            let index: usize = match col.binary_search(&item) {
                                Ok(num) => num,
                                Err(_) => continue,
                            };
                            indexes.push(index);
                        },
                        _ => unreachable!(
                            "If we ever get here then the table is invalid. Crash immediately.\n###################\nTable name: {}\n##########################"
                            , self.name
                        ),
                    }
                }
            },
            DbColumn::Floats(_) => unreachable!(
                "If we ever get here then the table is invalid. Crash immediately.\n###################\nTable name: {}\n##########################"
                , self.name
            ),
        }

        let imut = self.columns.values_mut();
        for col in imut {
            match col {
                DbColumn::Floats(v) => {
                    remove_indices(v, &indexes);
                }
                DbColumn::Ints(v) => {
                    remove_indices(v, &indexes);
                }
                DbColumn::Texts(v) => {
                    remove_indices(v, &indexes);
                }
            };
        }

        Ok(())
    }

    pub fn delete_by_indexes(&mut self, indexes: &[usize]) {
        

        let imut = self.columns.values_mut();
        for col in imut {
            match col {
                DbColumn::Floats(v) => {
                    remove_indices(v, indexes);
                }
                DbColumn::Ints(v) => {
                    remove_indices(v, indexes);
                }
                DbColumn::Texts(v) => {
                    remove_indices(v, indexes);
                }
            };
        }
    }


    /// Deletes a single row from the table by primary key
    fn delete(&mut self, query: &str) -> Result<(), EzError> {
        

        self.delete_list(Vec::from([query]))
    }

    pub fn clear(&mut self) {
        

        for column in self.columns.values_mut() {
            match column {
                DbColumn::Ints(col) => {
                    *col = Vec::with_capacity(0);
                },
                DbColumn::Floats(col) => {
                    *col = Vec::with_capacity(0);
                },
                DbColumn::Texts(col) => {
                    *col = Vec::with_capacity(0);
                },
            }
        }
    }

    pub fn add_column(&mut self, name: KeyString, column: DbColumn) -> Result<(), EzError> {
        
        let kind = match column {
            DbColumn::Ints(_) => DbType::Int,
            DbColumn::Texts(_) => DbType::Text,
            DbColumn::Floats(_) => DbType::Float,
        };

        if self.columns.is_empty() {
            self.header.insert(HeaderItem {
                name: name,
                key: TableKey::Primary,
                kind: kind,
            });
            self.columns.insert(name, column);
        } else {
            if self.len() != column.len() {
                return Err(EzError{tag: ErrorTag::Structure, text: format!("Attempting to add an uneven column.\nExisting columns: '{}'\nNew_column: '{}'", self.len(), column.len())})
            }

            self.header.insert(HeaderItem {
                name: name,
                key: TableKey::None,
                kind: kind,
            });
            self.columns.insert(name, column);

        }



        Ok(())
    }


    pub fn alt_left_join(&mut self, right_table: &ColumnTable, predicate_column: &KeyString) -> Result<(), EzError> {

        match self.columns.keys().find(|x| **x == *predicate_column) {
            Some(_) => (),
            None => return Err(EzError{tag: ErrorTag::Query, text: "Predicate column is not common".to_owned()})
        };

        match right_table.columns.keys().find(|x| **x == *predicate_column) {
            Some(_) => (),
            None => return Err(EzError{tag: ErrorTag::Query, text: "Predicate column is not common".to_owned()})
        };

        
        let mut indexes: Vec<usize> = Vec::with_capacity(self.len());
        match &self.columns[predicate_column] {
            DbColumn::Ints(column) => {
                let right_col = right_table.get_column_int(predicate_column)?;
                let mut lookup = HashMap::with_capacity(right_col.len());
                for (index, item) in right_col.iter().enumerate() {
                    lookup.insert(item, index);
                }

                for item in column {
                    indexes.push(lookup[item]);
                }
            },
            DbColumn::Texts(column) => {
                let right_col = right_table.get_column_text(predicate_column)?;
                let mut lookup = HashMap::with_capacity(right_col.len());
                for (index, item) in right_col.iter().enumerate() {
                    lookup.insert(item, index);
                }

                for item in column {
                    indexes.push(lookup[item]);
                }
            },
            DbColumn::Floats(_column) => unreachable!("Can never have a float key column"),
        }
        
        for (name, column) in right_table.columns.iter() {
            if name == predicate_column {
                continue
            }

            match column {
                DbColumn::Ints(col) => {
                    let mut new_column = Vec::with_capacity(indexes.len());
                    for index in &indexes {
                        new_column.push(col[*index]);
                    }
                    self.add_column(*name, DbColumn::Ints(new_column))?;
                },
                DbColumn::Texts(col) => {
                    let mut new_column = Vec::with_capacity(indexes.len());
                    for index in &indexes {
                        new_column.push(col[*index]);
                    }
                    self.add_column(*name, DbColumn::Texts(new_column))?;
                },
                DbColumn::Floats(col) => {
                    let mut new_column = Vec::with_capacity(indexes.len());
                    for index in &indexes {
                        new_column.push(col[*index]);
                    }
                    self.add_column(*name, DbColumn::Floats(new_column))?;
                },
            }
        }

        Ok(())
    }


    pub fn left_join(&mut self, right_table: &ColumnTable, predicate_column: &KeyString) -> Result<(), EzError> {
        


        match self.columns.keys().find(|x| **x == *predicate_column) {
            Some(_) => (),
            None => return Err(EzError{tag: ErrorTag::Query, text: "Predicate column is not common".to_owned()})
        };

        match right_table.columns.keys().find(|x| **x == *predicate_column) {
            Some(_) => (),
            None => return Err(EzError{tag: ErrorTag::Query, text: "Predicate column is not common".to_owned()})
        };

        
        let mut indexes: Vec<usize> = Vec::with_capacity(self.len());
        match &self.columns[predicate_column] {
            DbColumn::Ints(column) => {
                let right_col = right_table.get_column_int(predicate_column)?;
                let mut lookup = HashMap::with_capacity(right_col.len());
                for item in column.iter() {
                    if lookup.contains_key(item) {
                        indexes.push(lookup[item]);
                    } else {
                        match right_col.binary_search(item) {
                            Ok(x) => {
                                indexes.push(x);
                                lookup.insert(item, x);
                            },
                            Err(_) => todo!("This should only happen if the database is out of sync. Off key was {}", item),
                        };
                    }
                }
            },
            DbColumn::Texts(column) => {
                let right_col = right_table.get_column_text(predicate_column)?;
                let mut lookup = HashMap::with_capacity(right_col.len());
                for item in column.iter() {
                    if lookup.contains_key(item) {
                        indexes.push(lookup[item]);
                    } else {
                        match right_col.binary_search(item) {
                            Ok(x) => {
                                indexes.push(x);
                                lookup.insert(item, x);
                            },
                            Err(_) => todo!("This should only happen if the database is out of sync"),
                        };
                    }
                }
            },
            DbColumn::Floats(_column) => unreachable!("Can never have a float key column"),

        }
        
        for (name, column) in right_table.columns.iter() {
            if name == predicate_column {
                continue
            }

            match column {
                DbColumn::Ints(col) => {
                    let mut new_column = Vec::with_capacity(indexes.len());
                    for index in &indexes {
                        new_column.push(col[*index]);
                    }
                    self.add_column(*name, DbColumn::Ints(new_column))?;
                },
                DbColumn::Texts(col) => {
                    let mut new_column = Vec::with_capacity(indexes.len());
                    for index in &indexes {
                        new_column.push(col[*index]);
                    }
                    self.add_column(*name, DbColumn::Texts(new_column))?;
                },
                DbColumn::Floats(col) => {
                    let mut new_column = Vec::with_capacity(indexes.len());
                    for index in &indexes {
                        new_column.push(col[*index]);
                    }
                    self.add_column(*name, DbColumn::Floats(new_column))?;
                },
            }
        }

        Ok(())
    }

    pub fn size_of_table(&self) -> usize {
        let mut acc = 128; // the table name and the packet type are 64 byte KeyStrings 

        acc += self.header.len() * 72;

        acc += 16 + 64; // Length of metadata

        for (_, col) in &self.columns {
            acc += 64;
            match col {
                DbColumn::Ints(vec) => acc += vec.len() * 4,
                DbColumn::Texts(vec) => acc += vec.len() * 64,
                DbColumn::Floats(vec) => acc += vec.len() * 4,
            }
        }

        acc
    }

    pub fn size_of_row(&self) -> usize {
        
        let mut acc = 0;
        
        for (_, col) in &self.columns {
            match col {
                DbColumn::Ints(_) => acc += 4,
                DbColumn::Texts(_) => acc += 64,
                DbColumn::Floats(_) => acc += 4,
            }
        }

        acc
    }

    /// Writes to EZ binary format
    pub fn to_binary(&self) -> Vec<u8> {
        
        let mut binary: Vec<u8> = Vec::with_capacity(self.size_of_table());
        
        write_column_table_binary_header(&mut binary, self);
        
        // WRITING COLUMNS
        for column in self.columns.values() {
            match &column {
                DbColumn::Floats(col) => {

                    for item in col {
                        binary.extend_from_slice(&item.to_le_bytes());
                    }
                }
                &DbColumn::Ints(col) => {
                    for item in col {
                        // println!("item: {}", item);
                        binary.extend_from_slice(&item.to_le_bytes());
                    }
                }
                DbColumn::Texts(col) => {
                    for item in col {
                        binary.extend_from_slice(item.raw());
                    }
                }
            };
        }
        binary
    }


    /// Reads an EZ binary formatted file to a ColumnTable, checking for strictness.
    pub fn from_binary(name: Option<&str>, binary: &[u8]) -> Result<ColumnTable, EzError> {

        if binary.len() < 128 + 8 + 8 {
            return Err(EzError{tag: ErrorTag::Deserialization, text: ("binary is less than 144 bytes".to_owned())});
        }

        let packet_type = match KeyString::try_from(&binary[0..64]) {
            Ok(x) => x,
            Err(_) => return Err(EzError{tag: ErrorTag::Deserialization, text: ("Packet_type corrupted".to_owned())}),
        };

        let mut table_name = KeyString::try_from(&binary[64..128])?;
        match packet_type.as_str() {
            "EZDB_COLUMNTABLE" => (),
            _ => return Err(EzError{tag: ErrorTag::Deserialization, text: "Not ColumnTable".to_owned()})
        };

        let header_len = u64_from_le_slice(&binary[128..136]) as usize;
        let column_len = u64_from_le_slice(&binary[136..144]) as usize;

        let keys_and_kinds = &binary[144..144+header_len*8];
        let mut acc_kk = Vec::new();
        for chunk in keys_and_kinds.chunks(8) {
            let kind = match chunk[3] {
                b'i' => DbType::Int,
                b'f' => DbType::Float,
                b't' => DbType::Text,
                _ => panic!("TODO: Make this a proper error"),
            };
            let key = match chunk[7] {
                b'P' => TableKey::Primary,
                b'N' => TableKey::None,
                b'F' => TableKey::Foreign,
                _ => panic!("TODO: Make this a proper error"),
            };
            acc_kk.push((kind, key));
        }

        let header_names = &binary[144+header_len*8..144+header_len*8 + header_len*64];
        
        let mut names = Vec::new();
        for chunk in header_names.chunks_exact(64) {
            names.push(KeyString::try_from(chunk).unwrap());
        }

        let mut header = BTreeSet::new();

        for i in 0..header_len {
            header.insert(HeaderItem{name: names[i], kind: acc_kk[i].0, key: acc_kk[i].1 });
        }

        println!("HEADER: {:?}", header);

        let mut columns = BTreeMap::new();

        let mut pointer = 144+header_len*8 + header_len*64;
        for item in &header {
            match item.kind {
                DbType::Int => {
                    let blob = &binary[pointer..pointer + (column_len * 4)];
                    let v = blob.chunks(4).map(i32_from_le_slice).collect();
                    
                    columns.insert(item.name, DbColumn::Ints(v));
                    pointer += column_len*4;
                }
                DbType::Float => {
                    let blob = &binary[pointer..pointer + (column_len * 4)];
                    let v = blob.chunks(4).map(f32_from_le_slice).collect();
                    
                    columns.insert(item.name, DbColumn::Floats(v));
                    pointer += column_len*4;
                }
                DbType::Text => {
                    let blob = &binary[pointer..pointer + column_len*64];
                    let v: Result<Vec<KeyString>, EzError> = blob.chunks(64).map(KeyString::try_from).collect();
                    let v = v?;
                    pointer += column_len * 64;
                    columns.insert(item.name, DbColumn::Texts(v));
                }
            }
        }

        if name.is_some() {
            table_name = ksf(name.unwrap());
        }

        let new_table = ColumnTable {
            name: table_name,
            header,
            columns,
        };

        Ok(new_table)
    }

    
}

pub fn write_column_table_binary_header(binary: &mut Vec<u8>, table: &ColumnTable) -> usize {
    
    binary.extend_from_slice(ksf("EZDB_COLUMNTABLE").raw());
    binary.extend_from_slice(table.name.raw());
    
    // WRITING LENGTHS
    binary.extend_from_slice(&table.header.len().to_le_bytes());
    binary.extend_from_slice(&table.len().to_le_bytes());
    
    // WRITING TABLE NAME
    
    // WRITING HEADER
    let mut keys_and_kinds = Vec::new();
    let mut names = Vec::new();
    for item in &table.header {
        let kind = match item.kind {
            DbType::Int => b'i',
            DbType::Float => b'f',
            DbType::Text => b't',
        };
        let key_type = match &item.key {
            TableKey::Primary => b'P',
            TableKey::None => b'N',
            TableKey::Foreign => b'F',
        };
        keys_and_kinds.extend_from_slice(&[0,0,0,kind,0,0,0,key_type]);
        names.extend_from_slice(item.name.raw());
    }
    binary.extend_from_slice(&keys_and_kinds);
    binary.extend_from_slice(&names);
    
    128 + table.header.len()+80
} 


pub struct DbRow<'a> {
    inner: &'a [u8],
}


pub struct RowTable {
    arena: bumpalo::Bump
}


pub fn write_subtable_to_raw_binary(subtable: &ColumnTable) -> Vec<u8> {
    let mut total_bytes = 0;

        let length = subtable.len();
        for item in subtable.columns.values() {
            match item {
                DbColumn::Texts(_) => {
                    total_bytes += length * 64;
                }
                _ => {
                    total_bytes += length * 4;
                }
            };
        }

        let mut output: Vec<u8> = Vec::with_capacity(total_bytes);

        for column in subtable.columns.values() {
            match &column {
                DbColumn::Floats(col) => {
                    for item in col {
                        output.extend_from_slice(&item.to_le_bytes());
                    }
                }
                &DbColumn::Ints(col) => {
                    for item in col {
                        output.extend_from_slice(&item.to_le_bytes());
                    }
                }
                DbColumn::Texts(col) => {
                    for item in col {
                        output.extend_from_slice(item.raw());
                    }
                }
            };
        }
        output
}


pub fn subtable_from_keys(table: &ColumnTable, mut keys: Vec<KeyString>) -> Result<ColumnTable, EzError> {
    let mut indexes = Vec::new();
    match table.get_primary_key_type() {
        DbType::Int => {
            let mut int_keys = Vec::new();
            for key in keys {
                match key.to_i32_checked() {
                    Ok(x) => int_keys.push(x),
                    Err(e) => return Err(EzError{tag: ErrorTag::Query, text: format!("Invalid int: {e}")})
                }
            }
            int_keys.sort();
            let mut key_pointer = 0;
            let col = table.get_column_int(&table.get_primary_key_col_index()).unwrap();
            for index in 0..col.len() {
                if int_keys[key_pointer] == col[index] {
                    indexes.push(index);
                    key_pointer += 1
                }
            }

        },
        DbType::Text => {
            keys.sort();
            let mut key_pointer = 0;
            let col = table.get_column_text(&table.get_primary_key_col_index()).unwrap();
            for index in 0..col.len() {
                if keys[key_pointer] == col[index] {
                    indexes.push(index);
                    key_pointer += 1
                }
            }
        },
        DbType::Float => unreachable!(),
    };

    Ok(
        table.subtable_from_indexes(&indexes, &KeyString::from("__RESULT__"))
    )
}

pub fn table_from_inserts(value_columns: &[KeyString], values: &str, table_name: &str) -> Result<ColumnTable, EzError> {
    let mut new_header = Vec::new();

    let first_line = match values.split('\n').next() {
        Some(x) => x,
        None => return Err(EzError{tag: ErrorTag::Deserialization, text: ("Empty input".to_owned())}),
    };

    let mut i = 0;
    for value in first_line.split(';') {
        let temp_key: TableKey;
        if i == 0 {
            temp_key = TableKey::Primary;
        } else {
            temp_key = TableKey::None;
        }
        if value.parse::<f32>().is_ok() {
            new_header.push(HeaderItem{name: value_columns[i], kind: DbType::Float, key: temp_key})
        } else if value.parse::<i32>().is_ok() {
            new_header.push(HeaderItem{name: value_columns[i], kind: DbType::Int, key: temp_key})
        } else if value.len() <= 64 {
            new_header.push(HeaderItem{name: value_columns[i], kind: DbType::Text, key: temp_key})
        } else {
            return Err(EzError{tag: ErrorTag::Deserialization, text: format!("Unsupported type: {}", value)})
        }
        i += 1;
    }

    let mut csv = print_sep_list(&new_header, ";");
    csv.push('\n');
    csv.push_str(values);

    let input_table = ColumnTable::from_csv_string(&csv, table_name, "inserts")?;
    Ok(input_table)
}


/// Helper function for the table sorting.
/// This rearranges a column by a list of given indexes.
/// This is how the other columns as sorted to match the primary key column after it is sorted.
#[inline]
fn rearrange_by_index<T: Clone>(col: &mut Vec<T>, indexer: &[usize]) {
    

    let mut temp = Vec::with_capacity(col.len());
    for i in 0..col.len() {
        temp.push(col[indexer[i]].clone());
    }
    *col = temp;
}

/// Helper function to remove indices in batches.
pub fn remove_indices<T>(vec: &mut Vec<T>, indices: &[usize]) {
    

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

/// Helper function to merge two sorted Vecs. Used in the update methods.
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
            }
            std::cmp::Ordering::Equal => {
                output.push(two[two_pointer].clone());
                record_vec.push(3);
                two_pointer += 1;
                one_pointer += 1;
            }
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

/// Helper function for merging two unsorted vecs in the order of another vec. Used to sort.
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
            }
            2 => {
                output.push(two[two_pointer].clone());
                two_pointer += 1;
            }
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

/// This is the struct that carries the binary blob of the key/value pairs along with some metadata
#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct Value {
    pub name: KeyString,
    pub body: Vec<u8>,
}

impl Value {
    pub fn new(name: &str, body: &[u8]) -> Value {
        
        let mut body = Vec::from(body);
        body.shrink_to_fit();
        Value {
            name: KeyString::from(name),
            body: body,
        }
    }

    pub fn update(&mut self, value: Value) {
        

        assert_eq!(self.name, value.name);
        self.body = value.body;

    } 

    pub fn write_to_binary(&self) -> Vec<u8> {
        

        let mut output = Vec::with_capacity(self.body.len() + 80);

        // WRITING METADATA
        output.extend_from_slice(self.name.raw());
        output.extend_from_slice(&self.body);

        output
    }

    pub fn from_binary(name: &str, binary: &[u8]) -> Result<Value, EzError> {

        let binary_name = KeyString::try_from(&binary[0..64])?;
        if binary_name.as_str() != name {
            return Err(EzError {tag: ErrorTag::Deserialization, text: "given name does not match written name of value".to_owned()})
        }

        let body = &binary[64..];

        Ok(
            Value {
                name: KeyString::from(name),
                body: body.to_vec(),
            }
        )
    }
}

#[cfg(test)]
mod tests {
    #![allow(unused)]

    use std::io::Write;

    use aes_gcm::Key;
    use ezcbor::cbor::decode_cbor;
    use rand::Rng;

    use super::*;

    #[test]
    fn test_keystring() {
        let data: [u8;7] = [b't', b'e', b's', b't', 0,0,0];
        let ks = KeyString::try_from(data.as_slice()).unwrap();
        println!("ks: {}", ks);
    }

    #[test]
    fn test_columntable_from_to_string() {
        let input = "1vnr,i-P;2heiti,t-N;3magn,i-N\n113035;undirlegg;200\n113050;annad undirlegg;500";
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
            printer.push_str(&format!(
                "a{i};{random_string};{random_number};{random_float}\n"
            ));
            printer2.push_str(&format!(
                "b{i};{random_string};{random_number};{random_float}\n"
            ));
            printer22.push_str(&format!(
                "b{i};{random_string};{random_number};{random_float}\n"
            ));

            i += 1;
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
        let unsorted1 = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_unsorted.csv"
        ))
        .unwrap();
        let unsorted2 = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets2_unsorted.csv"
        ))
        .unwrap();
        let sorted_combined = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv"
        ))
        .unwrap();

        let mut a = ColumnTable::from_csv_string(&unsorted1, "a", "test").unwrap();
        let b = ColumnTable::from_csv_string(&unsorted2, "b", "test").unwrap();
        let c = ColumnTable::from_csv_string(&sorted_combined, "c", "test").unwrap();
        a.update(&b).unwrap();
        let mut file = std::fs::File::create("combined.csv").unwrap();
        file.write_all(a.to_string().as_bytes());

        let a_string = a.to_string();
        let b_string = b.to_string();

        let mut a_iter = a_string.split(';');
        let mut b_iter = b_string.split(';');

        loop {
            let x = a_iter.next();
            if x.is_none() {break}
            let y = b_iter.next();
            if y.is_none() {break}

            println!("a: {}", x.unwrap());
            println!("b: {}", y.unwrap());
        }

        // assert_eq!(a.to_string(), c.to_string());
    }

    #[test]
    fn test_columntable_query_list() {
        let input = "vnr,i-P;heiti,t-N;magn,i-N\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        // println!("t: {}", t.to_string());
        let x = t.query_list(Vec::from(["113035"])).unwrap();
        assert_eq!(x, "undirlegg;200;113035");
    }

    #[test]
    fn test_columntable_query_single() {
        let input = "vnr,i-P;heiti,t-N;magn,i-N\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
        // println!("t: {}", t.to_string());
        let x = t.query("113035").unwrap();
        assert_eq!(x, "undirlegg;200;113035");
    }

    // #[test]
    // fn test_columntable_query_range() {
    //     let input = "vnr,i-P;heiti,t-N;magn,i-N\n113035;undirlegg;200\n113050;annad undirlegg;500\n18572054;flísalím;42\n113446;harlech;250";
    //     let t = ColumnTable::from_csv_string(input, "test", "test").unwrap();
    //     let x = t.query_range(("113035", "113060")).unwrap();

    //     assert_eq!(x, "undirlegg;200;113035\nannad undirlegg;500;113050")
    // }

    #[test]
    fn test_binary_format() {
        // let input = "vnr,i-P;heiti,t;magn,i\n113035;undirlegg;200\n113050;annad undirlegg;500";
        let input = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv"
        ))
        .unwrap();
        let t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();
        let bin_t = t.to_binary();
        let trans_t = ColumnTable::from_binary(Some("test"), &bin_t).unwrap();
        assert_eq!(t, trans_t);
    }

    // TEST QUERIES ###############################################################################################################################################################################

    #[test]
    fn test_delete_range() {
        let input = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv"
        ))
        .unwrap();
        let test_input = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted_test_range.csv"
        ))
        .unwrap();
        let mut t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();

        let test_t = ColumnTable::from_csv_string(&test_input, "test", "test").unwrap();
        // println!("{}", t);
        t.delete_range(("262", "673"));
        // println!("{}", t);
        assert_eq!(t.to_string(), test_t.to_string());
    }

    #[test]
    fn test_delete_list() {
        let input = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv"
        ))
        .unwrap();
        let test_input = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted_test_range.csv"
        ))
        .unwrap();
        let mut t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();

        let test_t = ColumnTable::from_csv_string(&test_input, "test", "test").unwrap();
        // println!("{}", t);
        t.delete_list(vec!["262", "264", "353", "544", "656"]);
        // println!("{}", t);
        assert_eq!(t.to_string(), test_t.to_string());
    }

    #[test]
    fn test_copy_lines() {
        let input_string = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv"
        ))
        .unwrap();

        let table = ColumnTable::from_csv_string(&input_string, "source", "test").unwrap();

        let input_string = std::fs::read_to_string(format!(
            "test_files{PATH_SEP}test_csv_from_google_sheets_sorted.csv"
        ))
        .unwrap();

        let mut target = ColumnTable::from_csv_string(&input_string, "target", "test").unwrap();

        let line_keys = DbColumn::Ints(vec![
            178,
            262,
            264,
            353,
            544,
            656,
        ]);

        table.copy_lines(&mut target, &line_keys);
        
    }

    #[test]
    fn test_subtable_from_index_range() {
        let table_string = std::fs::read_to_string(&format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
        let table = ColumnTable::from_csv_string(&table_string, "basic_test", "test").unwrap();
        let subtable = table.create_subtable_from_index_range(0, 7515);
        println!("{}", subtable);
    }

    #[test]
    fn test_left_join() {
        let left_string = std::fs::read_to_string(format!("test_files{PATH_SEP}employees.csv")).unwrap();
        let right_string = std::fs::read_to_string(format!("test_files{PATH_SEP}departments.csv")).unwrap();

        let mut left_table = ColumnTable::from_csv_string(&left_string, "employees", "test").unwrap();
        let right_table = ColumnTable::from_csv_string(&right_string, "departments", "test").unwrap();
        println!("{}", left_table);
        println!("{}", right_table);
        left_table.left_join(&right_table, &KeyString::from("department"));
        println!("{}", left_table);

    }

    #[test]
    fn test_cbor_eztable() {
        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}departments.csv")).unwrap();
        let table = ColumnTable::from_csv_string(&csv, "cbor test", "test").unwrap();
        println!("table:\n{}", table);
        let bytes = table.to_cbor_bytes();
        println!("{:x?}", bytes);
        let decoded_table = decode_cbor::<ColumnTable>(&bytes).unwrap();
        assert_eq!(table, decoded_table);
    }

    #[test]
    fn test_keystring_display() {
        let s = KeyString::from("test");
        println!("{}", s);
    }

    #[test]
    fn test_keystring_zeroes() {
        let bin = [0u8;64];
        let s = KeyString::try_from(bin.as_slice()).unwrap();
        println!("s: '{}'", s.as_str());

    }
}

