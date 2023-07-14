use std::{fmt, collections::HashMap};
use crate::basic_io_functions;

pub enum StrictError {
    MoreItemsThanHeader(usize),
    FewerItemsThanHeader(usize),
    RepeatingHeader(usize, usize),
    FloatPrimaryKey,
}

impl fmt::Display for StrictError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StrictError::MoreItemsThanHeader(n) => write!(f, "There are more items in line {} than in the header.\n", n),
            StrictError::FewerItemsThanHeader(n) => write!(f, "There are less items in line {} than in the header.\n", n),
            StrictError::RepeatingHeader(n, m) => write!(f, "Item {} and {} are repeated in the header.\n", n, m),
            StrictError::FloatPrimaryKey => write!(f, "Primary key can't be a floating point number. Must be an integer or string."),
        }
    }
}

pub struct Metadata {
    name: String,
    header: Vec<DbEntry>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum DbEntry {
    Int(i64),
    Float(f64),
    Text(String),
}

pub struct StrictTable {
    metadata: Metadata,
    table: HashMap<String, Vec<DbEntry>>,
}

pub struct CasualTable<T> {
    metadata: Metadata,
    table: Vec<Vec<T>>,
}


pub fn create_StrictTable_from_csv(s: &String) -> Result<StrictTable, StrictError> {    
    let mut header = Vec::new();
    
    {    /* Checking for unique header */
        let mut rownum = 0;
        for item in s.lines().next().unwrap().split(';') {
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
        let mut rownum: usize = 0;
        let mut colnum: usize = 0;
        
        loop {
            loop{
                if rownum == header.len()-1 {
                    break;
                } else if rownum == colnum {
                    rownum += 1;
                    continue;
                } else if header[rownum] == header[colnum]{
                    return Err(StrictError::RepeatingHeader(colnum, rownum))
                } else {
                    rownum += 1;
                }
            }
            if colnum == header.len()-1 {
                break;
            }
            colnum += 1;
        }
    }

    { // Checking that all rows have same number of items as header
        let mut count_rows: usize = 0;
        let mut count_columns: usize = 0;
        let mut row_check: usize = 0;
        for line in s.split('\n') {
            count_rows = line.split(';').count();
            if count_columns == 0 {
                row_check = count_rows;
            } else {
                if row_check < count_rows {
                    return Err(StrictError::MoreItemsThanHeader(count_columns))
                } else if row_check > count_rows {
                    return Err(StrictError::FewerItemsThanHeader(count_columns))
                }
            }
            count_rows = 0;
            count_columns += 1;
        }
    } // Finished checking

    let mut output = HashMap::new();
    let mut rownum: usize = 0;
    let mut colnum: usize = 0;
    for row in s.lines() {
        let mut temp = Vec::new();
        for col in row.split(';') {
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
        match &temp[0] {
            DbEntry::Text(value) => output.insert(value.to_owned(), temp.clone()),
            DbEntry::Int(value) => output.insert(value.to_string(), temp.clone()),
            _ => panic!("This is not supposed to happen"),
        };
        rownum = 0;
        colnum += 1;
    }


    let r = StrictTable {
        metadata: Metadata {name: "test".to_owned(), header: header},
        table: output,
    };

    Ok(r)


}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_StrictError() {
        let s = "here baby;1;2\n3;4;5".to_owned();
        let out: StrictTable;
        match create_StrictTable_from_csv(&s) {
            Ok(o) => out = o,
            Err(e) => {
                println!("{}", e);
                return;
            },
        };
        println!("Table is:\n{:?}\n and metadata is:\nName: {}\nHeader: {:?}", out.table, out.metadata.name, out.metadata.header);
        
    }
}