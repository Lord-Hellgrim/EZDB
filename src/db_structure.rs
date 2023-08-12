use std::{fmt, collections::BTreeMap};

#[derive(Debug, PartialEq)]
pub enum StrictError {
    MoreItemsThanHeader(usize),
    FewerItemsThanHeader(usize),
    RepeatingHeader(usize, usize),
    FloatPrimaryKey,
    Empty,
}

impl fmt::Display for StrictError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            StrictError::MoreItemsThanHeader(n) => write!(f, "There are more items in line {} than in the header.\n", n),
            StrictError::FewerItemsThanHeader(n) => write!(f, "There are less items in line {} than in the header.\n", n),
            StrictError::RepeatingHeader(n, m) => write!(f, "Item {} and {} are repeated in the header.\n", n, m),
            StrictError::FloatPrimaryKey => write!(f, "Primary key can't be a floating point number. Must be an integer or string."),
            StrictError::Empty => write!(f, "Don't pass an empty string."),
        }
    }
}

// This struct is here to future proof the StrictTable. More metadata will be added in future.
#[derive(PartialEq, Clone, Debug)]
pub struct Metadata {
    pub name: String,
    pub header: Vec<DbEntry>,
}

#[derive(PartialEq, Clone, Debug)]
pub enum DbEntry {
    Int(i64),
    Float(f64),
    Text(String),
}

#[derive(PartialEq, Clone, Debug)]
pub struct CasualTable<T> {
    metadata: Metadata,
    table: Vec<Vec<T>>,
}

#[derive(PartialEq, Clone, Debug)]
pub struct StrictTable {
    pub metadata: Metadata,
    pub table: BTreeMap<String, Vec<DbEntry>>,
}

impl StrictTable {
    pub fn from_csv_string(s: &String, name: &str) -> Result<StrictTable, StrictError> {
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

        let mut output = BTreeMap::new();
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
            metadata: Metadata {name: name.to_owned(), header: header},
            table: output,
        };

        Ok(r)
    }







    pub fn to_csv_string(&self) -> String {
        let mut printer = String::from("");
        let map = &self.table;
        let header = &self.metadata.header;

        // for item in header {
        //     match item {
        //         DbEntry::Float(value) => printer.push_str(&value.to_string()),
        //         DbEntry::Int(value) => printer.push_str(&value.to_string()),
        //         DbEntry::Text(value) => printer.push_str(&value),
        //     };
        //     printer.push(';');
        // }
        // printer.pop().unwrap();
        // printer.push('\n');

        for (_, line) in map.iter() {
            for item in line {
                match item {
                    DbEntry::Float(value) => printer.push_str(&value.to_string()),
                    DbEntry::Int(value) => printer.push_str(&value.to_string()),
                    DbEntry::Text(value) => printer.push_str(value),
                }
                printer.push(';')
            }
            printer.pop().unwrap();
            printer.push('\n');
        }

        printer = printer.trim().to_owned();
        printer
    }

}


pub fn create_StrictTable_from_csv(s: &String, name: &str) -> Result<StrictTable, StrictError> {    
    
    let r =  match StrictTable::from_csv_string(s, name) {
        Ok(r) => Ok(r),
        Err(e) => Err(e),
    };

    r
    
}

#[cfg(test)]
mod tests {
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
        let t = StrictTable::from_csv_string(&"1;here baby;3;2\n2;3;4;5".to_owned(), "test").unwrap();
        let x = t.to_csv_string();
        println!("{}", x);
        assert_eq!(x, "1;here baby;3;2\n2;3;4;5".to_owned());
    }

}