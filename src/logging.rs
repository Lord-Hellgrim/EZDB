use std::{collections::BTreeMap, fmt::Display, fs::{File, OpenOptions}, io::{Read, Seek, Write}, os::unix::fs::FileExt, sync::{atomic::{AtomicU64, Ordering}, Mutex}};

use crate::{db_structure::{ColumnTable, KeyString}, ezql::Query, utilities::{ez_hash, get_current_time, get_precise_time, print_sep_list, u64_from_le_slice, Instruction}, server_networking::Database};

use crate::PATH_SEP;


pub struct Entry {
    count: u64,
    user: KeyString,
    client_address: KeyString,
    query: String,
    before_snap: BTreeMap<KeyString, ColumnTable>,
    after_snap: BTreeMap<KeyString, ColumnTable>,
    finished: bool,
}

impl Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut printer = format!(
            "{}{} from {} made {} at {}\n\nBefore change:\n",
            match self.finished {
                true => "",
                false => "UNFINISHED!!!",
            },
            self.user,
            self.client_address,
            self.query,
            self.count,
        );
        for table in self.before_snap.values() {
            printer.push_str(&table.to_string());
            printer.push_str("\n\n");
        }
        printer.push_str("After change:\n");

        for table in self.after_snap.values() {
            printer.push_str(&table.to_string());
            printer.push_str("\n\n");
        }

        write!(f, "{}", printer)

    }
}

impl Entry {
    fn to_binary(&self) -> Vec<u8> {
        let mut binary: Vec<u8> = Vec::new();

        let mut entry_size = 0;
        binary.extend_from_slice(&(0 as usize).to_le_bytes());

        binary.extend_from_slice(&self.count.to_le_bytes());
        entry_size += 8;

        binary.extend_from_slice(&self.user.raw());
        entry_size += 64;

        binary.extend_from_slice(&self.client_address.raw());
        entry_size += 64;

        binary.extend_from_slice(&self.query.len().to_le_bytes());
        entry_size += 8;
        
        binary.extend_from_slice(&self.query.as_bytes());
        entry_size += self.query.len();

        for (name, table) in &self.before_snap {
            binary.push(b'B');
            entry_size += 1;

            let table_binary = &table.write_to_binary();
            binary.extend_from_slice(&(table_binary.len()).to_le_bytes());
            entry_size += 8;

            binary.extend_from_slice(name.raw());
            entry_size += 64;

            binary.extend_from_slice(&table_binary);
            entry_size += table_binary.len();
        }

        for (name, table) in &self.after_snap {
            binary.push(b'A');
            entry_size += 1;

            let table_binary = &table.write_to_binary();
            binary.extend_from_slice(&(table_binary.len()).to_le_bytes());
            entry_size += 8;

            binary.extend_from_slice(name.raw());
            entry_size += 64;

            binary.extend_from_slice(&table_binary);
            entry_size += table_binary.len();
        }

        match self.finished {
            true => binary.extend_from_slice(&(1 as usize).to_le_bytes()),
            false => binary.extend_from_slice(&(0 as usize).to_le_bytes()),
        }
        let binary_len = binary.len() - 8; // Offset the length at the start
        binary[0..8].copy_from_slice(&binary_len.to_le_bytes());

        binary
    }

    fn from_binary(slice: &[u8]) -> Entry {
        let mut i = 0;

        let entry_size = u64_from_le_slice(&slice[i..i+8]);
        i += 8;

        let count = u64_from_le_slice(&slice[i..i+8]);
        i += 8;

        let user = KeyString::try_from(&slice[i..i+64]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
        i += 64;

        let client_address = KeyString::try_from(&slice[i..i+64]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
        i += 64;

        let query_len = u64_from_le_slice(&slice[i..i+8]);
        i += 8;

        let query = std::str::from_utf8(&slice[i..i+query_len as usize]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!())).to_owned();
        i += query.len();

        let mut before_snap = BTreeMap::new();
        let mut after_snap = BTreeMap::new();
        while i < (entry_size - 8) as usize {
            let before_or_after = slice[i];
            i += 1;
            match before_or_after {
                b'B' => {
                    let table_size = u64_from_le_slice(&slice[i..i+8]);
                    i += 8;
                    let name = KeyString::try_from(&slice[i..i+64]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
                    i += 64;
                    let table = ColumnTable::from_binary(name.as_str(), &slice[i..i+table_size as usize]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
                    println!("table\n{}", table);
                    i += table_size as usize;
                    before_snap.insert(name, table);
                },
                b'A' => {
                    let table_size = u64_from_le_slice(&slice[i..i+8]);
                    i += 8;
                    let name = KeyString::try_from(&slice[i..i+64]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
                    i += 64;
                    let table = ColumnTable::from_binary(name.as_str(), &slice[i..i+table_size as usize]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
                    i += table_size as usize;
                    after_snap.insert(name, table);
                },
                _ => panic!("This byte should always be either A or B. Failure occured at {} at {} and {}", file!(), line!(), column!()),
            };
        }
        let temp_finished = &slice[i..];
        let finished: u64;
        if temp_finished.len() != 8 {
            panic!("if reading a log entry from the binary fails, then there is a bug or the data is corruptedFailure occured at {} at {} and {}", file!(), line!(), column!());
        } else {
            finished = u64_from_le_slice(temp_finished);
        }
        let finished = match finished {
            1 => true,
            0 => false,
            _ => panic!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted, Failure occured at {} at {} and {}", file!(), line!(), column!()),
        };

        Entry {
            count,
            user,
            client_address,
            query,
            before_snap,
            after_snap,
            finished,
        }
    }
}

pub struct Logger {
    entries: BTreeMap<u64, Entry>,
    counter: AtomicU64,
}

impl Display for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", print_sep_list(&self.entries.values().collect::<Vec<&Entry>>(), "\n"))
    }
}

impl Logger {
    pub fn init() -> Logger {
        Logger {
            entries: BTreeMap::new(),
            counter: AtomicU64::from(0),
        }
    }

    pub fn read_log_file(timestamp_path: &str) -> Logger {
        let mut log_file = OpenOptions::new().read(true).append(true).open(&format!("EZconfig/log/{}", timestamp_path)).expect("Log file should exist before Logger is initialized");
        let mut log = Vec::new();
        log_file.read_to_end(&mut log).expect("If reading the log file fails then we damn well better panic!");
        let mut entries = BTreeMap::new();
        let mut counter = 0;
        if !log.is_empty() {
    
            let mut i = 0;
            while i < log.len() {
                let entry_size = u64_from_le_slice(&log[i..i+8]) as usize;
                i += 8;
                let entry = Entry::from_binary(&log[i..i+entry_size]);
                if entry.count > counter {
                    counter = entry.count;
                }
                entries.insert(entry.count, entry);
                i += entry_size;
            }
        }


        Logger {
            entries,
            counter: AtomicU64::from(counter),
        }
    }

    pub fn start_log(&mut self, query: &str, user: KeyString, client_address: KeyString) -> u64 {
        let entry = Entry {
            count: self.counter.load(Ordering::SeqCst).wrapping_add(1),
            user,
            client_address,
            query: query.to_owned(),
            before_snap: BTreeMap::new(),
            after_snap: BTreeMap::new(),
            finished: false,
            };

        let count = entry.count;
        self.entries.insert(entry.count, entry);
        count
    }

    pub fn update_before_log(&mut self, hash: u64, table: &ColumnTable) {
        self.entries.entry(hash).and_modify(|e| { 
            if !e.finished {
                e.before_snap.insert(table.name, table.clone()); 
            }
        });
    }

    pub fn update_after_log(&mut self, hash: u64, table: &ColumnTable) {
        self.entries.entry(hash).and_modify(|e| { 
            if !e.finished {
                e.after_snap.insert(table.name, table.clone()); 
            }
        });
    }

    pub fn finish_log(&mut self, hash: u64) {
        self.entries.entry(hash).and_modify(|e| e.finished = true);
    }

    pub fn flush_to_disk(&mut self) {
        let mut binary = Vec::new();
        for entry in self.entries.values() {
            let entry_binary = entry.to_binary();
            let entry_size = u64_from_le_slice(&entry_binary[0..8]);
            binary.extend_from_slice(&entry_size.to_le_bytes());
            binary.extend_from_slice(&entry_binary);
        }
        let mut log_file = File::create(format!("EZconfig/log/{}", get_precise_time())).unwrap();
        log_file.write_all(&binary).unwrap();
        self.entries = BTreeMap::new();
        self.counter.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use crate::{db_structure::table_from_inserts, ezql::{execute_insert_query, parse_EZQL}};

    use super::*;


    #[test]
    fn test_logger_basics() {
        let mut logger = Logger::init();

        let csv = std::fs::read_to_string(format!("test_files{PATH_SEP}good_csv.txt")).unwrap();
        let mut table = ColumnTable::from_csv_string(&csv, "good_csv", "logger_test").unwrap();
        // println!("before:\n{}", table);
        // vnr,i-P;heiti,t-N;magn,i-N
        // 0113000;undirlegg2;100
        // 0113035;undirlegg;200
        // 18572054;flísalím;42

        let query_string = "INSERT(table_name: good_csv, value_columns: (vnr, heiti, magn), new_values: (0113446, harlech, 2500))".to_owned();
        let hash = logger.start_log(&query_string, KeyString::from("test"), table.metadata.created_by);
        logger.update_before_log(hash, &table);
        let query = parse_EZQL(&query_string).unwrap();
        match &query {
            Query::INSERT { table_name, inserts } => logger.update_after_log(hash, &table_from_inserts(inserts, "log_inserts", &table).unwrap()),
            _ => panic!(),
        }
        logger.finish_log(hash);
        execute_insert_query(query, &mut table).unwrap();
        // println!("table:\n{}", table);

        let binary = logger.entries.get(&hash).unwrap().to_binary();
        let entry = Entry::from_binary(&binary);
        println!("binary:\n{:x?}", binary);

        logger.flush_to_disk();
    }
}