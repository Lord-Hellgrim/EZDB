use std::{collections::BTreeMap, fmt::Display, fs::File, io::Read, os::unix::fs::FileExt, sync::{atomic::{AtomicU64, Ordering}, Mutex}};

use crate::{db_structure::{EZTable, KeyString}, ezql::Query, networking_utilities::{blake3_hash, get_current_time, print_sep_list, u64_from_le_slice, Instruction}, server_networking::Database};

pub struct Entry {
    count: u64,
    user: KeyString,
    client_address: KeyString,
    query: String,
    before_snap: BTreeMap<KeyString, EZTable>,
    after_snap: BTreeMap<KeyString, EZTable>,
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
        entry_size += self.query.len();

        for (name, table) in &self.before_snap {
            binary.push(b'B');
            binary.extend_from_slice(&(64 + table.metadata.size_of_table()).to_le_bytes());
            binary.extend_from_slice(name.raw());
            binary.extend_from_slice(&table.write_to_binary());
            entry_size += 1 + 64 + table.metadata.size_of_table() + 8;
        }

        for (name, table) in &self.after_snap {
            binary.push(b'A');
            binary.extend_from_slice(&(64 + table.metadata.size_of_table()).to_le_bytes());
            binary.extend_from_slice(name.raw());
            binary.extend_from_slice(&table.write_to_binary());
            entry_size += 1 + 64 + table.metadata.size_of_table() + 8;
        }

        match self.finished {
            true => binary.extend_from_slice(&(1 as usize).to_le_bytes()),
            false => binary.extend_from_slice(&(0 as usize).to_le_bytes()),
        }
        binary[0..8].copy_from_slice(&entry_size.to_le_bytes());

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

        let mut before_snap = BTreeMap::new();
        let mut after_snap = BTreeMap::new();
        while i < entry_size as usize {
            let before_or_after = slice[i];
            i += 1;
            match before_or_after {
                b'B' => {
                    let table_size = u64_from_le_slice(&slice[i..i+8]);
                    i += 8;
                    let name = KeyString::try_from(&slice[i..i+64]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
                    let table = EZTable::from_binary(name.as_str(), &slice[i+64..i+table_size as usize]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
                    i += table_size as usize;
                    before_snap.insert(name, table);
                },
                b'A' => {
                    let table_size = u64_from_le_slice(&slice[i..i+8]);
                    i += 8;
                    let name = KeyString::try_from(&slice[i..i+64]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
                    let table = EZTable::from_binary(name.as_str(), &slice[i+64..i+table_size as usize]).expect(&format!("if reading a log entry from the binary fails, then there is a bug or the data is corrupted: Failure occured at {} at {} and {}", file!(), line!(), column!()));
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
    log_file: File,
}

impl Display for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", print_sep_list(&self.entries.values().collect::<Vec<&Entry>>(), "\n"))
    }
}

impl Logger {
    pub fn init() -> Logger {
        
        let mut log_file = File::open("EZconfig/log").expect("Log file should exist before Logger is initialized");
        let mut log = Vec::new();
        log_file.read_to_end(&mut log).expect("If reading the log file fails hen we damn well better panic!");
        let mut entries = BTreeMap::new();
        let mut counter = 0;

        let mut i = 0;
        while i < log.len() {
            let entry_size = u64_from_le_slice(&log[i..i+8]) as usize;
            let entry = Entry::from_binary(&log[i+8..i+8+entry_size]);
            if entry.count > counter {
                counter = entry.count;
            }
            entries.insert(entry.count, entry);
            i += entry_size;
        }


        Logger {
            entries,
            counter: AtomicU64::from(counter),
            log_file,
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

        entry.count
    }

    pub fn update_before_log(&mut self, hash: u64, table: &EZTable) {
        self.entries.entry(hash).and_modify(|e| { e.before_snap.insert(table.name, table.clone()); });
    }

    pub fn update_after_log(&mut self, hash: u64, table: &EZTable) {
        self.entries.entry(hash).and_modify(|e| { e.after_snap.insert(table.name, table.clone()); });
    }

    pub fn finish_log(&mut self, hash: u64) {
        self.entries.entry(hash).and_modify(|e| e.finished = true);
    }

    pub fn flush_to_disk(&self) {
        let mut last_entry_offset = 0;
        for entry in self.entries.values() {
            let binary = entry.to_binary();
            let entry_size = u64_from_le_slice(&binary[0..8]);
            self.log_file.write_at(&binary, last_entry_offset);
            last_entry_offset = entry_size;
        }
    }
}
