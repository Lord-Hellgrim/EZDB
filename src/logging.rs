use std::{collections::BTreeMap, fmt::Display};

use serde::{Deserialize, Serialize};

use crate::{db_structure::{EZTable, KeyString}, ezql::Query, networking_utilities::{print_sep_list, Instruction}};

pub struct Entry {
    timestamp: u64,
    user: KeyString,
    client_address: KeyString,
    query: String,
    before_snap: BTreeMap<KeyString, EZTable>,
    after_snap: BTreeMap<KeyString, EZTable>,
}

impl Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut printer = format!(
            "{} from {} made {} at {}\n\nBefore change:\n",
            self.user,
            self.client_address,
            self.query,
            self.timestamp,
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

pub struct Logger {
    entries: Vec<Entry>,
}

impl Display for Logger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", print_sep_list(&self.entries, "\n"))
    }
}

impl Logger {
    pub fn new() -> Logger {
        Logger {
            entries: Vec::new(),
        }
    }
}