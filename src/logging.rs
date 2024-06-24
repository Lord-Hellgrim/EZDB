use std::fmt::Display;

use crate::{db_structure::KeyString, ezql::Query, networking_utilities::{print_sep_list, Instruction}};

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Entry {
    action: Instruction,
    timestamp: u64,
    user: KeyString,
    client_address: KeyString,
}

impl Display for Entry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f,"{} from {} did {} at {}", self.user, self.client_address, self.action, self.timestamp)
    }
}

#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
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

