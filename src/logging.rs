use crate::db_structure::KeyString;


#[derive(Default, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct Logger {
    entries: Vec<KeyString>,
}

