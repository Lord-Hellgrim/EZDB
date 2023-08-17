#![allow(unused)]
#![allow(non_snake_case)]

use std::{collections::HashMap, sync::{Arc, Mutex}};

mod basic_io_functions;
mod db_structure;
mod server_networking;
mod client_networking;
mod networking_utilities;
mod logger;

fn main() {
    let mut global: HashMap<String, db_structure::StrictTable> = HashMap::new();
    let arc_global = Arc::new(Mutex::new(global));
    server_networking::server("127.0.0.1:3004", arc_global.clone());
}
