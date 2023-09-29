//#![allow(unused)]
#![allow(non_snake_case)]
#![feature(core_intrinsics, stdsimd)]


use std::{collections::HashMap, sync::{Arc, Mutex}};


mod db_structure;
mod server_networking;
mod client_networking;
mod networking_utilities;
mod logger;
mod auth;
mod aes;
mod aes_temp_crypto;
mod diffie_hellman;

fn main() -> Result<(), networking_utilities::ServerError> {

    let global: HashMap<String, db_structure::StrictTable> = HashMap::new();
    let arc_global = Arc::new(Mutex::new(global));
    server_networking::server("127.0.0.1:3004", arc_global.clone())?;

    Ok(())
}
