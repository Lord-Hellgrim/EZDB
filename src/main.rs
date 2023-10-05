#![allow(unused)]
#![allow(non_snake_case)]
#![feature(core_intrinsics, stdsimd)]


use std::{collections::HashMap, sync::{Arc, Mutex}};


use EZDB::db_structure;
use EZDB::server_networking;
use EZDB::client_networking;
use EZDB::networking_utilities;
use EZDB::logger;
use EZDB::auth;
use EZDB::aes;
use EZDB::aes_temp_crypto;
use EZDB::diffie_hellman;

fn main() -> Result<(), networking_utilities::ServerError> {

    #[cfg(target_feature="avx2")]
    unsafe fn p() {
        println!("AVX2");
    }

    #[cfg(not(target_feature="avx2"))]
    fn p() {
        println!("not avx2");
    }

    unsafe { p() };

    let global: HashMap<String, db_structure::StrictTable> = HashMap::new();
    let arc_global = Arc::new(Mutex::new(global));
    server_networking::server("127.0.0.1:3004", arc_global.clone())?;

    Ok(())
}
