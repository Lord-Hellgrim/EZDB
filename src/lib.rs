//#![allow(unused)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![feature(portable_simd)]

#[cfg(target_os="windows")]
pub const PATH_SEP: char = '\\';

#[cfg(target_os="linux")]
pub const PATH_SEP: char = '/';


// pub mod aes;
pub mod aes_temp_crypto;
pub mod auth;
pub mod client_networking;
pub mod compression;
pub mod db_structure;
pub mod disk_utilities;
pub mod ezql;
pub mod handlers;
pub mod logging;
pub mod utilities;
pub mod server_networking;
// pub mod bloom_filter;
pub mod row_arena;
pub mod http_interface;
pub mod thread_pool;
pub mod testing_tools;
pub mod row_table;
// pub mod query_execution;