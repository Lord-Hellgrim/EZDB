//#![allow(unused)]
#![allow(non_snake_case)]

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
pub mod ezql;
pub mod handlers;
pub mod logging;
pub mod networking_utilities;
pub mod server_networking;