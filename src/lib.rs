//#![allow(unused)]
#![allow(non_snake_case)]

#[cfg(target_os="windows")]
pub const PATH_SEP: char = '\\';

#[cfg(target_os="linux")]
pub const PATH_SEP: char = '/';


pub mod db_structure;
pub mod server_networking;
pub mod client_networking;
pub mod networking_utilities;
pub mod auth;
// pub mod aes;
pub mod aes_temp_crypto;
pub mod handlers;