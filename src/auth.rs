use std::fmt;

use crate::networking_utilities::{decode_hex, ServerError};


#[derive(Debug, Clone)]
pub struct User {
    pub Username: String,
    pub Password: Vec<u8>,
    pub LastAddress: String,
    pub Authenticated: bool,
}

impl User {
    pub fn from_str(s: &str) -> Result<Self, ServerError> {

    let s: Vec<&str> = s.split(';').collect();
    println!("{:?}", s);

    let Username = s[0].to_owned();
    let Password = decode_hex(s[1]).expect("File must have been corrupted"); // safe because we are reading froma file that was written to by encode_hex
    let LastAddress = s[2].to_owned();
    let Authenticated = s[3].parse::<bool>().unwrap(); // safe since we write only "true" or "false" to the file
    Ok(
        User {
            Username: Username,
            Password: Password,
            LastAddress: LastAddress,
            Authenticated: Authenticated,
        }
    )

    }

}

#[derive(Debug, Clone)]
pub enum AuthenticationError {
    WrongUser(String),
    WrongPassword(String),
    TooLong
}

impl fmt::Display for AuthenticationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuthenticationError::WrongUser(_) => write!(f, "Username is incorrect"),
            AuthenticationError::WrongPassword(_) => write!(f, "Password is incorrect"),
            AuthenticationError::TooLong => write!(f, "Neither password or username can be more than 512 bytes"),
        }
    }
}