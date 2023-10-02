use std::fmt;

use crate::networking_utilities::{decode_hex, ServerError};


#[derive(Debug, Clone)]
pub struct User {
    pub Username: String,
    pub PasswordHash: Vec<u8>,
    pub LastAddress: String,
    pub Authenticated: bool,
    // Permissions
    pub Read: Vec<String>,
    pub Update: Vec<String>,
    pub Create: bool,
}

impl User {
    pub fn from_str(s: &str) -> Result<Self, ServerError> {

    let s: Vec<&str> = s.split(';').collect();
    println!("{:?}", s);

    let Username = s[0].to_owned();
    let PasswordHash = decode_hex(s[1]).expect("File must have been corrupted"); // safe because we are reading froma file that was written to by encode_hex
    let LastAddress = s[2].to_owned();
    let Authenticated = s[3].parse::<bool>().unwrap(); // safe since we write onle "true" or "false" to the file
    let read: Vec<&str> = s[4].split(',').collect();
    
    let mut Read = Vec::with_capacity(read.len());
    for item in read {
        Read.push(item.to_owned());
    }
    
    let update: Vec<&str> = s[5].split(',').collect();
    
    let mut Update = Vec::with_capacity(update.len());
    for item in update {
        Update.push(item.to_owned());
    }
    let Create = s[6].parse::<bool>().unwrap(); // safe since we write onle "true" or "false" to the file

    Ok(
        User {
            Username: Username,
            PasswordHash: PasswordHash,
            LastAddress: LastAddress,
            Authenticated: Authenticated,
            Read: Read,
            Update: Update,
            Create: Create,
        }
    )

    }

}

#[derive(Debug, Clone)]
pub enum AuthenticationError {
    WrongUser(String),
    WrongPassword(String),
}

impl fmt::Display for AuthenticationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuthenticationError::WrongUser(_) => write!(f, "Username is incorrect"),
            AuthenticationError::WrongPassword(_) => write!(f, "Password is incorrect"),
        }
    }
}