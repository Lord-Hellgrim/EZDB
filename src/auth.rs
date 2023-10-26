use std::{fmt, collections::HashMap};

use crate::networking_utilities::{decode_hex, ServerError, encode_hex};


#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Upload,
    Download,
    Update,
    Query,
    All,
}

impl Permission {
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Upload" => Some(Permission::Upload),
            "Download" => Some(Permission::Download),
            "Update" => Some(Permission::Update),
            "Query" => Some(Permission::Query),
            "All" => Some(Permission::All),
            _ => None
        }
    }

    pub fn to_str(&self) -> String {
        match self {
            Permission::Upload => "Upload".to_owned(),
            Permission::Download => "Download".to_owned(),
            Permission::Update => "Update".to_owned(),
            Permission::Query => "Query".to_owned(),
            Permission::All => "All".to_owned(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct User {
    pub Username: String,
    pub Password: Vec<u8>,
    pub Permissions: HashMap<String, Vec<Permission>>,
}

impl User {
    pub fn from_str(s: &str) -> Result<Self, ServerError> {

        let s: Vec<&str> = s.split(';').collect();

        let Username = s[0].to_owned();
        let Password = decode_hex(s[1]).expect("File must have been corrupted"); // safe because we are reading froma file that was written to by encode_hex
        let permissions_temp = s[2];
        let mut permissions = HashMap::new();
        let permissions_temp: Vec<&str> = permissions_temp.split('-').collect();
        for permission in permissions_temp {
            let temp: Vec<&str> = permission.split(':').collect();
            let perms: Vec<&str> = temp[1].split(',').collect();
            let perms: Vec<Permission> = perms.iter().map(|n| Permission::from_str(n).unwrap()).collect();
            permissions.insert(temp[0].to_owned(), perms);
        }
        Ok(
            User {
                Username: Username,
                Password: Password,
                Permissions: permissions,
            }
        )
    }

    pub fn to_str(&self) -> String {
        let mut output = String::new();

        output.push_str(&self.Username);
        output.push_str(";");
        output.push_str(&encode_hex(&self.Password));
        output.push_str(";");
        for (table, permissions) in &self.Permissions {
            output.push_str(table);
            output.push_str(":");
            for permission in permissions {
                output.push_str(&permission.to_str());
                output.push_str(",");
            }
            output.pop();
            output.push_str("-");
        }
        output.pop();

        output
    }

}

#[derive(Debug, Clone)]
pub enum AuthenticationError {
    WrongUser(String),
    WrongPassword(Vec<u8>),
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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_string_parsing() {
        let user_string = String::from("admin;6ef5f331ccc2384c9e744dead5cb61b7e1624b9bf2eaf9b2a1aa8baf4cc0692e;All:All");
        let user = User::from_str(&user_string).unwrap();
        let mut expected_permissions = HashMap::new();
        expected_permissions.insert("All".to_owned(), vec![Permission::All]);
        let expected_user = User {
            Username: "admin".to_owned(),
            Password: decode_hex("6ef5f331ccc2384c9e744dead5cb61b7e1624b9bf2eaf9b2a1aa8baf4cc0692e").unwrap(),
            Permissions: expected_permissions,
        };
        assert!(user == expected_user);
        let stringed_user = user.to_str();
        assert!(user_string == stringed_user);

    }

}