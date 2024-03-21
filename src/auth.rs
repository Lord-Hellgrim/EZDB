use std::{
    collections::BTreeMap,
    fmt,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use crate::{db_structure::KeyString, networking_utilities::blake3_hash};

/// Defines a permission a user has to interact with a given table
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Permission {
    Upload,
    Download,
    Update,
    Query,
}

impl Permission {
    /// Creates a Permission enum from a string, tolerating some common spellings
    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "Upload" | "Uploading" | "KvUpload" => Some(Permission::Upload),
            "Download" | "Downloading" | "KvDownload" => Some(Permission::Download),
            "Update" | "Updating" | "KvUpdate" => Some(Permission::Update),
            "Query" | "Querying" => Some(Permission::Query),
            _ => None,
        }
    }

    /// Serializes the Permission to a String directly.
    pub fn to_str(&self) -> String {
        match self {
            Permission::Upload => "Upload".to_owned(),
            Permission::Download => "Download".to_owned(),
            Permission::Update => "Update".to_owned(),
            Permission::Query => "Query".to_owned(),
        }
    }
}

/// The struct that represents a user.
/// The password field is a blake3 hash of the users password
/// the can_upload field tracks whether the user should be allowed to upload tables or binary blobs
/// the can_X fields are lists of tables / values on which X operation is allowed.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct User {
    pub username: String,
    pub password: [u8; 32],
    pub admin: bool,
    pub can_upload: bool,
    pub can_download: Vec<String>,
    pub can_update: Vec<String>,
    pub can_query: Vec<String>,
}

impl User {
    /// Create new standard non-admin user with no permissions
    pub fn new(username: &str, password: &str) -> User {
        User {
            username: String::from(username),
            password: blake3_hash(password.as_bytes()),
            admin: false,
            can_upload: false,
            can_download: Vec::new(),
            can_update: Vec::new(),
            can_query: Vec::new(),
        }
    }

    /// Create admin user. Admin user by default have all permissions. May disable this later.
    pub fn admin(username: &str, password: &str) -> User {
        User {
            username: String::from(username),
            password: blake3_hash(password.as_bytes()),
            admin: true,
            can_upload: true,
            can_download: Vec::new(),
            can_update: Vec::new(),
            can_query: Vec::new(),
        }
    }

    // pub fn from_str(s: &str) -> Result<Self, ServerError> {

    //     let s: Vec<&str> = s.split(';').collect();

    //     let username = s[0].to_owned();
    //     let password = decode_hex(s[1]).expect("User config file must have been corrupted"); // safe because we are reading froma file that was written to by encode_hex
    //     let permissions_temp = s[2];
    //     if permissions_temp == "Admin" {
    //         return Ok(User::admin(&username))
    //     }
    //     let permissions_temp: Vec<&str> = permissions_temp.split('-').collect();
    //     let mut user = User::new(&username);
    //     for permission in permissions_temp {
    //         let t: Vec<&str> = permission.split(':').collect();
    //         if t.len() >= 2 {
    //             match t[0] {
    //                 "Upload" => user.can_upload = t[1].parse::<bool>().expect("Config file must be wrongly spelled. Make sure upload is ony eith 'false' or 'true"),
    //                 "Download" => user.can_download = t[1].split(',').map(|n| n.to_owned()).collect() ,
    //                 "Update" => user.can_update = t[1].split(',').map(|n| n.to_owned()).collect(),
    //                 "Query" => user.can_query = t[1].split(',').map(|n| n.to_owned()).collect(),
    //             }
    //         }
    //     }
    //     Ok(user)
    // }

    // pub fn to_str(&self) -> String {
    //     let mut output = String::new();
    //     output.push_str(&self.username);
    //     output.push_str(&encode_hex(&self.password));
    //     output.push_str(&format!("Upload:{}", self.can_upload));
    //     output.push_str("Download");
    //     for permission in self.can_download {
    //         output.push_str(string)
    //     }

    //     guest;0d99d15ec31cb06b828ed4de120e2f82a3b3d1ca716b4fd574159d97f13cf6b3;Upload:false-Download:good_csv,test_csv-Update:good_csv-Query:All
    // }
}

/// Check if the user has permission to access a given table.
/// This probably needs to be rewritten as I reduce reliance on Arc<<Mutex<T>>>
#[inline]
pub fn user_has_permission(
    table_name: &str,
    action: &str,
    username: &str,
    users: Arc<RwLock<BTreeMap<KeyString, RwLock<User>>>>,
) -> bool {
    let permission = match Permission::from_str(action) {
        Some(action) => action,
        None => return false,
    };
    let user = users.read().unwrap();
    let user = match user.get(&KeyString::from(username)) {
        Some(u) => u.read().unwrap(),
        None => return false,
    };

    if user.admin {
        return true;
    }

    match permission {
        Permission::Upload => user.can_upload,
        Permission::Download => user.can_download.contains(&table_name.to_owned()),
        Permission::Update => user.can_update.contains(&table_name.to_owned()),
        Permission::Query => user.can_query.contains(&table_name.to_owned()),
    }
}

/// The error generated by auth functions.
#[derive(Debug, Clone)]
pub enum AuthenticationError {
    WrongUser(String),
    WrongPassword,
    TooLong,
    Permission,
}

/// These are all 2 bytes (2 ascii chars) to facilitate known length error reporting back to the client
/// Will probably change the known length later.
impl fmt::Display for AuthenticationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuthenticationError::WrongUser(_) => write!(f, "IU"),
            AuthenticationError::WrongPassword => write!(f, "IP"),
            AuthenticationError::TooLong => write!(f, "LA"),
            AuthenticationError::Permission => write!(f, "NP"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_user_string_parsing() {
        let temp = String::from(
            r#"(username:"admin",password:(210,137,178,218,155,112,81,243,107,78,57,110,10,243,224,105,231,140,241,25,167,253,203,100,55,182,133,196,135,94,159,158),admin:true,can_upload:true,can_download:[],can_update:[],can_query:[])"#,
        );
        let test_user: User = ron::from_str(&temp).unwrap();
        dbg!(test_user);
        let user_string = ron::to_string(&User::admin("admin", "admin")).unwrap();
        println!("{}", user_string);
        let user: User = ron::from_str(&user_string).unwrap();
        assert_eq!(user, User::admin("admin", "admin"));
    }
}
