use std::fmt;


#[derive(Debug, Clone)]
pub struct User {
    pub Username: String,
    pub PasswordHash: String,
    pub LastAddress: String,
    pub Authenticated: bool,
    // Permissions
    pub Read: Vec<String>,
    pub Update: Vec<String>,
    pub Create: bool,
}

impl User {
    pub fn from_str(s: &str) -> Self {

    let s: Vec<&str> = s.split(';').collect();
    println!("{:?}", s);

    let Username = s[0].to_owned();
    let PasswordHash = s[1].to_owned();
    let LastAddress = s[2].to_owned();
    let Authenticated = s[3].parse::<bool>().unwrap();
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
    let Create = s[6].parse::<bool>().unwrap();

    User {
        Username: Username,
        PasswordHash: PasswordHash,
        LastAddress: LastAddress,
        Authenticated: Authenticated,
        Read: Read,
        Update: Update,
        Create: Create,
    }

    }

}

#[derive(Debug, Clone)]
pub enum AuthenticationError {
    WrongUser(String),
    WrongPassword(String),
    MissingField,
}

impl fmt::Display for AuthenticationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AuthenticationError::WrongUser(s) => write!(f, "Username: {}\nIs incorrect", s),
            AuthenticationError::WrongPassword(s) => write!(f, "PasswordHash: {}\nIs incorrect", s),
            AuthenticationError::MissingField => write!(f, "Missing username or password or both"),
        }
    }
}