use std::arch::asm;
use std::fmt::Display;
use std::simd;
use std::io::{ErrorKind, Read};
use std::net::TcpStream;
use std::num::{ParseFloatError, ParseIntError};
use std::simd::num::SimdInt;
use std::str::{self, Utf8Error};
use std::string::FromUtf8Error;
use std::sync::Arc;
use std::{usize, fmt};

use std::arch::x86_64;

use ezcbor::cbor::{byteslice_from_cbor, byteslice_to_cbor, Cbor, CborError};
use eznoise::CipherState;
use fnv::FnvHashMap;
use aes_gcm::aead;
use sha2::{Sha256, Digest};

use crate::auth::AuthenticationError;
use crate::db_structure::Value;
use crate::server_networking::Database;


pub const INSTRUCTION_BUFFER: usize = 1024;
pub const DATA_BUFFER: usize = 1_048;//_576; // 1 mb
pub const MAX_DATA_LEN: usize = u32::MAX as usize;

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub enum ErrorTag {
    Utf8,
    Io,
    Instruction,
    Confirmation,
    Authentication,
    Crypto,
    ParseInt,
    ParseFloat,
    ParseResponse,
    ParseUser,
    OversizedData,
    Decompression,
    Query,
    Debug,
    NoMoreBufferSpace,
    Unimplemented,
    Serialization,
    Deserialization,
    Structure,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct EzError {
    pub tag: ErrorTag,
    pub text: String,
}

impl EzError {
    pub fn to_binary(&self) -> Vec<u8> {
        let mut binary = Vec::new();
        match self.tag {
            ErrorTag::Utf8 => binary.extend_from_slice(ksf("Utf8").raw()),
            ErrorTag::Io => binary.extend_from_slice(ksf("Io").raw()),
            ErrorTag::Instruction => binary.extend_from_slice(ksf("Instruction").raw()),
            ErrorTag::Confirmation => binary.extend_from_slice(ksf("Confirmation").raw()),
            ErrorTag::Authentication => binary.extend_from_slice(ksf("Authentication").raw()),
            ErrorTag::Crypto => binary.extend_from_slice(ksf("Crypto").raw()),
            ErrorTag::ParseInt => binary.extend_from_slice(ksf("ParseInt").raw()),
            ErrorTag::ParseFloat => binary.extend_from_slice(ksf("ParseFloat").raw()),
            ErrorTag::ParseResponse => binary.extend_from_slice(ksf("ParseResponse").raw()),
            ErrorTag::ParseUser => binary.extend_from_slice(ksf("ParseUser").raw()),
            ErrorTag::OversizedData => binary.extend_from_slice(ksf("OversizedData").raw()),
            ErrorTag::Decompression => binary.extend_from_slice(ksf("Decompression").raw()),
            ErrorTag::Query => binary.extend_from_slice(ksf("Query").raw()),
            ErrorTag::Debug => binary.extend_from_slice(ksf("Debug").raw()),
            ErrorTag::NoMoreBufferSpace => binary.extend_from_slice(ksf("NoMoreBufferSpace").raw()),
            ErrorTag::Unimplemented => binary.extend_from_slice(ksf("Unimplemented").raw()),
            ErrorTag::Serialization => binary.extend_from_slice(ksf("Serialization").raw()),
            ErrorTag::Deserialization => binary.extend_from_slice(ksf("Deserialization").raw()),
            ErrorTag::Structure => binary.extend_from_slice(ksf("Structure").raw()),
        };

        binary.extend_from_slice(&self.text.len().to_le_bytes());
        binary.extend_from_slice(self.text.as_bytes());

        binary
    }

    pub fn from_binary(binary: &[u8]) -> Result<EzError, EzError> {
        let tag = KeyString::try_from(&binary[0..64])?;
        let tag = match tag.as_str() {
            "Utf8" => ErrorTag::Utf8,
            "Io" => ErrorTag::Io,
            "Instruction" => ErrorTag::Instruction,
            "Confirmation" => ErrorTag::Confirmation,
            "Authentication" => ErrorTag::Authentication,
            "Crypto" => ErrorTag::Crypto,
            "ParseInt" => ErrorTag::ParseInt,
            "ParseFloat" => ErrorTag::ParseFloat,
            "ParseResponse" => ErrorTag::ParseResponse,
            "ParseUser" => ErrorTag::ParseUser,
            "OversizedData" => ErrorTag::OversizedData,
            "Decompression" => ErrorTag::Decompression,
            "Query" => ErrorTag::Query,
            "Debug" => ErrorTag::Debug,
            "NoMoreBufferSpace" => ErrorTag::NoMoreBufferSpace,
            "Unimplemented" => ErrorTag::Unimplemented,
            "Serialization" => ErrorTag::Serialization,
            "Deserialization" => ErrorTag::Deserialization,
            "Structure" => ErrorTag::Structure,
            other => return Err(EzError{tag: ErrorTag::Unimplemented, text: format!("No error type called '{}'", other)})
        };
        let len = u64_from_le_slice(&binary[64..72]) as usize;
        let text = String::from_utf8(binary[72..72+len].to_vec())?;

        Ok(EzError{tag, text})
    }
}

impl Display for EzError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut disp = "Tag: ".to_owned();
        match self.tag {
            ErrorTag::Utf8 => disp.push_str("Utf8"),
            ErrorTag::Io => disp.push_str("Io"),
            ErrorTag::Instruction => disp.push_str("Instruction"),
            ErrorTag::Confirmation => disp.push_str("Confirmation"),
            ErrorTag::Authentication => disp.push_str("Authentication"),
            ErrorTag::Crypto => disp.push_str("Crypto"),
            ErrorTag::ParseInt => disp.push_str("ParseInt"),
            ErrorTag::ParseFloat => disp.push_str("ParseFloat"),
            ErrorTag::ParseResponse => disp.push_str("ParseResponse"),
            ErrorTag::ParseUser => disp.push_str("ParseUser"),
            ErrorTag::OversizedData => disp.push_str("OversizedData"),
            ErrorTag::Decompression => disp.push_str("Decompression"),
            ErrorTag::Query => disp.push_str("Query"),
            ErrorTag::Debug => disp.push_str("Debug"),
            ErrorTag::NoMoreBufferSpace => disp.push_str("NoMoreBufferSpace"),
            ErrorTag::Unimplemented => disp.push_str("Unimplemented"),
            ErrorTag::Serialization => disp.push_str("Serialization"),
            ErrorTag::Deserialization => disp.push_str("Deserialization"),
            ErrorTag::Structure => disp.push_str("Structure"),
        };
        disp.push_str("\nError text:\n");
        disp.push_str(&self.text);
        disp.push('\n');
        write!(f, "{}", disp)
    }
}

impl From<std::io::Error> for EzError {
    fn from(e: std::io::Error) -> Self {
        let tag = ErrorTag::Io;
        let text = e.to_string();
        EzError { tag, text }
    }
}

impl From<Utf8Error> for EzError {
    fn from(e: Utf8Error) -> Self {
        let tag = ErrorTag::Utf8;
        let text = e.to_string();
        EzError { tag, text }
    }
}

impl From<InstructionError> for EzError {
    fn from(e: InstructionError) -> Self {
        let tag = ErrorTag::Instruction;
        let text = e.to_string();
        EzError { tag, text }
    }
}

impl From<AuthenticationError> for EzError {
    fn from(e: AuthenticationError) -> Self {
        let tag = ErrorTag::Authentication;
        let text = e.to_string();
        EzError { tag, text }
    }
}

impl From<aead::Error> for EzError {
    fn from(e: aead::Error) -> Self {
        let tag = ErrorTag::Crypto;
        let text = e.to_string();
        EzError { tag, text }
    }
}

impl From<ParseIntError> for EzError {
    fn from(e: ParseIntError) -> Self {
        let tag = ErrorTag::ParseInt;
        let text = e.to_string();
        EzError { tag, text }
    }
}

impl From<ParseFloatError> for EzError {
    fn from(e: ParseFloatError) -> Self {
        let tag = ErrorTag::ParseFloat;
        let text = e.to_string();
        EzError { tag, text }
    }
}

impl From<CborError> for EzError {
    fn from(e: CborError) -> Self {
        let tag = ErrorTag::Deserialization;
        let text = match e {
            CborError::IllFormed(x) => x,
            CborError::Unexpected(x) => x,
        };
        EzError{tag, text}
    }
}

impl From<FromUtf8Error> for EzError {
    fn from(e: FromUtf8Error) -> Self {
        let tag = ErrorTag::Utf8;
        let text = e.to_string();
        EzError { tag, text }
    }
}

impl From<eznoise::NoiseError> for EzError {
    fn from(_e: eznoise::NoiseError) -> Self {
        let tag = ErrorTag::Io;
        let text = "No information about NoiseErrors. Sorry, hackers.".to_owned();
        EzError { tag, text }
    }
}


#[repr(align(8))]
#[derive(Clone, Copy, Hash, PartialEq)]
pub struct KeyString {
    inner: [u8;64],
}


impl fmt::Debug for KeyString {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        
        f.debug_struct("KeyString").field("inner", &self.as_str()).finish()
    }
}

impl fmt::Display for KeyString {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let text = std::str::from_utf8(&self.inner).expect(&format!("A KeyString should always be valid utf8.\nThe KeyString that was just attempted to Display was:\n{:x?}", self.inner));
        write!(f, "{}", text)
    }   
}

impl Default for KeyString {
    fn default() -> Self {
        Self { inner: [0;64] }
    }
}

/// Turns a &str into a KeyString. If the &str has more than 64 bytes, the last bytes will be cut.
impl From<&str> for KeyString {
    fn from(s: &str) -> Self {

        let mut inner = [0u8;64];

        let mut min = std::cmp::min(s.len(), 64);
        inner[0..min].copy_from_slice(&s.as_bytes()[0..min]);

        loop {
            if min == 0 {break}
            match std::str::from_utf8(&inner[0..min]) {
                Ok(_) => break,
                Err(_) => min -= 1,
            }
        }

        KeyString {
            inner
        }
    }
}

impl TryFrom<&[u8]> for KeyString {
    type Error = EzError;

    fn try_from(s: &[u8]) -> Result<Self, Self::Error> {
        let mut inner = [0u8;64];

        let min = std::cmp::min(s.len(), 64);
        inner[0..min].copy_from_slice(&s[0..min]);

        match std::str::from_utf8(&inner) {
            Ok(_) => {
                Ok(KeyString {inner})
            },
            Err(e) => Err(EzError{tag: ErrorTag::Utf8, text: e.to_string()})
        }
    }
}

impl Eq for KeyString {}

impl Ord for KeyString {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_str().cmp(other.as_str())
    }
}

impl PartialOrd for KeyString {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.as_str().cmp(other.as_str()))
    }
}

impl Cbor for KeyString {
    fn to_cbor_bytes(&self) -> Vec<u8> {
        byteslice_to_cbor(self.as_bytes())
    }

    fn from_cbor_bytes(bytes: &[u8]) -> Result<(Self, usize), ezcbor::cbor::CborError>
        where 
            Self: Sized 
    {
        let (bytes, bytes_read) = byteslice_from_cbor(bytes)?;
        let text = match String::from_utf8(bytes) {
            Ok(t) => t,
            Err(_) => return Err(CborError::Unexpected(format!("Error originated in KeyString implementation")))
        };
        Ok((KeyString::from(text.as_str()), bytes_read))
    }
}

impl KeyString {

    pub fn new() -> Self {
        KeyString {
            inner: [0u8; 64]
        }
    }

    pub fn len(&self) -> usize {
        let mut output = 0;
        for byte in self.inner {
            match byte {
                0 => break,
                _ => output += 1,
            }
        }
        output
    }

    pub fn push(&mut self, s: &str) -> usize {

        let start = self.as_str().len();
        let len = std::cmp::min(s.len(), 64-start);

        self.inner[start..start+len].copy_from_slice(&s.as_bytes()[0..len]);

        len
    }

    pub fn as_str(&self) -> &str {
        // This is safe since an enforced invariant of KeyString is that it is utf8
        unsafe { std::str::from_utf8_unchecked(&self.inner[0..self.len()]) }
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.inner[0..self.len()]
    }

    pub fn raw(&self) -> &[u8] {
        &self.inner
    }

    /// These functions may panic and should only be called if you are certain that the KeyString contains a valid number
    pub fn to_i32(&self) -> i32 {
        self.as_str().parse::<i32>().unwrap()
    }

    /// These functions may panic and should only be called if you are certain that the KeyString contains a valid number
    pub fn to_f32(&self) -> f32 {
        self.as_str().parse::<f32>().unwrap()
    }

    pub fn to_i32_checked(&self) -> Result<i32, ParseIntError> {
        self.as_str().parse::<i32>()
    }

    pub fn to_f32_checked(&self) -> Result<f32, ParseFloatError> {
        self.as_str().parse::<f32>()
    }

}



// /// The main error of all EZDB. Any error that can occur during a client request should be covered here. Internal errors are covered elsewhere.
// #[derive(Debug)]
// pub enum EzError {
//     Utf8(Utf8Error),
//     Io(ErrorKind),
//     Instruction(String),
//     Confirmation(String),
//     Authentication(String),
//     Crypto(String),
//     ParseInt(ParseIntError),
//     ParseFloat(ParseFloatError),
//     ParseResponse(String),
//     ParseUser(String),
//     OversizedData(String),
//     Decompression(String),
//     Query(String),
//     Debug(String),
//     NoMoreBufferSpace(String),
//     Unimplemented(String),
//     Serialization(String),
//     Deserialization(String),
//     Structure(String),
// }

// impl fmt::Display for EzError {
//     fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
//         match self {
//             EzError::Utf8(e) => write!(f, "Encontered invalid utf-8: {}\n", e),
//             EzError::Io(e) => write!(f, "Encountered an IO error: {}", e),
//             EzError::Instruction(e) => write!(f, "{}", e),
//             EzError::Confirmation(e) => write!(f, "Received corrupt confirmation {:?}", e),
//             EzError::Authentication(e) => write!(f, "{}", e),
//             EzError::Crypto(e) => write!(f, "There has been a crypto error. Most likely the nonce was incorrect. The error is: {}", e),
//             EzError::ParseInt(e) => write!(f, "There has been a problem parsing an integer. The error signature is: {}", e),
//             EzError::ParseFloat(e) => write!(f, "There has been a problem parsing a float. The error signature is: {}", e),
//             EzError::ParseUser(e) => write!(f, "Failed to parse user from string because: {}", e),
//             EzError::OversizedData(s) => write!(f, "Sent data is too long. Maximum data size is {MAX_DATA_LEN}.\nAdditional information: {}", s),
//             EzError::ParseResponse(e) => write!(f, "{}", e),
//             EzError::Decompression(e) => write!(f, "Decompression error occurred from miniz_oxide library.\nLibrary error: {}", e),
//             EzError::Query(s) => write!(f, "Query could not be processed because of:\n############################{}\n################################", s),
//             EzError::NoMoreBufferSpace(x) => write!(f, "No more space in buffer pool. Need to free {x} bytes"),
//             EzError::Unimplemented(s) => write!(f, "{}", s),
//             EzError::Debug(s) => write!(f, "{}", s),
//             EzError::Serialization(s) => write!(f, "{}", s),
//             EzError::Deserialization(s) => write!(f, "{}", s),
//             EzError::Structure(s) => write!(f, "{}", s),

//         }
//     }
// }

// impl From<std::io::Error> for EzError {
//     fn from(e: std::io::Error) -> Self {
//         EzError::Io(e.kind())
//     }
// }

// impl From<Utf8Error> for EzError {
//     fn from(e: Utf8Error) -> Self {
//         EzError::Utf8(e)
//     }
// }

// impl From<InstructionError> for EzError {
//     fn from(e: InstructionError) -> Self {
//         EzError::Instruction(e.to_string())
//     }
// }

// impl From<AuthenticationError> for EzError {
//     fn from(e: AuthenticationError) -> Self {
//         EzError::Authentication(e.to_string())
//     }
// }

// impl From<aead::Error> for EzError {
//     fn from(e: aead::Error) -> Self {
//         EzError::Crypto(format!("{e}"))
//     }
// }

// impl From<ParseIntError> for EzError {
//     fn from(e: ParseIntError) -> Self {
//         EzError::ParseInt(e)
//     }
// }

// impl From<ParseFloatError> for EzError {
//     fn from(e: ParseFloatError) -> Self {
//         EzError::ParseFloat(e)
//     }
// }

// impl From<CborError> for EzError {
//     fn from(e: CborError) -> Self {
//         let s = match e {
//             CborError::IllFormed(x) => x,
//             CborError::Unexpected(x) => x,
//         };
//         EzError::Serialization(s)
//     }
// }

// impl From<FromUtf8Error> for EzError {
//     fn from(e: FromUtf8Error) -> Self {
//         EzError::Utf8(e.utf8_error())
//     }
// }

// impl From<eznoise::NoiseError> for EzError {
//     fn from(e: eznoise::NoiseError) -> Self {
//         match e {
//             eznoise::NoiseError::Ring => EzError::Crypto("".to_owned()),
//             eznoise::NoiseError::WrongState => EzError::Crypto("Noise error. Noise is in wrong state".to_owned()),
//             eznoise::NoiseError::Io => EzError::Io(ErrorKind::BrokenPipe),
//         }
//     }
// }

// impl EzError {
//     pub fn to_binary(&self) -> Vec<u8> {
//         let mut binary = Vec::new();
//         match self {
//             EzError::Utf8(s) => {
//                 binary.extend_from_slice(ksf("Utf8").raw());
//                 binary.extend_from_slice(s.to_string().as_bytes());
//             }
//             EzError::Io(s) => {
//                 binary.extend_from_slice(ksf("Io").raw());
//                 binary.extend_from_slice(s.to_string().as_bytes());
//             }
//             EzError::Instruction(s) => {
//                 binary.extend_from_slice(ksf("Instruction").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Confirmation(s) => {
//                 binary.extend_from_slice(ksf("Confirmation").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Authentication(s) => {
//                 binary.extend_from_slice(ksf("Authentication").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Crypto(s) => {
//                 binary.extend_from_slice(ksf("Crypto").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::ParseInt(s) => {
//                 binary.extend_from_slice(ksf("ParseInt").raw());
//                 binary.extend_from_slice(s.to_string().as_bytes());
//             }
//             EzError::ParseFloat(s) => {
//                 binary.extend_from_slice(ksf("ParseFloat").raw());
//                 binary.extend_from_slice(s.to_string().as_bytes());
//             }
//             EzError::ParseResponse(s) => {
//                 binary.extend_from_slice(ksf("ParseResponse").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::ParseUser(s) => {
//                 binary.extend_from_slice(ksf("ParseUser").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::OversizedData(s) => {
//                 binary.extend_from_slice(ksf("OversizedData").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Decompression(s) => {
//                 binary.extend_from_slice(ksf("Decompression").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Query(s) => {
//                 binary.extend_from_slice(ksf("Query").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Debug(s) => {
//                 binary.extend_from_slice(ksf("Debug").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::NoMoreBufferSpace(s) => {
//                 binary.extend_from_slice(ksf("NoMoreBufferSpace").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Unimplemented(s) => {
//                 binary.extend_from_slice(ksf("Unimplemented").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Serialization(s) => {
//                 binary.extend_from_slice(ksf("Serialization").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Deserialization(s) => {
//                 binary.extend_from_slice(ksf("Deserialization").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//             EzError::Structure(s) => {
//                 binary.extend_from_slice(ksf("Structure").raw());
//                 binary.extend_from_slice(s.as_bytes());
//             }
//         }

//         binary
//     }
// }


/// An enum that lists the possible instructions that the database can receive.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Instruction {
    Query,
    NewUser,
    MetaListTables,
    MetaListKeyValues,
}

impl Display for Instruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        
        

        match self {
            Instruction::Query => write!(f, "Query()"),
            Instruction::NewUser => write!(f, "NewUser()"),
            Instruction::MetaListTables => write!(f, "MetaListTables"),
            Instruction::MetaListKeyValues => write!(f, "MetaListKeyValues"),
        }
    }
}

/// An error that happens during instruction parsing.
#[derive(Debug, PartialEq, Clone)]
pub enum InstructionError {
    Invalid(String),
    // TooLong may be unnecessary because of the instruction buffer
    TooLong,
    Utf8(Utf8Error),
    InvalidTable(String),
}

impl fmt::Display for InstructionError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        
        

        match self {
            InstructionError::Invalid(instruction) => write!(f, "The instruction:\n\n\t{instruction}\n\nis invalid. See documentation for valid buffer\n\n"),
            InstructionError::TooLong => write!(f, "Your instruction is too long. Maximum instruction length is: {INSTRUCTION_BUFFER} bytes\n\n"),
            InstructionError::Utf8(e) => write!(f, "Invalid utf-8: {e}"),
            InstructionError::InvalidTable(_) => write!(f, "NT"),
        }
    }
}

impl From<Utf8Error> for InstructionError {
    fn from(e: Utf8Error) -> Self {
        InstructionError::Utf8(e)
    }
}

pub struct SocketSide {
    pub stream: TcpStream,
    pub work_status: Option<CsPair>,
}

pub struct CsPair {
    pub c1: CipherState,
    pub c2: CipherState,
}

/// THe server side of the Connection exchange
pub fn perform_handshake_and_authenticate(s: eznoise::KeyPair, stream: TcpStream, db_ref: Arc<Database>) -> Result<eznoise::Connection, EzError> {
    
    let mut connection = eznoise::ESTABLISH_CONNECTION(stream, s.clone())?;
    let auth_buffer = connection.RECEIVE_C1()?;

    println!("About to parse auth_string");
    let username = match bytes_to_str(&auth_buffer[0..512]) {
        Ok(s) => s,
        Err(e) => {
            println!("failed to read auth_string from bytes because: {}", e);
            return Err(EzError{tag: ErrorTag::Utf8, text: e.to_string()});
        }
    };
    let password = &auth_buffer[512..];
    let password = ez_hash(bytes_to_str(password).unwrap().as_bytes());
    println!("About to verify username and password");

    let users_lock = db_ref.users.read().unwrap();
    if !users_lock.contains_key(&KeyString::from(username)) {
        println!("printing keys..");

        for key in users_lock.keys() {
            println!("key: '{}'", key);
        }
        println!("Username:\n\t'{}'\n...is wrong", username);
        return Err(EzError{tag: ErrorTag::Authentication, text: format!("Username: '{}' does not exist", username)});
    } else if db_ref.users.read().unwrap()[&KeyString::from(username)].read().unwrap().password != password {
        // println!("thread_users_lock[username].password: {:?}", user_lock.password);
        // println!("password: {:?}", password);
        // println!("Password hash:\n\t{:?}\n...is wrong", password);
        return Err(EzError{tag: ErrorTag::Authentication, text: "Wrong password.".to_owned()});
    }
    Ok(
        connection
    )

}

pub fn authenticate_client(connection: &mut eznoise::Connection, db_ref: Arc<Database>) -> Result<(), EzError> {
    let auth_buffer = connection.RECEIVE_C1()?;

    println!("About to parse auth_string");
    let username = match bytes_to_str(&auth_buffer[0..512]) {
        Ok(s) => s,
        Err(e) => {
            println!("failed to read auth_string from bytes because: {}", e);
            return Err(EzError{tag: ErrorTag::Utf8, text: e.to_string()});
        }
    };
    connection.peer = username.to_string();
    let password = &auth_buffer[512..];
    let password = ez_hash(bytes_to_str(password).unwrap().as_bytes());
    println!("About to verify username and password");

    let users_lock = db_ref.users.read().unwrap();
    println!("taken MUTEX on users");
    if !users_lock.contains_key(&KeyString::from(username)) {
        println!("printing keys..");

        for key in users_lock.keys() {
            println!("key: '{}'", key);
        }
        println!("Username:\n\t'{}'\n...is wrong", username);
        return Err(EzError{tag: ErrorTag::Authentication, text: format!("Username: '{}' does not exist", username)});
    } else if db_ref.users.read().unwrap()[&KeyString::from(username)].read().unwrap().password != password {
        println!("password: {:?}", password);
        return Err(EzError{tag: ErrorTag::Authentication, text: "Wrong password.".to_owned()});
    }
    Ok(())
}

pub fn read_known_length(stream: &mut TcpStream) -> Result<Vec<u8>, EzError> {
    stream.set_nonblocking(false)?;
    let mut size_buffer: [u8; 8] = [0; 8];
    stream.read_exact(&mut size_buffer)?;

    let data_len = usize::from_le_bytes(size_buffer);
    let mut data = Vec::with_capacity(data_len);
    let mut buffer = [0; 4096];
    let mut total_read: usize = 0;
    

    while total_read < data_len {
        let to_read = std::cmp::min(4096, data_len - total_read);
        let bytes_received = stream.read(&mut buffer[..to_read])?;
        println!("read: {} bytes", bytes_received);
        
        if bytes_received == 0 {
            return Err(EzError{tag: ErrorTag::Io, text: ErrorKind::BrokenPipe.to_string()});
        }
        data.extend_from_slice(&buffer[..bytes_received]);
        total_read += bytes_received;
    }

    stream.set_nonblocking(true)?;


    Ok(data)

}


pub fn ksf(s: &str) -> KeyString {
    KeyString::from(s)
}

/// Just a hash.
#[inline]
pub fn ez_hash(s: &[u8]) -> [u8;32]{
    
    let mut hasher = Sha256::new();
    hasher.update(s);
    let result = hasher.finalize();

    result.into()
    
    // blake3::hash(s).into()

}

/// Gets the current time as seconds since UNIX_EPOCH. Used for logging, mostly.
#[inline]
pub fn get_current_time() -> u64 {
    
    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

/// Gets the current time as seconds since UNIX_EPOCH. Used for logging, mostly.
#[inline]
pub fn get_precise_time() -> u128 {
    
    


    std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_micros()
}

/// Count cycles for benchmarking
#[inline(always)]
pub fn rdtsc() -> u64 {
    
    

    let lo: u32;
    let hi: u32;
    unsafe {
        asm!("rdtsc", out("eax") lo, out("edx") hi, options(nostack, preserves_flags));
    }
    ((hi as u64) << 32) | (lo as u64)
}

/// Incredibly convoluted way to print the current date. Copied from StackOverflow
pub fn time_print(s: &str, cycles: u64) {
    
    

    let num = cycles.to_string()
    .as_bytes()
    .rchunks(3)
    .rev()
    .map(std::str::from_utf8)
    .collect::<Result<Vec<&str>, _>>()
    .unwrap()
    .join(".");  // separator

    let millis = (cycles/1_700_000).to_string()
    .as_bytes()
    .rchunks(3)
    .rev()
    .map(std::str::from_utf8)
    .collect::<Result<Vec<&str>, _>>()
    .unwrap()
    .join(".");  // separator

    println!("{}: {}\n\tApproximately {} milliseconds", s, num, millis);
}


/// Removes the trailing 0 bytes from a str created from a byte buffer
pub fn bytes_to_str(bytes: &[u8]) -> Result<&str, Utf8Error> {

    let mut index: usize = 0;
    let len = bytes.len();
    let mut start: usize = 0;
    
    while index < len {
        if bytes[index] != 0 {
            break
        }
        index += 1;
        start += 1;
    }

    if bytes.is_empty() {
        return Ok("")
    }

    if start >= bytes.len()-1 {
        return Ok("")
    }

    let mut stop: usize = start;
    while index < len {
        if bytes[index] == 0 {
            break
        }
        index += 1;
        stop += 1;
    }

    str::from_utf8(&bytes[start..stop])
}

/// Parses any 8 byte slice as a usize.
#[inline]
pub fn bytes_to_usize(bytes: [u8; 8]) -> usize {

    std::primitive::usize::from_le_bytes(bytes)
}

/// Encodes a byte slice as a hexadecimal String
pub fn encode_hex(bytes: &[u8]) -> String {
    
    let mut s = String::new();
    for &b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

/// Decodes a hexadecimal String as a byte slice.
pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {

    // println!("s.len(): {}", s.len());
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}

pub fn decode_hex_to_arr32(s: &str) -> Result<[u8;32], ParseIntError> {
    
    // println!("s.len(): {}", s.len());
    let mut arr = [0u8;32];
    let mut i = 0;
    for _ in (0..s.len()).step_by(2) {
        arr[i] = u8::from_str_radix(&s[i..i+2], 16)?;
        i += 1;
    }

    Ok(arr)
}

pub fn hash_string(a: &str) -> [u8;32] {

    ez_hash(a.as_bytes())

    // blake3::hash(a.as_bytes()).into()
}

/// Creates a i32 from a &[u8] of length 4. Panics if len is different than 4. 
#[inline]
pub fn i32_from_le_slice(slice: &[u8]) -> i32 {

    assert!(slice.len() == 4);
    let l: [u8;4] = [slice[0], slice[1], slice[2], slice[3]];
    i32::from_le_bytes(l)
}

/// Creates a u32 from a &[u8] of length 4. Panics if len is different than 4.
#[inline]
pub fn u32_from_le_slice(slice: &[u8]) -> u32 {

    assert!(slice.len() == 4);
    let l: [u8;4] = [slice[0], slice[1], slice[2], slice[3]];
    u32::from_le_bytes(l)
}

/// Creates a u64 from a &[u8] of length 8. Panics if len is different than 8.
#[inline]
pub fn u64_from_le_slice(slice: &[u8]) -> u64 {

    assert!(slice.len() == 8);
    let l: [u8;8] = [ slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7] ];
    u64::from_le_bytes(l)
}

/// Creates a u32 from a &[u8] of length 4. Panics if len is different than 4.
#[inline]
pub fn f32_from_le_slice(slice: &[u8]) -> f32 {   

    assert!(slice.len() == 4);
    let l: [u8;4] = [slice[0], slice[1], slice[2], slice[3]];
    f32::from_le_bytes(l)
}

/// Creates a usize from a &[u8] of length 8. Panics if len is different than 8.
#[inline]
pub fn usize_from_le_slice(slice: &[u8]) -> usize {   

    assert!(slice.len() == 8);
    let l: [u8;8] = [slice[0], slice[1], slice[2], slice[3], slice[4], slice[5], slice[6], slice[7]];
    usize::from_le_bytes(l)
}


#[inline]
pub fn print_sep_list<T>(list: &[T], sep: &str) -> String 
where T: Display  {

    let mut printer = String::with_capacity(64*list.len());
    for item in list {
        printer.push_str(&item.to_string());
        printer.push_str(sep);
    }
    for _ in 0..sep.len() {
        printer.pop();
    }

    printer
}


#[inline]
pub fn chunk3_vec<T>(list: &[T]) -> Option<[&T;3]> {

    let mut i = list.iter();
    let one = match i.next() {
        Some(x) => x,
        None => return None,
    };
    let two = match i.next() {
        Some(x) => x,
        None => return None,
    };
    let three = match i.next() {
        Some(x) => x,
        None => return None,
    };

    Some([one, two, three])
}

#[inline]
pub fn sum_i32_slice(slice: &[i32]) -> i32 {


    let mut suma = simd::i32x4::splat(0);
    let mut sumb = simd::i32x4::splat(0);
    let mut sumc = simd::i32x4::splat(0);
    let mut sumd = simd::i32x4::splat(0);
    let mut i = 0;
    while i + 15 < slice.len() {
        suma = suma.saturating_add(simd::i32x4::from_slice(&slice[i..i+4]));
        sumb = sumb.saturating_add(simd::i32x4::from_slice(&slice[i+4..i+8]));
        sumc = sumc.saturating_add(simd::i32x4::from_slice(&slice[i+8..i+12]));
        sumd = sumd.saturating_add(simd::i32x4::from_slice(&slice[i+12..i+16]));
        i += 16;
    }

    let suma = suma.as_array().iter().fold(0, |acc: i32, x| acc.saturating_add(*x));
    let sumb = sumb.as_array().iter().fold(0, |acc: i32, x| acc.saturating_add(*x));
    let sumc = sumc.as_array().iter().fold(0, |acc: i32, x| acc.saturating_add(*x));
    let sumd = sumd.as_array().iter().fold(0, |acc: i32, x| acc.saturating_add(*x));

    let mut sum = suma.saturating_add(sumb).saturating_add(sumc).saturating_add(sumd);
    while i < slice.len() {
        sum = sum.saturating_add(slice[i]);
        i += 1;
    }

    sum
}

#[inline]
pub fn sum_f32_slice(slice: &[f32]) -> f32 {

    let mut suma = simd::f32x4::splat(0.0);
    let mut sumb = simd::f32x4::splat(0.0);
    let mut sumc = simd::f32x4::splat(0.0);
    let mut sumd = simd::f32x4::splat(0.0);
    let mut i = 0;
    while i + 15 < slice.len() {
        suma = suma + simd::f32x4::from_slice(&slice[i..i+4]);
        sumb = sumb + simd::f32x4::from_slice(&slice[i+4..i+8]);
        sumc = sumc + simd::f32x4::from_slice(&slice[i+8..i+12]);
        sumd = sumd + simd::f32x4::from_slice(&slice[i+12..i+16]);
        i += 16;
    }

    // let suma = suma.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    // let sumb = sumb.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    // let sumc = sumc.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    // let sumd = sumd.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    let suma = suma[0] + suma[1] + suma[2] + suma[3];
    let sumb = sumb[0] + sumb[1] + sumb[2] + sumb[3];
    let sumc = sumc[0] + sumc[1] + sumc[2] + sumc[3];
    let sumd = sumd[0] + sumd[1] + sumd[2] + sumd[3];

    let mut sum = suma + sumb + sumc + sumd;
    while i < slice.len() {
        sum = sum + slice[i];
        i += 1;
    }

    sum
}

pub unsafe fn raw_sum_f32_slice(slice: &[f32]) -> f32 {

    let mut suma = x86_64::_mm_setzero_ps();
    let mut sumb = x86_64::_mm_setzero_ps();
    let mut sumc = x86_64::_mm_setzero_ps();
    let mut sumd = x86_64::_mm_setzero_ps();
    let mut i = 0;
    while i + 15 < slice.len() {
        suma = x86_64::_mm_add_ps(suma, x86_64::_mm_load_ps(slice[i..i+4].as_ptr()));
        sumb = x86_64::_mm_add_ps(sumb, x86_64::_mm_load_ps(slice[i+4..i+8].as_ptr()));
        sumc = x86_64::_mm_add_ps(sumc, x86_64::_mm_load_ps(slice[i+8..i+12].as_ptr()));
        sumd = x86_64::_mm_add_ps(sumd, x86_64::_mm_load_ps(slice[i+12..i+16].as_ptr()));
        i += 16;
    }

    let mut pa = [0f32;4];
    let mut pb = [0f32;4];
    let mut pc = [0f32;4];
    let mut pd = [0f32;4];

    x86_64::_mm_store_ps(pa.as_mut_ptr(), suma);
    x86_64::_mm_store_ps(pb.as_mut_ptr(), sumb);
    x86_64::_mm_store_ps(pc.as_mut_ptr(), sumc);
    x86_64::_mm_store_ps(pd.as_mut_ptr(), sumd);

    let suma = pa.iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumb = pb.iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumc = pc.iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumd = pd.iter().fold(0.0, |acc: f32, x| acc + *x);


    let mut sum = suma + sumb + sumc + sumd;
    while i < slice.len() {
        sum = sum + slice[i];
        i += 1;
    }

    sum
}

#[inline]
pub fn mean_i32_slice(slice: &[i32]) -> f32 {

    let mut suma = simd::f32x4::splat(0.0);
    let mut sumb = simd::f32x4::splat(0.0);
    let mut sumc = simd::f32x4::splat(0.0);
    let mut sumd = simd::f32x4::splat(0.0);
    let mut i = 0;
    while i + 15 < slice.len() {
        suma = suma + simd::i32x4::from_slice(&slice[i..i+4]).cast();
        sumb = sumb + simd::i32x4::from_slice(&slice[i+4..i+8]).cast();
        sumc = sumc + simd::i32x4::from_slice(&slice[i+8..i+12]).cast();
        sumd = sumd + simd::i32x4::from_slice(&slice[i+12..i+16]).cast();
        i += 16;
    }

    let suma = suma.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumb = sumb.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumc = sumc.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumd = sumd.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);

    let mut sum = suma + sumb + sumc + sumd;
    while i < slice.len() {
        sum = sum + slice[i] as f32;
        i += 1;
    }

    sum / slice.len() as f32
}

#[inline]
pub fn mean_f32_slice(slice: &[f32]) -> f32 {

    sum_f32_slice(slice) / (slice.len() as f32)
}

#[inline]
pub fn mode_i32_slice(slice: &[i32]) -> i32 {


    let mut map = FnvHashMap::default();
    for item in slice {
        map
        .entry(item)
        .and_modify(|n| *n += 1)
        .or_insert(1);
    }

    let mut max = 0;
    let mut result = 0;
    for (key, value) in map {
        if value > max {
            max = value;
            result = key.clone();
        }
    }
    result
}


#[inline]
pub fn mode_string_slice(slice: &[KeyString]) -> KeyString {


    let mut map = FnvHashMap::default();
    for item in slice {
        map
        .entry(item)
        .and_modify(|n| *n += 1)
        .or_insert(1);
    }

    let mut max = 0;
    let mut result = KeyString::new();
    for (key, value) in map {
        if value > max {
            max = value;
            result = *key;
        }
    }
    result
}


#[inline]
pub fn stdev_i32_slice(slice: &[i32]) -> f32 {

    let mean = mean_i32_slice(slice);

    let mut variancea = simd::f32x4::splat(0.0);
    let mut varianceb = simd::f32x4::splat(0.0);
    let mut variancec = simd::f32x4::splat(0.0);
    let mut varianced = simd::f32x4::splat(0.0);

    let mut i = 0;
    while i+15 < slice.len() {
        let mut updatea: simd::f32x4 = simd::i32x4::from_slice(&slice[i..i+4]).cast();
        let mut updateb: simd::f32x4 = simd::i32x4::from_slice(&slice[i+4..i+8]).cast();
        let mut updatec: simd::f32x4 = simd::i32x4::from_slice(&slice[i+8..i+12]).cast();
        let mut updated: simd::f32x4 = simd::i32x4::from_slice(&slice[i+12..i+16]).cast();

        updatea = updatea - simd::f32x4::splat(mean);
        updateb = updateb - simd::f32x4::splat(mean);
        updatec = updatec - simd::f32x4::splat(mean);
        updated = updated - simd::f32x4::splat(mean);

        variancea = updatea * updatea;
        varianceb = updateb * updateb;
        variancec = updatec * updatec;
        varianced = updated * updated;

        i += 16;
    }

    let mut variance = variancea.as_array().iter().fold(0.0, |acc, x| acc + x);
    variance += varianceb.as_array().iter().fold(0.0, |acc, x| acc + x);
    variance += variancec.as_array().iter().fold(0.0, |acc, x| acc + x);
    variance += varianced.as_array().iter().fold(0.0, |acc, x| acc + x);

    while i < slice.len() {
        variance += (slice[i] as f32 - mean) * (slice[i] as f32 - mean);
        i += 1;
    }

    (variance/slice.len() as f32).sqrt()

}

#[inline]
pub fn stdev_f32_slice(slice: &[f32]) -> f32 {

    let mean = mean_f32_slice(slice);

    let mut variancea = simd::f32x4::splat(0.0);
    let mut varianceb = simd::f32x4::splat(0.0);
    let mut variancec = simd::f32x4::splat(0.0);
    let mut varianced = simd::f32x4::splat(0.0);

    let mut i = 0;
    while i+15 < slice.len() {
        let mut updatea: simd::f32x4 = simd::f32x4::from_slice(&slice[i..i+4]);
        let mut updateb: simd::f32x4 = simd::f32x4::from_slice(&slice[i+4..i+8]);
        let mut updatec: simd::f32x4 = simd::f32x4::from_slice(&slice[i+8..i+12]);
        let mut updated: simd::f32x4 = simd::f32x4::from_slice(&slice[i+12..i+16]);

        updatea = updatea - simd::f32x4::splat(mean);
        updateb = updateb - simd::f32x4::splat(mean);
        updatec = updatec - simd::f32x4::splat(mean);
        updated = updated - simd::f32x4::splat(mean);

        variancea = updatea * updatea;
        varianceb = updateb * updateb;
        variancec = updatec * updatec;
        varianced = updated * updated;

        i += 16;
    }

    let mut variance = variancea.as_array().iter().fold(0.0, |acc, x| acc + x);
    variance += varianceb.as_array().iter().fold(0.0, |acc, x| acc + x);
    variance += variancec.as_array().iter().fold(0.0, |acc, x| acc + x);
    variance += varianced.as_array().iter().fold(0.0, |acc, x| acc + x);

    while i < slice.len() {
        variance += (slice[i] - mean) * (slice[i] - mean);
        i += 1;
    }

    (variance/slice.len() as f32).sqrt()
}

#[inline]
fn partition<T: Copy + PartialOrd>(data: &[T]) -> (Vec<T>, T, Vec<T>) {

    let (pivot_slice, tail) = data.split_at(1);
    let pivot = pivot_slice[0];

    let mut left = Vec::new();
    let mut right = Vec::new();
    for item in tail.iter() {
        if *item < pivot {
            left.push(*item);
        } else {
            right.push(*item);
        }
    }

    (left, pivot, right)
}

#[inline]
fn select<T: Copy + PartialOrd>(data: &[T], k: usize) -> T {


    let (left, pivot, right) = partition(data);

    let pivot_idx = left.len();

    match pivot_idx.cmp(&k) {
        std::cmp::Ordering::Equal => pivot,
        std::cmp::Ordering::Greater => select(&left, k),
        std::cmp::Ordering::Less => select(&right, k - (pivot_idx + 1)),
    }
}

#[inline]
pub fn median_i32_slice(data: &[i32]) -> f32 {

    match data.len() {
        even if even % 2 == 0 => {
            let fst_med = select(data, (even / 2) - 1);
            let snd_med = select(data, even / 2);

            (fst_med + snd_med) as f32 / 2.0
        },
        odd => select(data, odd / 2) as f32
    }
}

#[inline]
pub fn median_f32_slice(data: &[f32]) -> f32 {


    match data.len() {
        even if even % 2 == 0 => {
            let fst_med = select(data, (even / 2) - 1);
            let snd_med = select(data, even / 2);

            (fst_med + snd_med) / 2.0
        },
        odd => select(data, odd / 2)
    }
}

#[inline]
pub fn bytes_from_strings(strings: &[&str]) -> Vec<u8> {

    let mut v = Vec::with_capacity(strings.len()*64);
    for string in strings {
        v.extend_from_slice(KeyString::from(*string).raw());
    }

    v
}

/// Helper function that parses a response from instruction_send_and_confirm().
#[inline]
pub fn parse_response(response: &str, username: &str, table_name: &str) -> Result<(), EzError> {

    if response == "OK" {
        Ok(())
    } else if response == "IU" {
        Err(EzError{tag: ErrorTag::ParseResponse, text: format!("Username: {}, is invalid", username)})
    } else if response == "IP" {
        Err(EzError{tag: ErrorTag::ParseResponse, text: "Password is invalid".to_owned()})
    } else if response == ("NT") {
        Err(EzError{tag: ErrorTag::ParseResponse, text: format!("No such table as {}", table_name)})
    } else {
        panic!("Need to handle error: {}", response);
    }

}

pub fn kv_query_results_to_binary(query_results: &Vec<Result<Option<Value>, EzError>>) -> Vec<u8> {
    let mut binary = Vec::new();
    binary.extend_from_slice(&query_results.len().to_le_bytes());
    for _ in 0..query_results.len() {
        binary.extend_from_slice(&[0u8;8]);
    }
    let mut offsets = Vec::new();

    for result in query_results {
        dbg!(&result);
        match result {
            Ok(x) => match x {
                Some(value) => {
                    let len = value.body.len();
                    let mut temp = Vec::new();
                    temp.extend_from_slice(ksf("VALUE").raw());
                    temp.extend_from_slice(value.name.raw());
                    temp.extend_from_slice(&len.to_le_bytes());
                    temp.extend_from_slice(&value.body);
                    offsets.push(temp.len());
                    binary.extend_from_slice(&temp);
                },
                None => {
                    let mut temp = Vec::new();
                    temp.extend_from_slice(ksf("NONE").raw());
                    offsets.push(temp.len());
                    binary.extend_from_slice(&temp);
                }
            },
            Err(e) => {
                let mut temp = Vec::new();
                temp.extend_from_slice(ksf("ERROR").raw());
                temp.extend_from_slice(&e.to_binary());
                offsets.push(temp.len());
                binary.extend_from_slice(&temp);
            }
        }
    }

    let mut i = 0;
    for offset in offsets {
        binary[8+i..8+i+8].copy_from_slice(&offset.to_le_bytes());
        i += 8;
    }
    binary
}

pub fn kv_query_results_from_binary(binary: &[u8]) -> Result<Vec<Result<Option<Value>, EzError>>, EzError> {
    let number_of_responses = u64_from_le_slice(&binary[0..8]) as usize;
    let mut offsets = Vec::new();
    let mut last = 0;
    for i in 0..number_of_responses {
        let offset = u64_from_le_slice(&binary[8+8*i..8+8*i+8]) as usize;
        offsets.push(last + offset);
        last += offset;
    }

    println!("offsets: {:?}", offsets);
    
    let body = &binary[8+8*offsets.len()..];
    
    let mut results = Vec::new();
    for i in 0..offsets.len() {
        println!("i: {}", i);
        let current_blob: &[u8];
        if i == 0 {
            current_blob = &body[0..offsets[i]];
        } else {
            current_blob = &body[offsets[i-1]..offsets[i]];
        }

        let tag = KeyString::try_from(&current_blob[0..64]).unwrap();
        match tag.as_str() {
            "VALUE" => {
                let name = KeyString::try_from(&current_blob[64..128])?;
                let len = u64_from_le_slice(&current_blob[128..136]) as usize;
                let value = current_blob[136..136+len].to_vec();
                let value = Value {name, body: value};
                results.push(Ok(Some(value)));
            },
            "ERROR" => {
                let error = EzError::from_binary(&current_blob[64..])?;
                results.push(Err(error));
            } ,
            "NONE"  => {
                results.push(Ok(None));
            },
            other => {
                results.push(Err(EzError{tag: ErrorTag::Query, text: format!("Incorrectly formatted response. '{}' is not a valid response type", other)}));
            }
        }

    }

    Ok(results)
}


#[cfg(test)]
mod tests {
    use crate::testing_tools::random_ez_error;

    use super::*;

    #[test]
    fn test_kv_queries_serde() {
        let results: Vec<Result<Option<Value>, EzError>> = vec![
            Ok(
                Some(Value{name: ksf("test2"), body: vec![8,7,6,5,4,3,2,1]}),
            ),
            Ok(
                Some(Value{name: ksf("test2"), body: vec![0,0,0,0,0,0,0,0]}),
            ),
            Ok(
                Some(Value{name: ksf("test1"), body: vec![1,2,3,4,5,6,7,8]}),
            ),
            Ok(
                None,
            ),
            Err(
                EzError{tag: ErrorTag::Query, text: "Test".to_owned()}
            )

        ];

        let binary = kv_query_results_to_binary(&results);

        let parsed = kv_query_results_from_binary(&binary).unwrap();

        assert_eq!(results, parsed);
    }

    #[test]
    fn test_bytes_to_str() {
        let bytes = [0,0,0,0,0,49,50,51,0,0,0,0,0];
        let x = bytes_to_str(&bytes).unwrap();
        assert_eq!("123", x);
    }

    #[test]
    fn test_encode_hex() {
        let byte = [0u8];
        let x = encode_hex(&byte);
        println!("{}", x);
    }

    #[test]
    fn test_median() {
        let data = [3, 1, 6, 1, 5, 8, 1, 8, 10, 11];
    
        let med = median_i32_slice(&data);
        assert_eq!(med, 5.5);
    }

    #[test]
    fn test_mode() {
        let data = [3, 1, 6, 1, 5, 8, 1, 8, 10, 11];
    
        let mode = mode_i32_slice(&data);
        assert_eq!(mode, 1);

        let text_data = [KeyString::from("3"), KeyString::from("1"), KeyString::from("6"), KeyString::from("1"), KeyString::from("5"), KeyString::from("8"), KeyString::from("1"), KeyString::from("8"), KeyString::from("10"), KeyString::from("11")];
        let text_mode = mode_string_slice(&text_data);
        assert_eq!(text_mode, KeyString::from("1"));
    }

    #[test]
    fn test_mean() {
        let data = [3, 1, 6, 1, 5, 8, 1, 8, 10, 11];
    
        let mean = mean_i32_slice(&data);
        assert_eq!(mean, 5.4);
    }

    #[test]
    fn test_stdev() {
        let data = [3, 1, 6, 1, 5, 8, 1, 8, 10, 11, 3, 1, 6, 1, 5, 8, 1, 8, 10, 11];
        let stdev = stdev_i32_slice(&data);
        println!("stdev: {}", stdev);
        assert!(stdev > 3.611 && stdev < 3.612);
    }

    #[test]
    fn test_sum_i32_slice() {
        let data = [3, 6, 9];
        let sum = sum_i32_slice(&data);
        println!("sum: {}", sum);
        assert!(sum == 18);
    }

    #[test]
    fn test_sum_f32_slice() {
        let data = [3.0, 6.0, 9.0];
        let sum = sum_f32_slice(&data);
        println!("sum: {}", sum);
        assert!(sum == 18.0);
    }

    #[test]
    fn test_ez_error_serde() {
        for _ in 0..100 {
            let error = random_ez_error();
            let binary = error.to_binary();
            let parsed = EzError::from_binary(&binary).unwrap();
            assert_eq!(error, parsed);
        }
    }

}
