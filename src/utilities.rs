use std::arch::asm;
use std::fmt::Display;
use std::simd;
use std::io::{Write, Read, ErrorKind};
use std::net::TcpStream;
use std::num::ParseIntError;
use std::simd::num::SimdInt;
use std::str::{self, Utf8Error};
use std::sync::Arc;
use std::time::Duration;
use std::{usize, fmt};

use ezcbor::cbor::CborError;
use fnv::FnvHashMap;
use x25519_dalek::{EphemeralSecret, PublicKey};
use aes_gcm::aead;

use crate::aes_temp_crypto::{encrypt_aes256, decrypt_aes256};
use crate::auth::AuthenticationError;
use crate::compression;
use crate::db_structure::{KeyString, StrictError};
use crate::ezql::QueryError;
use crate::server_networking::{Database, Server};


pub const INSTRUCTION_BUFFER: usize = 1024;
pub const DATA_BUFFER: usize = 1_000_000;
pub const INSTRUCTION_LENGTH: usize = 4;
pub const MAX_DATA_LEN: usize = u32::MAX as usize;



/// The main error of all networking. Any error that can occur during a networking function should be covered here.
#[derive(Debug)]
pub enum EzError {
    Utf8(Utf8Error),
    Io(ErrorKind),
    Instruction(InstructionError),
    Confirmation(String),
    Authentication(AuthenticationError),
    Strict(StrictError),
    Crypto(aead::Error),
    ParseInt(ParseIntError),
    ParseResponse(String),
    ParseUser(String),
    OversizedData,
    Decompression(miniz_oxide::inflate::DecompressError),
    Query(String),
    Debug(String),
    NoMoreBufferSpace(usize),
    Unimplemented(String),
    Serialization(String),
}

impl fmt::Display for EzError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            EzError::Utf8(e) => write!(f, "Encontered invalid utf-8: {}", e),
            EzError::Io(e) => write!(f, "Encountered an IO error: {}", e),
            EzError::Instruction(e) => write!(f, "{}", e),
            EzError::Confirmation(e) => write!(f, "Received corrupt confirmation {:?}", e),
            EzError::Authentication(e) => write!(f, "{}", e),
            EzError::Strict(e) => write!(f, "{}", e),
            EzError::Crypto(e) => write!(f, "There has been a crypto error. Most likely the nonce was incorrect. The error is: {}", e),
            EzError::ParseInt(e) => write!(f, "There has been a problem parsing an integer, presumably while sending a data_len. The error signature is: {}", e),
            EzError::ParseUser(e) => write!(f, "Failed to parse user from string because: {}", e),
            EzError::OversizedData => write!(f, "Sent data is too long. Maximum data size is {MAX_DATA_LEN}"),
            EzError::ParseResponse(e) => write!(f, "{}", e),
            EzError::Decompression(e) => write!(f, "Decompression error occurred from miniz_oxide library.\nLibrary error: {}", e),
            EzError::Query(s) => write!(f, "Query could not be processed because of: {}", s),
            EzError::NoMoreBufferSpace(x) => write!(f, "No more space in buffer pool. Need to free {x} bytes"),
            EzError::Unimplemented(s) => write!(f, "{}", s),
            EzError::Debug(s) => write!(f, "{}", s),
            EzError::Serialization(s) => write!(f, "{}", s),

        }
    }
}

impl From<std::io::Error> for EzError {
    fn from(e: std::io::Error) -> Self {
        EzError::Io(e.kind())
    }
}

impl From<Utf8Error> for EzError {
    fn from(e: Utf8Error) -> Self {
        EzError::Utf8(e)
    }
}

impl From<InstructionError> for EzError {
    fn from(e: InstructionError) -> Self {
        EzError::Instruction(e)
    }
}

impl From<AuthenticationError> for EzError {
    fn from(e: AuthenticationError) -> Self {
        EzError::Authentication(e)
    }
}

impl From<StrictError> for EzError {
    fn from(e: StrictError) -> Self {
        EzError::Strict(e)
    }
}

impl From<aead::Error> for EzError {
    fn from(e: aead::Error) -> Self {
        EzError::Crypto(e)
    }
}

impl From<ParseIntError> for EzError {
    fn from(e: ParseIntError) -> Self {
        EzError::ParseInt(e)
    }
}

impl From<QueryError> for EzError {
    fn from(e: QueryError) -> Self {
        EzError::Query(e.to_string())
    }
}

impl From<CborError> for EzError {
    fn from(e: CborError) -> Self {
        let s = match e {
            CborError::IllFormed(x) => x,
            CborError::Unexpected(x) => x,
        };
        EzError::Serialization(s)
    }
}

impl EzError {
    pub fn to_error_code(&self) -> u64{
        match self {
            EzError::Utf8(_) => 1,
            EzError::Io(_) => 2,
            EzError::Instruction(_) => 3,
            EzError::Confirmation(_) => 4,
            EzError::Authentication(_) => 5,
            EzError::Strict(_) => 6,
            EzError::Crypto(_) => 7,
            EzError::ParseInt(_) => 8,
            EzError::ParseResponse(_) => todo!(),
            EzError::ParseUser(_) => todo!(),
            EzError::OversizedData => todo!(),
            EzError::Decompression(_) => todo!(),
            EzError::Query(_) => todo!(),
            EzError::Debug(_) => todo!(),
            EzError::NoMoreBufferSpace(_) => todo!(),
            EzError::Unimplemented(_) => todo!(),
            EzError::Serialization(_) => todo!(),
        }
    }
}

/// An enum that lists the possible instructions that the database can receive.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Instruction {
    Upload(KeyString),
    Download(KeyString),
    Update(KeyString),
    Query(String),
    Delete(KeyString),
    NewUser(Vec<u8>),
    KvUpload(KeyString),
    KvUpdate(KeyString),
    KvDelete(KeyString),
    KvDownload(KeyString),
    MetaListTables,
    MetaListKeyValues,
}

impl Display for Instruction {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Instruction::Upload(s) => write!(f, "Upload({})", s),
            Instruction::Download(s) => write!(f, "Download({})", s),
            Instruction::Update(s) => write!(f, "Update({})", s),
            Instruction::Query(s) => write!(f, "Query({})", s),
            Instruction::Delete(s) => write!(f, "Delete({})", s),
            Instruction::NewUser(s) => write!(f, "NewUser({:x?})", s),
            Instruction::KvUpload(s) => write!(f, "KvUpload({})", s),
            Instruction::KvUpdate(s) => write!(f, "KvUpdate({})", s),
            Instruction::KvDelete(s) => write!(f, "KvDelete({})", s),
            Instruction::KvDownload(s) => write!(f, "KvDownload({})", s),
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


/// A connection to a peer. The client side uses the same struct.
pub struct Connection {
    pub stream: TcpStream,
    pub user: String,
    pub aes_key: [u8;32],   
}

impl Connection {
    /// Initialize a connection. This means doing diffie hellman key exchange and establishing a shared secret
    pub fn connect(address: &str, username: &str, password: &str) -> Result<Connection, EzError> {

        if username.len() > 512 || password.len() > 512 {
            return Err(EzError::Authentication(AuthenticationError::TooLong))
        }

        let client_private_key = EphemeralSecret::random();
        let client_public_key = PublicKey::from(&client_private_key);

        let mut stream = TcpStream::connect(address)?;
        let mut key_buffer: [u8; 32] = [0u8;32];
        stream.read_exact(&mut key_buffer)?;
        let server_public_key = PublicKey::from(key_buffer);
        stream.write_all(client_public_key.as_bytes())?;
        let shared_secret = client_private_key.diffie_hellman(&server_public_key);
        let aes_key = blake3_hash(&shared_secret.to_bytes());

        let mut auth_buffer = [0u8; 1024];
        auth_buffer[0..username.len()].copy_from_slice(username.as_bytes());
        auth_buffer[512..512+password.len()].copy_from_slice(password.as_bytes());
        // println!("auth_buffer: {:x?}", auth_buffer);
        
        let (encrypted_data, data_nonce) = encrypt_aes256(&auth_buffer, &aes_key);
        println!("data_nonce: {:x?}", data_nonce);
        // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
        // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
        let mut encrypted_data_block = Vec::with_capacity(encrypted_data.len() + 28);
        encrypted_data_block.extend_from_slice(&encrypted_data);
        encrypted_data_block.extend_from_slice(&data_nonce);
        // println!("Encrypted auth string: {:x?}", encrypted_data_block);
        // println!("Encrypted auth string.len(): {}", encrypted_data_block.len());
        
        // println!("Sending data...");
        stream.write_all(&encrypted_data_block)?;
        stream.flush()?;
        stream.set_read_timeout(Some(Duration::from_secs(20)))?;

        let user = username.to_owned();
        Ok(
            Connection {
                stream: stream,
                user: user,
                aes_key: aes_key,
            }
        )

    }
}

/// THe server side of the Connection exchange
pub fn establish_connection(mut stream: TcpStream, server: Arc<Server>, db_ref: Arc<Database>) -> Result<Connection, EzError> {

    match stream.write(server.public_key.as_bytes()) {
        Ok(_) => (),
        Err(e) => {
            println!("failed to write server public key because: {}", e);
            return Err(EzError::Io(e.kind()));
        }
    }
    println!("About to get crypto");
    let mut buffer: [u8; 32] = [0; 32];
    
    match stream.read_exact(&mut buffer){
        Ok(_) => (),
        Err(e) => {
            println!("failed to read client public key because: {}", e);
            return Err(EzError::Io(e.kind()));
        }
    }
    
    let client_public_key = PublicKey::from(buffer);
    
    let shared_secret = server.private_key.diffie_hellman(&client_public_key);
    let aes_key = blake3_hash(shared_secret.as_bytes());

    let mut auth_buffer = [0u8; 1052];
    println!("About to read auth string");
    match stream.read_exact(&mut auth_buffer) {
        Ok(_) => (),
        Err(e) => {
            println!("failed to read auth_string because: {}", e);
            return Err(EzError::Io(e.kind()));
        }
    }
    // println!("encrypted auth_buffer: {:x?}", auth_buffer);
    // println!("Encrypted auth_buffer.len(): {}", auth_buffer.len());

    let (ciphertext, nonce) = (&auth_buffer[0..auth_buffer.len()-12], &auth_buffer[auth_buffer.len()-12..auth_buffer.len()]);
    println!("About to decrypt auth string");
    let auth_string = match decrypt_aes256(ciphertext, &aes_key, nonce) {
        Ok(s) => s,
        Err(e) => {
            println!("failed to decrypt auth string because: {}", e);
            return Err(e);
        }
    };
    println!("About to parse auth_string");
    let username = match bytes_to_str(&auth_string[0..512]) {
        Ok(s) => s,
        Err(e) => {
            println!("failed to read auth_string from bytes because: {}", e);
            return Err(EzError::Utf8(e));
        }
    };
    let password = &auth_string[512..];
    println!("password: {:?}", password);

    // println!("username: {}\npassword: {:x?}", username, password);
    let password = blake3_hash(bytes_to_str(password).unwrap().as_bytes());
    println!("password: {:?}", password);
    // println!("password_hash: {:x?}", password);
    println!("About to verify username and password");

    {
        let users_lock = db_ref.users.read().unwrap();
        if !users_lock.contains_key(&KeyString::from(username)) {
            println!("printing keys..");

            for key in users_lock.keys() {
                println!("key: '{}'", key);
            }
            println!("Username:\n\t'{}'\n...is wrong", username);
            return Err(EzError::Authentication(AuthenticationError::WrongUser(format!("Username: '{}' does not exist", username))));
        } else if db_ref.users.read().unwrap()[&KeyString::from(username)].read().unwrap().password != password {
            // println!("thread_users_lock[username].password: {:?}", user_lock.password);
            // println!("password: {:?}", password);
            // println!("Password hash:\n\t{:?}\n...is wrong", password);
            return Err(EzError::Authentication(AuthenticationError::WrongPassword));
        }
        Ok(
            Connection {
                stream: stream, 
                user: username.to_owned(), 
                aes_key: aes_key
            }
        )
    }

}

fn extract_query(request: &str) -> &str {
    if let Some(pos) = request.find("\r\n\r\n") {
        return &request[pos + 4..];
    }
    ""
}

pub fn check_if_http_request(stream: &TcpStream) -> Result<String, EzError> {

    let mut buffer = [0u8;1024];
    stream.peek(&mut buffer)?;

    let text = bytes_to_str(&buffer)?;
    if text.starts_with("POST /query HTTP/1.1") {
        Ok(extract_query(text).to_owned())
    } else {
        Err(EzError::Query("Not http. Proceed with normal".to_owned()))
    }


}

/// Just a blake3 hash.
#[inline]
pub fn blake3_hash(s: &[u8]) -> [u8;32]{
    blake3::hash(s).into()
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

/// Just a blake3 hash
pub fn hash_function(a: &str) -> [u8;32] {
    blake3::hash(a.as_bytes()).into()
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

    let suma = suma.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumb = sumb.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumc = sumc.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);
    let sumd = sumd.as_array().iter().fold(0.0, |acc: f32, x| acc + *x);

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



pub fn instruction_send_and_confirm(instruction: Instruction, connection: &mut Connection) -> Result<String, EzError> {
    let instruction = match instruction {
        Instruction::Download(table_name) => bytes_from_strings(&[&connection.user, "Downloading", &table_name.as_str(),"blank", ]),
        Instruction::Upload(table_name) => bytes_from_strings(&[&connection.user, "Uploading", &table_name.as_str(),"blank", ]), 
        Instruction::Update(table_name) => bytes_from_strings(&[&connection.user, "Updating", &table_name.as_str(),"blank", ]), 
        Instruction::Query(query) => {
            let mut q = bytes_from_strings(&[&connection.user, "Querying", "blank", ]);
            q.extend_from_slice(query.as_bytes());
            q
        }, 
        Instruction::Delete(table_name) => bytes_from_strings(&[&connection.user, "Deleting", &table_name.as_str(),"blank", ]), 
        Instruction::NewUser(user_string) => {
            let mut bytes = bytes_from_strings(&[&connection.user, "NewUser", "blank"]);
            bytes.extend_from_slice(&user_string);
            bytes
        }
        Instruction::KvUpload(table_name) => bytes_from_strings(&[&connection.user, "KvUpload", &table_name.as_str(),"blank", ]), 
        Instruction::KvUpdate(table_name) => bytes_from_strings(&[&connection.user, "KvUpdate", &table_name.as_str(),"blank", ]),
        Instruction::KvDelete(table_name) => bytes_from_strings(&[&connection.user, "KvDelete", &table_name.as_str(),"blank", ]),
        Instruction::KvDownload(table_name) => bytes_from_strings(&[&connection.user, "KvDownload", &table_name.as_str(),"blank", ]), 
        Instruction::MetaListTables => bytes_from_strings(&[&connection.user, "MetaListTables", "blank","blank", ]), 
        Instruction::MetaListKeyValues => bytes_from_strings(&[&connection.user, "MetaListKeyValues", "blank","blank", ]), 
    };

    println!("{:x?}", instruction);

    let (encrypted_instructions, nonce) = encrypt_aes256(&instruction, &connection.aes_key);

    let mut encrypted_data_block = Vec::with_capacity(encrypted_instructions.len() + 28);
    encrypted_data_block.extend_from_slice(&encrypted_instructions);
    encrypted_data_block.extend_from_slice(&nonce);

    // // println!("encrypted instructions.len(): {}", encrypted_instructions.len());
    match connection.stream.write_all(&encrypted_data_block) {
        Ok(_) => println!("Wrote request as {} bytes", encrypted_data_block.len()),
        Err(e) => {return Err(EzError::Io(e.kind()));},
    };
    connection.stream.flush()?;
    
    let mut buffer: [u8;2] = [0;2];
    println!("Waiting for response from server");
    connection.stream.read_exact(&mut buffer)?;
    println!("INSTRUCTION_BUFFER: {:x?}", buffer);
    println!("About to parse response from server");
    let response = bytes_to_str(&buffer)?;
    println!("repsonse: {}", response);

    Ok(response.to_owned())

}



/// Helper function that parses a response from instruction_send_and_confirm().
#[inline]
pub fn parse_response(response: &str, username: &str, table_name: &str) -> Result<(), EzError> {

    if response == "OK" {
        Ok(())
    } else if response == "IU" {
        Err(EzError::ParseResponse(format!("Username: {}, is invalid", username)))
    } else if response == "IP" {
        Err(EzError::ParseResponse("Password is invalid".to_owned()))
    } else if response == ("NT") {
        Err(EzError::ParseResponse(format!("No such table as {}", table_name)))
    } else {
        panic!("Need to handle error: {}", response);
    }

}

/// This is the function primarily responsible for transmitting data.
/// It compresses, encrypts, sends, and confirms receipt of the data.
/// Used by both client and server.
pub fn data_send_and_confirm(connection: &mut Connection, data: &[u8]) -> Result<String, EzError> {

    // // println!("data: {:x?}", data);

    let data = compression::miniz_compress(data)?;
    let (encrypted_data, data_nonce) = encrypt_aes256(&data, &connection.aes_key);

    let mut encrypted_data_block = Vec::with_capacity(data.len() + 28);
    encrypted_data_block.extend_from_slice(&encrypted_data);
    encrypted_data_block.extend_from_slice(&data_nonce);


    // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
    // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
    let mut block = Vec::from(&(data.len() + 28).to_le_bytes());
    block.extend_from_slice(&encrypted_data_block);
    connection.stream.write_all(&block)?;
    // connection.stream.write_all(&(data.len() + 28).to_le_bytes())?;
    // connection.stream.write_all(&encrypted_data_block)?;
    
    // println!("data sent");
    let mut buffer: [u8;INSTRUCTION_BUFFER] = [0;INSTRUCTION_BUFFER];
    match connection.stream.read(&mut buffer) {
        Ok(_) => {
            println!("Confirmation '{}' received", bytes_to_str(&buffer)?);
        },
        Err(_) => println!("Did not confirm transmission with peer"),
    }
    
    let confirmation = bytes_to_str(&buffer).unwrap_or("corrupt data");
    Ok(confirmation.to_owned())

}

/// This is the function primarily responsible for receiving data.
/// It receives, decompresses, decrypts, and confirms receipt of the data.
/// Used by both client and server.
pub fn receive_data(connection: &mut Connection) -> Result<Vec<u8>, EzError> {
    
    let mut size_buffer: [u8; 8] = [0; 8];
    connection.stream.read_exact(&mut size_buffer)?;
    println!("HERE 4!!!");

    let data_len = usize::from_le_bytes(size_buffer);
    if data_len > MAX_DATA_LEN {
        return Err(EzError::OversizedData)
    }
    
    let mut data = Vec::with_capacity(data_len);
    let mut buffer = [0; DATA_BUFFER];
    let mut total_read: usize = 0;
    
    while total_read < data_len {
        let to_read = std::cmp::min(DATA_BUFFER, data_len - total_read);
        let bytes_received = connection.stream.read(&mut buffer[..to_read])?;
        if bytes_received == 0 {
            return Err(EzError::Confirmation("Read failure".to_owned()));
        }
        data.extend_from_slice(&buffer[..bytes_received]);
        total_read += bytes_received;
        println!("Total read: {}", total_read);
    }
    println!("HERE 3!!!");
    


    let (ciphertext, nonce) = (&data[0..data.len()-12], &data[data.len()-12..]);
    let csv = decrypt_aes256(ciphertext, &connection.aes_key, nonce)?;

    let csv = compression::miniz_decompress(&csv)?;
    Ok(csv)
}



#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_sum_i32() {
        let i32_slice: Vec<i32> = (0..98304).collect();
        let start = std::time::Instant::now();
        for i in 0..100 {
            let sum = sum_i32_slice(&i32_slice);
        }
        let stop = start.elapsed().as_millis();
        println!("{}", stop);
    }

    #[test]
    fn test_sum_f32_slice() {
        let data = [3.0, 6.0, 9.0];
        let sum = sum_f32_slice(&data);
        println!("sum: {}", sum);
        assert!(sum == 18.0);
    }

}
