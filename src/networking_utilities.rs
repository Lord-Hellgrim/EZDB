use std::arch::asm;
use std::fmt::Display;
use std::hash::Hash;
use std::io::{Write, Read, ErrorKind};
use std::net::TcpStream;
use std::num::ParseIntError;
use std::str::{self, Utf8Error};
use std::time::Duration;
use std::{usize, fmt};

use fnv::FnvHashMap;
use x25519_dalek::{EphemeralSecret, PublicKey};
use aes_gcm::aead;

use crate::aes_temp_crypto::{encrypt_aes256, decrypt_aes256};
use crate::auth::AuthenticationError;
use crate::compression;
use crate::db_structure::{KeyString, StrictError};
use crate::ezql::QueryError;


pub const INSTRUCTION_BUFFER: usize = 1024;
pub const DATA_BUFFER: usize = 1_000_000;
pub const INSTRUCTION_LENGTH: usize = 4;
pub const MAX_DATA_LEN: usize = u32::MAX as usize;



/// The main error of all networking. Any error that can occur during a networking function should be covered here.
#[derive(Debug)]
pub enum ServerError {
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
}

impl fmt::Display for ServerError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ServerError::Utf8(e) => write!(f, "Encontered invalid utf-8: {}", e),
            ServerError::Io(e) => write!(f, "Encountered an IO error: {}", e),
            ServerError::Instruction(e) => write!(f, "{}", e),
            ServerError::Confirmation(e) => write!(f, "Received corrupt confirmation {:?}", e),
            ServerError::Authentication(e) => write!(f, "{}", e),
            ServerError::Strict(e) => write!(f, "{}", e),
            ServerError::Crypto(e) => write!(f, "There has been a crypto error. Most likely the nonce was incorrect. The error is: {}", e),
            ServerError::ParseInt(e) => write!(f, "There has been a problem parsing an integer, presumably while sending a data_len. The error signature is: {}", e),
            ServerError::ParseUser(e) => write!(f, "Failed to parse user from string because: {}", e),
            ServerError::OversizedData => write!(f, "Sent data is too long. Maximum data size is {MAX_DATA_LEN}"),
            ServerError::ParseResponse(e) => write!(f, "{}", e),
            ServerError::Decompression(e) => write!(f, "Decompression error occurred from miniz_oxide library.\nLibrary error: {}", e),
            ServerError::Query(s) => write!(f, "Query could not be processed because of: {}", s),
            ServerError::NoMoreBufferSpace(x) => write!(f, "No more space in buffer pool. Need to free {x} bytes"),
            ServerError::Unimplemented(s) => write!(f, "{}", s),
            ServerError::Debug(s) => write!(f, "{}", s),

        }
    }
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        ServerError::Io(e.kind())
    }
}

impl From<Utf8Error> for ServerError {
    fn from(e: Utf8Error) -> Self {
        ServerError::Utf8(e)
    }
}

impl From<InstructionError> for ServerError {
    fn from(e: InstructionError) -> Self {
        ServerError::Instruction(e)
    }
}

impl From<AuthenticationError> for ServerError {
    fn from(e: AuthenticationError) -> Self {
        ServerError::Authentication(e)
    }
}

impl From<StrictError> for ServerError {
    fn from(e: StrictError) -> Self {
        ServerError::Strict(e)
    }
}

impl From<aead::Error> for ServerError {
    fn from(e: aead::Error) -> Self {
        ServerError::Crypto(e)
    }
}

impl From<ParseIntError> for ServerError {
    fn from(e: ParseIntError) -> Self {
        ServerError::ParseInt(e)
    }
}

impl From<QueryError> for ServerError {
    fn from(e: QueryError) -> Self {
        ServerError::Query(e.to_string())
    }
}

/// An enum that lists the possible instructions that the database can receive.
/// Will be rewritten soon to handle EZQL.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Instruction {
    Upload(String),
    Download(String),
    Update(String),
    Query(String),
    Delete(String /* table_name */, String /* query */),
    NewUser(String),
    KvUpload(String),
    KvUpdate(String),
    KvDownload(String),
    MetaListTables,
    MetaListKeyValues,
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
            InstructionError::TooLong => write!(f, "Your instruction is too long. Maximum instruction length is: {INSTRUCTION_BUFFER}\n\n"),
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
    pub fn connect(address: &str, username: &str, password: &str) -> Result<Connection, ServerError> {

        if username.len() > 512 || password.len() > 512 {
            return Err(ServerError::Authentication(AuthenticationError::TooLong))
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
pub fn print_sep_list<T>(list: &Vec<T>, sep: &str) -> String 
where T: Display  {
    let mut printer = String::new();
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
pub fn sum_i32_slice(slice: &[i32]) -> Option<i32> {
    let mut sum: i32 = 0;
    for item in slice {
        match sum.checked_add(*item) {
            Some(x) => sum += x,
            None => return None,
        }
    }
    Some(sum)
}

#[inline]
pub fn sum_f32_slice(slice: &[f32]) -> f32 {
    slice.iter().sum::<f32>()
}

#[inline]
pub fn mean_i32_slice(slice: &[i32]) -> f32 {
    slice.iter().map(|n| *n as f32).sum::<f32>() / (slice.len() as f32)
}

#[inline]
pub fn mean_f32_slice(slice: &[f32]) -> f32 {
    slice.iter().sum::<f32>() / (slice.len() as f32)
}

#[inline]
pub fn mode_i32_slice(slice: &[i32]) -> i32 {

    let mut map = FnvHashMap::default();
    for item in slice {
        map.entry(item).and_modify(|n| *n += 1).or_insert(1);
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
        map.entry(item).and_modify(|n| *n += 1).or_insert(1);
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

    let mut variance = 0.0;
    for item in slice {
        variance += (*item as f32 - mean) * (*item as f32 - mean);
    }


    (variance/slice.len() as f32).sqrt()
}

#[inline]
pub fn stdev_f32_slice(slice: &[f32]) -> f32 {
    let mean = mean_f32_slice(slice);

    let mut variance = 0.0;
    for item in slice {
        variance += (*item - mean) * (*item - mean);
    }


    (variance/slice.len() as f32).sqrt()
}

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

fn select<T: Copy + PartialOrd>(data: &[T], k: usize) -> T {

    let (left, pivot, right) = partition(data);

    let pivot_idx = left.len();

    match pivot_idx.cmp(&k) {
        std::cmp::Ordering::Equal => pivot,
        std::cmp::Ordering::Greater => select(&left, k),
        std::cmp::Ordering::Less => select(&right, k - (pivot_idx + 1)),
    }
}

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

/// This is a utility function that sends an instruction from the client to the server and returns the servers answer.
pub fn instruction_send_and_confirm(instruction: Instruction, connection: &mut Connection) -> Result<String, ServerError> {

    let instruction = match instruction {
        Instruction::Download(table_name) => format!("Downloading|{}|blank|{}", table_name, connection.user),
        Instruction::Upload(table_name) => format!("Uploading|{}|blank|{}", table_name, connection.user),
        Instruction::Update(table_name) => format!("Updating|{}|blank|{}", table_name, connection.user),
        Instruction::Query(query) => format!("Querying|blank|{}|{}", query, connection.user),
        Instruction::Delete(table_name, query) => format!("Deleting|{}|{}|{}", table_name, query, connection.user),
        Instruction::NewUser(user_string) => format!("NewUser|{}|blank|{}", user_string, connection.user),
        Instruction::KvUpload(table_name) => format!("KvUpload|{}|blank|{}", table_name, connection.user),
        Instruction::KvUpdate(table_name) => format!("KvUpdate|{}|blank|{}", table_name, connection.user),
        Instruction::KvDownload(table_name) => format!("KvDownload|{}|blank|{}", table_name, connection.user),
        Instruction::MetaListTables => format!("MetaListTables|blank|blank|{}", connection.user),
        Instruction::MetaListKeyValues => format!("MetaListKeyValues|blank|blank|{}", connection.user),
    };

    let (encrypted_instructions, nonce) = encrypt_aes256(instruction.as_bytes(), &connection.aes_key);

    // // println!("encrypted instructions: {:x?}", encrypted_instructions);
    // // println!("nonce: {:x?}", nonce);

    let mut encrypted_data_block = Vec::with_capacity(encrypted_instructions.len() + 28);
    encrypted_data_block.extend_from_slice(&encrypted_instructions);
    encrypted_data_block.extend_from_slice(&nonce);

    // // println!("encrypted instructions.len(): {}", encrypted_instructions.len());
    match connection.stream.write(&encrypted_data_block) {
        Ok(n) => println!("Wrote request as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e.kind()));},
    };
    connection.stream.flush()?;
    
    let mut buffer: [u8;2] = [0;2];
    // println!("Waiting for response from server");
    connection.stream.read_exact(&mut buffer)?;
    // println!("INSTRUCTION_BUFFER: {:x?}", buffer);
    // println!("About to parse response from server");
    let response = bytes_to_str(&buffer)?;
    println!("reponse: {}", response);
    // println!("response: {}", response);

    Ok(response.to_owned())
    
}

/// Helper function that parses a response from instruction_send_and_confirm().
#[inline]
pub fn parse_response(response: &str, username: &str, table_name: &str) -> Result<(), ServerError> {

    if response == "OK" {
        Ok(())
    } else if response == "IU" {
        Err(ServerError::ParseResponse(format!("Username: {}, is invalid", username)))
    } else if response == "IP" {
        Err(ServerError::ParseResponse("Password is invalid".to_owned()))
    } else if response == ("NT") {
        Err(ServerError::ParseResponse(format!("No such table as {}", table_name)))
    } else {
        panic!("Need to handle error: {}", response);
    }

}

/// This is the function primarily responsible for transmitting data.
/// It compresses, encrypts, sends, and confirms receipt of the data.
/// Used by both client and server.
pub fn data_send_and_confirm(connection: &mut Connection, data: &[u8]) -> Result<String, ServerError> {

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
pub fn receive_data(connection: &mut Connection) -> Result<Vec<u8>, ServerError> {
    
    let mut size_buffer: [u8; 8] = [0; 8];
    connection.stream.read_exact(&mut size_buffer)?;
    println!("HERE 4!!!");

    let data_len = usize::from_le_bytes(size_buffer);
    if data_len > MAX_DATA_LEN {
        return Err(ServerError::OversizedData)
    }
    
    let mut data = Vec::with_capacity(data_len);
    let mut buffer = [0; DATA_BUFFER];
    let mut total_read: usize = 0;
    
    while total_read < data_len {
        let to_read = std::cmp::min(DATA_BUFFER, data_len - total_read);
        let bytes_received = connection.stream.read(&mut buffer[..to_read])?;
        if bytes_received == 0 {
            return Err(ServerError::Confirmation("Read failure".to_owned()));
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
        let data = [3, 1, 6, 1, 5, 8, 1, 8, 10, 11];
        let stdev = stdev_i32_slice(&data);
        assert!(stdev > 3.611 && stdev < 3.612);
    }

}
