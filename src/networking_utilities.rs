use std::arch::asm;
use std::io::{Write, Read};
use std::net::TcpStream;
use std::num::ParseIntError;
use std::str::{self, Utf8Error};
use std::time::Duration;
use std::{usize, fmt};

use aes_gcm::aead;
use num_bigint::BigUint;

use crate::aes_temp_crypto::{encrypt_aes256, decrypt_aes256};
use crate::auth::AuthenticationError;
use crate::db_structure::StrictError;
use crate::diffie_hellman::*;


pub const INSTRUCTION_BUFFER: usize = 1024;
pub const DATA_BUFFER: usize = 1_000_000;
pub const INSTRUCTION_LENGTH: usize = 4;
pub const MAX_DATA_LEN: usize = u32::MAX as usize;



#[derive(Debug)]
pub enum ServerError {
    Utf8(Utf8Error),
    Io(std::io::Error),
    Instruction(InstructionError),
    Confirmation(String),
    Authentication(AuthenticationError),
    Strict(StrictError),
    Crypto(aead::Error),
    ParseInt(ParseIntError),
    OversizedData,
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
            ServerError::OversizedData => write!(f, "Sent data is too long. Maximum data size is {MAX_DATA_LEN}"),
        }
    }
}

impl From<std::io::Error> for ServerError {
    fn from(e: std::io::Error) -> Self {
        ServerError::Io(e)
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

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Instruction {
    Upload(String),
    Download(String),
    Update(String),
    Query(String /* table_name */, String /* query */),
    NewUser(String),
    KvUpload(String),
    KvUpdate(String),
    KvDownload(String),
}

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
            InstructionError::InvalidTable(s) => write!(f, "No such table as: {}", s),
        }
    }
}

impl From<Utf8Error> for InstructionError {
    fn from(e: Utf8Error) -> Self {
        InstructionError::Utf8(e)
    }
}


pub struct Connection {
    pub stream: TcpStream,
    pub user: String,
    pub aes_key: Vec<u8>,   
}

impl Connection {
    pub fn connect(address: &str, username: &str, password: &str) -> Result<Connection, ServerError> {

        if username.len() > 512 || password.len() > 512 {
            return Err(ServerError::Authentication(AuthenticationError::TooLong))
        }

        let client_dh = DiffieHellman::new();

        let mut stream = TcpStream::connect(address)?;
        let mut key_buffer: [u8; 256] = [0u8;256];
        stream.read(&mut key_buffer)?;
        let server_public_key = BigUint::from_bytes_le(&key_buffer);
        let client_public_key = client_dh.public_key().to_bytes_le();
        stream.write(&client_public_key)?;
        let shared_secret = client_dh.shared_secret(&server_public_key);
        let aes_key = blake3_hash(&shared_secret.to_bytes_le());

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



pub fn get_current_time() -> u64 {

    let x = std::time::SystemTime::now()
        .duration_since(std::time::SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    x

}

#[inline(always)]
pub fn rdtsc() -> u64 {
    let lo: u32;
    let hi: u32;
    unsafe {
        asm!("rdtsc", out("eax") lo, out("edx") hi, options(nostack, preserves_flags));
    }
    ((hi as u64) << 32) | (lo as u64)
}

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





//Removes the trailing 0 bytes from a str created from a byte buffer
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


pub fn bytes_to_usize(bytes: [u8; 8]) -> usize {
    let mut value: usize = 0;

    for &byte in bytes.iter() {
        value = (value << 8) | (byte as usize);
    }

    value
}

pub fn encode_hex(bytes: &[u8]) -> String {
    let mut s = String::new();
    for &b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

pub fn decode_hex(s: &str) -> Result<Vec<u8>, ParseIntError> {
    // println!("s.len(): {}", s.len());
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}


pub fn hash_function(a: &str) -> Vec<u8> {
    blake3::hash(a.as_bytes()).as_bytes().to_vec()
}









pub fn instruction_send_and_confirm(instruction: Instruction, connection: &mut Connection) -> Result<String, ServerError> {

    let instruction = match instruction {
        Instruction::Download(table_name) => format!("Downloading|{}|blank|{}", table_name, connection.user),
        Instruction::Upload(table_name) => format!("Uploading|{}|blank|{}", table_name, connection.user),
        Instruction::Update(table_name) => format!("Updating|{}|blank|{}", table_name, connection.user),
        Instruction::Query(table_name, query) => format!("Querying|{}|{}|{}", table_name, query, connection.user),
        Instruction::NewUser(user_string) => format!("NewUser|{}|blank|{}", user_string, connection.user),
        Instruction::KvUpload(table_name) => format!("KvUpload|{}|blank|{}", table_name, connection.user),
        Instruction::KvUpdate(table_name) => format!("KvUpdate|{}|blank|{}", table_name, connection.user),
        Instruction::KvDownload(table_name) => format!("KvDownload|{}|blank|{}", table_name, connection.user),
    };

    let instruction_string = format!("{instruction}");
    let (encrypted_instructions, nonce) = encrypt_aes256(&instruction_string.as_bytes(), &connection.aes_key);

    // // println!("encrypted instructions: {:x?}", encrypted_instructions);
    // // println!("nonce: {:x?}", nonce);

    let mut encrypted_data_block = Vec::with_capacity(encrypted_instructions.len() + 28);
    encrypted_data_block.extend_from_slice(&encrypted_instructions);
    encrypted_data_block.extend_from_slice(&nonce);

    // // println!("encrypted instructions.len(): {}", encrypted_instructions.len());
    match connection.stream.write(&encrypted_data_block) {
        Ok(n) => println!("Wrote request as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };
    
    let mut buffer: [u8;INSTRUCTION_BUFFER] = [0;INSTRUCTION_BUFFER];
    // println!("Waiting for response from server");
    connection.stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    connection.stream.read(&mut buffer)?;

    // println!("About to parse response from server");
    let response = bytes_to_str(&buffer)?;
    // println!("response: {}", response);

    Ok(response.to_owned())
    
}


pub fn parse_response(response: &str, username: &str, password: &[u8], table_name: &str) -> Result<(), ServerError> {

    if response == "OK" {
        return Ok(())
    } else if response == "Username is incorrect" {
        return Err(ServerError::Authentication(AuthenticationError::WrongUser(username.to_owned())));
    } else if response == "Password is incorrect" {
        return Err(ServerError::Authentication(AuthenticationError::WrongPassword(password.to_owned())));
    } else if response.starts_with("No such table as:") {
        return Err(ServerError::Instruction(InstructionError::InvalidTable(format!("No such table as {}", table_name))));
    } else {
        panic!("Need to handle error: {}", response);
    }

}


pub fn data_send_and_confirm(connection: &mut Connection, data: &[u8]) -> Result<String, ServerError> {

    // // println!("data: {:x?}", data);

    let (encrypted_data, data_nonce) = encrypt_aes256(data, &connection.aes_key);
    
    let mut encrypted_data_block = Vec::with_capacity(data.len() + 28);
    encrypted_data_block.extend_from_slice(&encrypted_data);
    encrypted_data_block.extend_from_slice(&data_nonce);
    
    
    // println!("Sending data...");
    // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
    // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
    connection.stream.write_all(&(data.len() + 28).to_le_bytes())?;
    connection.stream.write_all(&encrypted_data_block)?;
    connection.stream.flush()?;
    // println!("data sent");
    // println!("Waiting for confirmation from client");
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


pub fn receive_data(connection: &mut Connection) -> Result<(Vec<u8>, usize), ServerError> {

    // println!("Allocating size and data buffer");
    
    let mut size_buffer: [u8; 8] = [0; 8];
    connection.stream.read_exact(&mut size_buffer)?;

    let data_len = usize::from_le_bytes(size_buffer);
    if data_len > MAX_DATA_LEN {
        return Err(ServerError::OversizedData)
    }
    
    // println!("Expected data length: {}", data_len);
    
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
    
    // println!("Successfully read {} bytes", total_read);
    // // println!("Data: {:x?}", data);
    
    let (ciphertext, nonce) = (&data[0..data.len()-12], &data[data.len()-12..]);
    // println!("About to decrypt");
    // let instant = std::time::Instant::now();

    let csv = decrypt_aes256(&ciphertext, &connection.aes_key, &nonce)?;
    // let elapsed = instant.elapsed().as_millis();
    // println!("Finished decrypting in: {} milliseconds", elapsed);

    Ok((csv, total_read))
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


}
