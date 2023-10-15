use std::arch::asm;
use std::char::MAX;
use std::io::{Write, Read};
use std::net::{TcpStream, IpAddr};
use std::num::ParseIntError;
use std::str::{self, Utf8Error, FromStr};
use std::time::Duration;
use std::{usize, fmt};

use aes_gcm::{Aes256Gcm, AeadCore, aead};
use aes_gcm::aead::OsRng;
use rug::{Integer, Complete};
use rug::integer::Order;

use crate::aes_temp_crypto::{encrypt_aes256, decrypt_aes256};
use crate::auth::AuthenticationError;
use crate::db_structure::StrictError;
use crate::diffie_hellman::*;


pub const INSTRUCTION_BUFFER: usize = 1024;
pub const DATA_BUFFER: usize = 1_000_000;
pub const INSTRUCTION_LENGTH: usize = 5;
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
    pub peer: String,
    pub aes_key: Vec<u8>,   
}

impl Connection {
    pub fn connect(address: &str) -> Result<Connection, ServerError> {

        let client_dh = DiffieHellman::new();

        let mut stream = TcpStream::connect(address)?;
        let mut key_buffer: [u8; 256] = [0u8;256];
        stream.read(&mut key_buffer)?;
        let server_public_key = Integer::from_digits(&key_buffer, Order::Lsf);
        let client_public_key = client_dh.public_key().to_digits::<u8>(Order::Lsf);
        stream.write(&client_public_key)?;
        let shared_secret = client_dh.shared_secret(&server_public_key);
        let aes_key = aes256key(&shared_secret.to_digits::<u8>(Order::Lsf));
        Ok(
            Connection {
                stream: stream,
                peer: String::from(address),
                aes_key: aes_key,
            }
        )

    }
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


#[cfg(any(target_feature="sse", target_feature="avx", target_feature="avx2"))]
pub fn fast_split<'a>(s: &'a str, c: u8) -> Vec<&'a str> {
    use std::arch::x86_64::{_mm_loadu_si128, __m128i, _mm_set1_epi8, _mm_cmpeq_epi8, _mm_movemask_epi8,};

    let s = s.as_bytes();

    const MULTIPLY_DEBRUIJN_BIT_POSITION: [u8; 32] = [
    0, 1, 28, 2, 29, 14, 24, 3, 30, 22, 20, 15, 25, 17, 4, 8,
    31, 27, 13, 23, 21, 19, 16, 7, 26, 12, 18, 6, 11, 5, 10, 9
    ];

    let target = unsafe { _mm_set1_epi8(c as i8) };
    
    let mut i = 0;
    let mut slices = Vec::with_capacity(count_char(s, c)+10);
    let mut start = 0;
    let mut end = 0;

    if s.len() > 16 {
        while i < s.len()/16 {
            let block = unsafe {_mm_loadu_si128(s[i..i+16].as_ptr() as *const __m128i) };
            let cmp = unsafe { _mm_cmpeq_epi8(block, target) };
            let mut result = unsafe { _mm_movemask_epi8(cmp) };
            while result != 0 {
                end = i + MULTIPLY_DEBRUIJN_BIT_POSITION[(((result & result.wrapping_neg()) as u32).wrapping_mul(0x077CB531) >> 27) as usize] as usize;

                slices.push(str::from_utf8(&s[start..end]).expect("Should return utf8 since it's a slice of a utf8 str"));
                start = end + 1;
                result &= result - 1;
            }
            i += 16;
        }
    }

    while i < s.len() {
        if s[i] == c {
            slices.push(str::from_utf8(&s[start..i]).expect("Should return utf8 since it's a slice of a utf8 str"));
            start = i + 1;
        }
        
        i += 1;
    }

    slices.push(str::from_utf8(&s[start..]).expect("Should return utf8 since it's a slice of a utf8 str"));

    slices
}


#[cfg(any(target_feature="sse", target_feature="avx", target_feature="avx2"))]
pub fn count_char(s: &[u8], c: u8) -> usize {
    use std::arch::x86_64::{_mm_loadu_si128, __m128i, _mm_set1_epi8, _mm_cmpeq_epi8, _mm_movemask_epi8,};

    let target = unsafe { _mm_set1_epi8(c as i8) };
    
    let mut i = 0;
    let mut count = 0;

    if s.len() > 16 {
        while i < s.len()/16 {
            let block = unsafe {_mm_loadu_si128(s[i..i+16].as_ptr() as *const __m128i) };
            let cmp = unsafe { _mm_cmpeq_epi8(block, target) };
            let result = unsafe { _mm_movemask_epi8(cmp) };
            count += result.count_ones();
            i += 16;
        }
    }

    while i < s.len() {
        if s[i] == c {
            count += 1;
        }
        
        i += 1;
    }

    count as usize
}

#[cfg(not(target_feature="sse"))]
pub fn count_char(s: &[u8], c: u8) -> usize {

    let mut i = 0;
    let mut count = 0;
    while i < s.len() {
        if s[i] == c {
            count += 1;
        }
        
        i += 1;
    }
    count
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
    println!("s.len(): {}", s.len());
    (0..s.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&s[i..i + 2], 16))
        .collect()
}


pub fn hash_function(a: &str) -> Vec<u8> {
    blake3::hash(a.as_bytes()).as_bytes().to_vec()
}




pub fn instruction_send_and_confirm(username: &str, password: &str, instruction: Instruction, connection: &mut Connection) -> Result<String, ServerError> {

    let instruction = match instruction {
        Instruction::Download(table_name) => format!("Downloading|{}|blank", table_name),
        Instruction::Upload(table_name) => format!("Uploading|{}|blank", table_name),
        Instruction::Update(table_name) => format!("Updating|{}|blank", table_name),
        Instruction::Query(table_name, query) => format!("Querying|{}|{}", table_name, query),
    };

    let instruction_string = format!("{username}|{password}|{instruction}");
    let (encrypted_instructions, nonce) = encrypt_aes256(&instruction_string, &connection.aes_key);

    let mut encrypted_instructions = encode_hex(&encrypted_instructions);
    encrypted_instructions.push('|');
    encrypted_instructions.push_str(&encode_hex(&nonce));

    match connection.stream.write(&encrypted_instructions.as_bytes()) {
        Ok(n) => println!("Wrote request as {n} bytes"),
        Err(e) => {return Err(ServerError::Io(e));},
    };
    
    let mut buffer: [u8;INSTRUCTION_BUFFER] = [0;INSTRUCTION_BUFFER];
    println!("Waiting for response from server");
    connection.stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    connection.stream.read(&mut buffer)?;

    let response = bytes_to_str(&buffer)?;

    Ok(response.to_owned())
    
}


pub fn parse_response(response: &str, username: &str, password: &str, table_name: &str) -> Result<(), ServerError> {

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


pub fn data_send_and_confirm(connection: &mut Connection, data: &str) -> Result<String, ServerError> {

    let (encrypted_data, data_nonce) = encrypt_aes256(data, &connection.aes_key);
    
    let mut encrypted_data_block = Vec::with_capacity(data.len() + 28);
    encrypted_data_block.extend_from_slice(&encrypted_data);
    encrypted_data_block.extend_from_slice(&data_nonce);
    
    
    println!("Sending data...");
    // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
    // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
    connection.stream.write_all(&(data.len() + 28).to_le_bytes())?;
    connection.stream.write_all(&encrypted_data_block)?;
    connection.stream.flush()?;
    println!("data sent");
    println!("Waiting for confirmation from client");
    connection.stream.set_read_timeout(Some(Duration::from_secs(15)))?;
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


pub fn receive_data(connection: &mut Connection) -> Result<(String, usize), ServerError> {

    println!("Allocating size and data buffer");
    
    let mut size_buffer: [u8; 8] = [0; 8];
    connection.stream.read_exact(&mut size_buffer)?;

    let data_len = usize::from_le_bytes(size_buffer);
    if data_len > MAX_DATA_LEN {
        return Err(ServerError::OversizedData)
    }
    
    println!("Expected data length: {}", data_len);
    
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
    }
    
    println!("Successfully read {} bytes", total_read);
    
    let (ciphertext, nonce) = (&data[0..data.len()-12], &data[data.len()-12..]);
    println!("About to decrypt");
    let instant = std::time::Instant::now();

    let csv = decrypt_aes256(&ciphertext, &connection.aes_key, &nonce)?;
    let csv = bytes_to_str(&csv)?;
    let elapsed = instant.elapsed().as_millis();
    println!("Finished decrypting in: {} milliseconds", elapsed);

    Ok((csv.to_owned(), total_read))
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_char() {
        let mut i = 0;
        let mut printer = String::from("vnr;heiti;magn\n");
        loop {
            if i > 1_000_000 {
                break;
            }
            printer.push_str(&format!("i{};product name;569\n", i));
            i+= 1;
        }

        let split = count_char(&printer.as_bytes(), "\n".as_bytes()[0]);
        println!("char_count: {}", split);
    }


    #[test]
    fn test_fast_split() {
        let mut i = 0;
        let mut printer = String::from("vnr;heiti;magn\n");
        loop {
            if i > 1_000_000 {
                break;
            }
            printer.push_str(&format!("i{};product name;569\n", i));
            i+= 1;
        }
        
        let split = fast_split(&printer, "\n".as_bytes()[0]);
        
        println!("fast_split: split.len(): {}", split.len());
        println!("split.len(): {}", printer.split('\n').collect::<Vec<&str>>().len());
        assert_eq!(split.len(), printer.split('\n').collect::<Vec<&str>>().len());

    }

    #[test]
    fn test_bytes_to_str() {
        let bytes = [0,0,0,0,0,75,75,75,0,0,0,0,0];
        println!("{:?}", bytes_to_str(&bytes).unwrap().as_bytes());
    }


}


