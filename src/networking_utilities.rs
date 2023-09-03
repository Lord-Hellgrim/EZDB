use std::io::{Write, Read};
use std::net::TcpStream;
use std::str::{self, Utf8Error};
use std::time::Duration;
use std::{usize, fmt};

use crate::auth::AuthenticationError;
use crate::db_structure::StrictError;


pub const INSTRUCTION_BUFFER: usize = 1024;
pub const DATA_BUFFER: usize = 1_000_000;
pub const MIN_INSTRUCTION_LENGTH: usize = 4;
pub const MAX_INSTRUCTION_LENGTH: usize = 4;



#[derive(Debug)]
pub enum ServerError {
    Utf8(Utf8Error),
    Io(std::io::Error),
    Instruction(InstructionError),
    Confirmation(Vec<u8>),
    Authentication(AuthenticationError),
    Strict(StrictError),
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


pub enum Instruction {
    Upload(String),
    Download(String),
    Update(String),
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
            InstructionError::InvalidTable(s) => write!(f, "Table: {} does not exist.", s),
        }
    }
}

impl From<Utf8Error> for InstructionError {
    fn from(e: Utf8Error) -> Self {
        InstructionError::Utf8(e)
    }
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


pub fn hash_function(a: &str) -> &str{
    a
}


pub fn send_data(stream: &mut TcpStream, data: &str) -> Result<String, ServerError> {

    println!("Sending data size...");
    stream.write(&data.len().to_be_bytes())?;
    println!("Sending data...");
    stream.write(data.as_bytes())?;
    
    // Waiting for confirmation from client
    stream.set_read_timeout(Some(Duration::from_secs(5)))?;
    let mut buffer: [u8;INSTRUCTION_BUFFER] = [0;INSTRUCTION_BUFFER];
    match stream.read(&mut buffer) {
        Ok(_) => {
            println!("Confirmation '{}' received", bytes_to_str(&buffer)?);
        },
        Err(_) => println!("Did not confirm transmission with client"),
        
    }
    
    let confirmation = bytes_to_str(&buffer)?;

    Ok(confirmation.to_owned())

}


pub fn receive_data(stream: &mut TcpStream) -> Result<(String, usize), ServerError> {

    println!("Allocating csv buffer");
    let mut size_buffer: [u8;8] = [0;8];
    let mut buffer = [0;DATA_BUFFER];
    let mut total_read: usize = 0;
    stream.read(&mut size_buffer)?;
    let data_len = bytes_to_usize(size_buffer);
    println!("Expected data length: {}", data_len);
    let mut csv = String::new();
    loop {
        if total_read >= data_len {
         break
        }
       let bytes_received = stream.read(&mut buffer)?;
       csv.push_str(bytes_to_str(&buffer)?);
       buffer = [0;DATA_BUFFER];
       total_read += bytes_received;
       println!("Read {bytes_received} bytes. Total read {total_read}");
    }

    Ok((csv, total_read))
}



#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_bytes_to_str() {
        let bytes = [0,0,0,0,0,75,75,75,0,0,0,0,0];
        println!("{:?}", bytes_to_str(&bytes).unwrap().as_bytes());
    }


}


