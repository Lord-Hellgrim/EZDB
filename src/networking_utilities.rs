use std::ops::Mul;
use std::str::{self, Utf8Error};
use std::usize;


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


#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_bytes_to_str() {
        let bytes = [0,0,0,0,0,75,75,75,0,0,0,0,0];
        println!("{:?}", bytes_to_str(&bytes).unwrap().as_bytes());
    }


}


