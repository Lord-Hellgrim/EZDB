use std::str::{self, Utf8Error};


pub fn bytes_to_str(bytes: &[u8]) -> Result<&str, Utf8Error> {
    let mut counter: usize = 0;
    for byte in bytes {
        if byte.clone() == 0 {
            break
        }
        counter += 1;
    }

    str::from_utf8(&bytes[0..counter])

}