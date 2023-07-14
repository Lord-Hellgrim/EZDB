use std::io::Write;
use std::net::{self, TcpStream, TcpListener};
use std::error::Error;

pub fn listener() -> Result<(), Box<dyn Error>> {
    let l = TcpListener::bind("127.0.0.1:3004")?;

    for stream in l.incoming() {
        let mut x = match stream {
            Ok(value) => value,
            Err(_) => panic!(),
        };
        let s = "Hello";
        x.write(s.as_bytes()).unwrap();
        continue;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_Listener() {
        listener();
    }
}