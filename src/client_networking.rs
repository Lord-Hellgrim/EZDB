use std::{net::{TcpListener, TcpStream}, io::Read};

pub fn client() {
    let mut x = TcpStream::connect("127.0.0.1:3004").unwrap();
    let mut s = String::from("");
    match x.read_to_string(&mut s) {
        Ok(n) => println!("Read {} bytes", n),
        Err(_) => panic!(),
    };
    println!("{}", s.to_string());
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_Listener() {
        client();
    }
}