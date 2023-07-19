use std::{net::{TcpListener, TcpStream}, io::Read};

pub fn client() {
    let mut x = TcpStream::connect("127.0.0.1:3004").unwrap();
    let mut s = String::from("");
    match x.read_to_string(&mut s) {
        Ok(n) => {
            println!("Read {} bytes", n);
            println!("spacer\n\n");    
        },
        Err(_) => panic!(),
    };
    println!("{}", s);
}

pub fn client_bits() {
    let mut x = TcpStream::connect("127.0.0.1:3004").unwrap();
    let mut s: [u8;1000] = [0;1000];
    loop {
        match x.read(&mut s) {
            Ok(n) => {
                if n == 0 {
                    println!("end of file");
                    break;
                }
                println!("Read {} bytes", n);
                let mut output = String::from("");
                for byte in s {
                    output.push(char::from(byte));
                }
                println!("{}", output);
            },
            Err(_) => break,
        };
    }
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_Listener() {
        client_bits();
    }
}