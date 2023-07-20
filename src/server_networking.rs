use std::io::{Write, Read};
use std::net::TcpListener;
use std::error::Error;

pub fn server() -> Result<(), Box<dyn Error>> {
    let l = TcpListener::bind("127.0.0.1:3004")?;

    for stream in l.incoming() {
        println!("Accepted connection");
        std::thread::spawn(|| {
            println!("Spawned thread");
            let mut stream = match stream {
                Ok(value) => {println!("Unwrapped Result"); value},
                Err(e) => panic!("{}", e),
            };

            let mut instructions: [u8;15] = [0;15];
            println!("Initialized string buffer");
            loop {
                match stream.read(&mut instructions) {
                    Ok(n) => {
                        println!("Read {n} bytes");
                        break;
                    },
                    Err(e) => panic!("{e}"),
                };
            }
            
            let mut instruction_string = "".to_owned();
            for byte in instructions {
                if byte == 0 {
                    break;
                }
                instruction_string.push(char::from(byte));
            }
            dbg!(instruction_string.as_bytes());
            println!("{}", &instruction_string);

            if &instruction_string == "give me five!" {
                println!("matching...");
                match stream.write("FIVE!".as_bytes()) {
                    Ok(n) => println!("Wrote {n} bytes"),
                    Err(e) => panic!("{e}"),
                };
            }

        });
        continue;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_listener() {
        server();
    }
}