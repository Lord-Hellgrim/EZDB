use std::io::Write;
use std::net::TcpListener;
use std::error::Error;

pub fn server() -> Result<(), Box<dyn Error>> {
    let l = TcpListener::bind("127.0.0.1:8080")?;

    for stream in l.incoming() {
        
        std::thread::spawn(|| {
            let mut x = match stream {
                Ok(value) => value,
                Err(e) => panic!("{}", e),
            };
            let mut i = 0;
            while i < 10 {
                std::thread::sleep(std::time::Duration::from_secs(1));
                x.write(format!("String {}\n", i).as_bytes()).unwrap();
                i += 1;
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