use std::net::TcpStream;



pub fn check_if_http_request(stream: &TcpStream) -> bool {
    #[cfg(debug_assertions)]
    println!("calling: check_if_http_request()");

    false

    // let mut buffer = [0u8;1024];
    // stream.peek(&mut buffer)?;

    // let text = bytes_to_str(&buffer)?;
    // if text.starts_with("POST /query HTTP/1.1") {
    //     Ok(extract_query(text).to_owned())
    // } else {
    //     Err(EzError::Query("Not http. Proceed with normal".to_owned()))
    // }
}


pub fn extract_query(request: &str) -> &str {
    #[cfg(debug_assertions)]
    println!("calling: extract_query()");

    if let Some(pos) = request.find("\r\n\r\n") {
        return &request[pos + 4..];
    }
    ""
}

pub fn handle_http_connection() {
    
}