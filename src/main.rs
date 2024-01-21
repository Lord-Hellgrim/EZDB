//#![allow(unused)]
#![allow(non_snake_case)]


use EZDB::server_networking;
use EZDB::networking_utilities;

fn main() -> Result<(), networking_utilities::ServerError> {

    #[cfg(target_feature="avx2")]
    unsafe fn p() {
        println!("AVX2");
    }

    #[cfg(not(target_feature="avx2"))]
    fn p() {
        println!("not avx2");
    }

    unsafe { p() };
    let server = server_networking::Server::init("127.0.0.1:3004").unwrap();

    server_networking::run_server(server)?;

    Ok(())
}
