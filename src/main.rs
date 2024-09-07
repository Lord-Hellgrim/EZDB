//#![allow(unused)]
//#![allow(non_snake_case)]


use EZDB::server_networking;
use EZDB::utilities;

fn main() -> Result<(), utilities::EzError> {
    println!("calling: main()");


    let args = std::env::args();

    for arg in args {
        println!("{}", arg);
    }

    // This stuff is for debugging purposes around simd
    #[cfg(target_feature="avx2")]
    unsafe fn p() {
        println!("AVX2");
    }

    // This stuff is for debugging purposes around simd
    #[cfg(not(target_feature="avx2"))]
    fn p() {
        println!("not avx2");
    }

    // This stuff is for debugging purposes around simd
    unsafe { p() };
    
    server_networking::run_server("127.0.0.1:3004")?;

    Ok(())
}
