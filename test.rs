fn main() {
    #[cfg(target_feature="avx2")]
    fn p() {
        println!("AVX2");
    }

    #[cfg(not(target_feature="avx2"))]
    fn p() {
        println!("not avx2");
    }

    unsafe { p() };
}