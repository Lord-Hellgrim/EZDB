use std::io::Write;
use std::string;

use EZDB::client_networking;
use criterion::{criterion_group, criterion_main, Criterion};
use EZDB::compression::brotli_compress;
use EZDB::compression::brotli_decompress;
use EZDB::compression::miniz_compress;
use EZDB::compression::miniz_decompress;
use EZDB::db_structure::*;
use EZDB::networking_utilities::*;
use EZDB::client_networking::*;
use rand::Rng;

use EZDB::PATH_SEP;

fn my_benchmark(c: &mut Criterion) {

    // count char
    // let mut i = 0;
    // let mut bench_csv = String::from("vnr,text-p;heiti,text;magn,int;lengd,float\n");
    // loop {
    //     if i > 1_000_000 {
    //         break;
    //     }
    //     let random_number: i64 = rand::thread_rng().gen();
    //     let random_float: f64 = rand::thread_rng().gen();
    //     let random_key: u32 = rand::thread_rng().gen();
    //     let mut random_string = String::new();
    //     for _ in 0..8 {
    //         random_string.push(rand::thread_rng().gen_range(97..122) as u8 as char);
    //     }
    //     bench_csv.push_str(&format!("a{random_key};{random_string};{random_number};{random_float}\n"));
        
    //     i+= 1;
    // }


    // // concurrent connections
    // c.bench_function("concurrent downloads (small csv)", |b| b.iter( || test_concurrent_connections()));
    let mut group = c.benchmark_group("smaller sample");
    // group.bench_function("ColumnTable::from_csv_string, 1.000.000 random lines", |b| b.iter(|| ColumnTable::from_csv_string(&bench_csv, "bench_test", "criterion")));
    // group.bench_function("StrictTable::from_csv_string, 1.000.000 random lines", |b| b.iter(|| StrictTable::from_csv_string(&bench_csv, "bench_test")));
    
    // let address = "127.0.0.1:3004";
    // let username = "admin";
    // let password = "admin";
    // group.bench_function("upload_table", |b| b.iter(|| upload_table(address, username, password, "large_csv", &bench_csv)));

    let input = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
    let t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();
    let bint_t = t.write_to_raw_binary();
    let string_t = t.to_string();
    println!("bin_t lent: {}", bint_t.len());
    println!("string_t lent: {}", string_t.len());
    group.bench_function("binary", |b| b.iter(|| ColumnTable::read_raw_binary(&bint_t)));
    group.bench_function("csv", |b| b.iter(|| ColumnTable::from_csv_string(&string_t, "test", "test")));

    group.bench_function("compress string miniz_oxide", |b| b.iter(|| miniz_compress(string_t.as_bytes())));
    group.bench_function("compress string brotli", |b| b.iter(|| brotli_compress(string_t.as_bytes())));

    let comp_mo_string_t = miniz_compress(string_t.as_bytes()).unwrap();
    let comp_br_string_t = brotli_compress(string_t.as_bytes());

    group.bench_function("decompress string miniz_oxide", |b| b.iter(|| miniz_decompress(&comp_mo_string_t)));
    group.bench_function("decompress string brotli", |b| b.iter(|| brotli_decompress(&comp_br_string_t)));


    // StrictTable::from_csv_string
    // group.sample_size(10);
    // group.bench_function("StrictTable::from_csv_string, big", |b| b.iter(|| StrictTable::from_csv_string(&big_csv, "good_csv")));
    // group.sample_size(100);
    // group.bench_function("StrictTable::from_csv_string, small", |b| b.iter(|| StrictTable::from_csv_string(&good_csv, "good_csv")));
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);