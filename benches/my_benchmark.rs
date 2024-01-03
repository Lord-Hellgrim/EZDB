use std::io::Write;

use EZDB::client_networking;
use criterion::{criterion_group, criterion_main, Criterion};
use EZDB::db_structure::*;
use EZDB::networking_utilities::*;
use EZDB::client_networking::*;
use rand::Rng;

fn my_benchmark(c: &mut Criterion) {

    // count char
    let mut i = 0;
    let mut bench_csv = String::from("vnr,text-p;heiti,text;magn,int;lengd,float\n");
    loop {
        if i > 1_000_000 {
            break;
        }
        let random_number: i64 = rand::thread_rng().gen();
        let random_float: f64 = rand::thread_rng().gen();
        let random_key: u32 = rand::thread_rng().gen();
        let mut random_string = String::new();
        for _ in 0..8 {
            random_string.push(rand::thread_rng().gen_range(97..122) as u8 as char);
        }
        bench_csv.push_str(&format!("a{random_key};{random_string};{random_number};{random_float}\n"));
        
        i+= 1;
    }


    // // concurrent connections
    // c.bench_function("concurrent downloads (small csv)", |b| b.iter( || test_concurrent_connections()));
    let mut group = c.benchmark_group("smaller sample");
    group.bench_function("ColumnTable::from_csv_string, 1.000.000 random lines", |b| b.iter(|| ColumnTable::from_csv_string(&bench_csv, "bench_test", "criterion")));
    // group.bench_function("StrictTable::from_csv_string, 1.000.000 random lines", |b| b.iter(|| StrictTable::from_csv_string(&bench_csv, "bench_test")));
    
    // let address = "127.0.0.1:3004";
    // let username = "admin";
    // let password = "admin";
    // group.bench_function("upload_table", |b| b.iter(|| upload_table(address, username, password, "large_csv", &bench_csv)));

    // StrictTable::from_csv_string
    // group.sample_size(10);
    // group.bench_function("StrictTable::from_csv_string, big", |b| b.iter(|| StrictTable::from_csv_string(&big_csv, "good_csv")));
    // group.sample_size(100);
    // group.bench_function("StrictTable::from_csv_string, small", |b| b.iter(|| StrictTable::from_csv_string(&good_csv, "good_csv")));
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);