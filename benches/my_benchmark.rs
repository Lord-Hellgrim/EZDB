use std::io::Write;

use criterion::{criterion_group, criterion_main, Criterion};
use EZDB::db_structure::*;
use EZDB::networking_utilities::*;

fn my_benchmark(c: &mut Criterion) {

    // count char
    let mut i = 0;
    let mut printer = String::from("vnr;heiti;magn\n");
    loop {
        if i > 1_000_000 {
            break;
        }
        printer.push_str(&format!("i{};product name;569\n", i));
        i+= 1;
    }
    let mut file = std::fs::File::create("large.csv").unwrap();
    file.write_all(printer.as_bytes()).unwrap();

    let big_csv = std::fs::read_to_string("large.csv").unwrap();
    c.bench_function("count_char", |b| b.iter(|| count_char(big_csv.as_bytes(), 59u8)));

    let good_csv = std::fs::read_to_string("good_csv.txt").unwrap();
    c.bench_function("count_char small", |b| b.iter( || count_char(good_csv.as_bytes(), 59u8)));

    // Fast_ split
    let big_csv = std::fs::read_to_string("large.csv").unwrap();
    c.bench_function("fast_split", |b| b.iter(|| fast_split(&big_csv, "\n".as_bytes()[0])));
    
    let good_csv = std::fs::read_to_string("good_csv.txt").unwrap();
    c.bench_function("fast_split small", |b| b.iter( || fast_split(&good_csv, "\n".as_bytes()[0])));
    
    // StrictTable::from_csv_string
    let mut group = c.benchmark_group("smaller sample");
    group.sample_size(10);
    group.bench_function("StrictTable::from_csv_string, big", |b| b.iter(|| StrictTable::from_csv_string(&big_csv, "good_csv")));
    group.sample_size(100);
    group.bench_function("StrictTable::from_csv_string, small", |b| b.iter(|| StrictTable::from_csv_string(&good_csv, "good_csv")));
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);