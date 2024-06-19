
use criterion::{criterion_group, criterion_main, Criterion};
use EZDB::db_structure::*;
use EZDB::ezql::*;
use EZDB::ezql::parse_EZQL;

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
    let mut group = c.benchmark_group("All benchmarks");
    // group.bench_function("StrictTable::from_csv_string, 1.000.000 random lines", |b| b.iter(|| StrictTable::from_csv_string(&bench_csv, "bench_test")));
    
    // let address = "127.0.0.1:3004";
    // let username = "admin";
    // let password = "admin";
    // group.bench_function("upload_table", |b| b.iter(|| upload_table(address, username, password, "large_csv", &bench_csv)));
    
    let google_docs_csv = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
    
    let t = EZTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    
    group.bench_function("Write to raw binary", |b| b.iter(|| t.write_to_raw_binary()));
    group.bench_function("Write to csv string", |b| b.iter(|| t.to_string()));

    let bint_t = t.write_to_raw_binary();
    let string_t = t.to_string();
    
    group.bench_function("read from raw binary", |b| b.iter(|| EZTable::read_raw_binary("bench", &bint_t)));
    group.bench_function("read from csv string", |b| b.iter(|| EZTable::from_csv_string(&string_t, "bench2", "criterion")));

    // header:
    // vnr,i-P;heiti,t-N;magn,i-N;lager,t-N

    // - INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
    // - SELECT(table_name: products, primary_keys: *, columns: (price, stock), conditions: ((price greater-than 500) AND (stock less-than 1000)))
    // - UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))
    // - DELETE(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))
    // - SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))
    // - LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)
    let SELECT_query = "SELECT(table_name: test, primary_keys: *, columns: (heiti, magn, lager), conditions: ((magn greater_than 30) AND (lager equals lag15)))";
    let INSERT_query = "INSERT(table_name: test, value_columns: (vnr, heiti, magn, lager), new_values: ( (175, HAMMAR, 52, lag15), (173, HAMMAR, 51, lag20) ))";
    let UPDATE_query = "UPDATE(table_name: test, primary_keys: *, conditions: ((magn greater_than 30) AND (lager equals lag15)), updates: ( (magn -= 30) ))";
    let DELETE_query = "DELETE(table_name: test, primary_keys: *, conditions: ((lager equals lag30)))";
    let SUM_query = "SUMMARY(table_name: test, columns: ( (SUM magn) ))";
    let MEAN_query = "SUMMARY(table_name: test, columns: ( (MEAN magn) ))";
    let MODE_query = "SUMMARY(table_name: test, columns: ( (MODE magn) ))";
    let STDEV_query = "SUMMARY(table_name: test, columns: ( (STDEV magn) ))";
    let MEDIAN_query = "SUMMARY(table_name: test, columns: ( (MEDIAN magn) ))";
    let LEFT_JOIN_query = "LEFT_JOIN(left_table: test, right_table: insert_me, match_columns: (vnr, vnr), primary_keys: *)";

    group.bench_function("parse SELECT query", |b| b.iter(|| parse_EZQL(&SELECT_query)));
    group.bench_function("parse INSERT query", |b| b.iter(|| parse_EZQL(&INSERT_query)));
    group.bench_function("parse UPDATE query", |b| b.iter(|| parse_EZQL(&UPDATE_query)));
    group.bench_function("parse DELETE query", |b| b.iter(|| parse_EZQL(&DELETE_query)));
    group.bench_function("parse SUM query"   , |b| b.iter(|| parse_EZQL(&SUM_query)));
    group.bench_function("parse MEAN query"  , |b| b.iter(|| parse_EZQL(&MEAN_query)));
    group.bench_function("parse MODE query"  , |b| b.iter(|| parse_EZQL(&MODE_query)));
    group.bench_function("parse STDEV query" , |b| b.iter(|| parse_EZQL(&STDEV_query)));
    group.bench_function("parse MEDIAN query", |b| b.iter(|| parse_EZQL(&MEDIAN_query)));
    group.bench_function("parse LEFT_JOIN query", |b| b.iter(|| parse_EZQL(&LEFT_JOIN_query)));

    let parsed_SELECT_query = parse_EZQL(&SELECT_query).unwrap();
    let parsed_INSERT_query =     parse_EZQL(&INSERT_query).unwrap();
    let parsed_UPDATE_query =     parse_EZQL(&UPDATE_query).unwrap();
    let parsed_DELETE_query =     parse_EZQL(&DELETE_query).unwrap();
    let parsed_SUM_query =     parse_EZQL(&SUM_query).unwrap();
    let parsed_MEAN_query =    parse_EZQL(&MEAN_query).unwrap();
    let parsed_MODE_query =    parse_EZQL(&MODE_query).unwrap();
    let parsed_STDEV_query =      parse_EZQL(&STDEV_query).unwrap();
    let parsed_MEDIAN_query =     parse_EZQL(&MEDIAN_query).unwrap();
    let parsed_LEFT_JOIN_query =     parse_EZQL(&LEFT_JOIN_query).unwrap();

    let mut t = EZTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    group.bench_function("execute INSERT query", |b| b.iter(|| {
        execute_insert_query(parsed_INSERT_query.clone(), &mut t).unwrap();
        t = EZTable::read_raw_binary("test", &bint_t).unwrap();
    }));
    group.bench_function("execute UPDATE query", |b| b.iter(|| {
        execute_update_query(parsed_UPDATE_query.clone(), &mut t).unwrap();
        t = EZTable::read_raw_binary("test", &bint_t).unwrap();
    }));
    group.bench_function("execute DELETE query", |b| b.iter(|| {
        execute_delete_query(parsed_DELETE_query.clone(), &mut t).unwrap();
        t = EZTable::read_raw_binary("test", &bint_t).unwrap();
    }));
    group.bench_function("execute SUM query"   , |b| b.iter(|| {
        execute_summary_query(parsed_SUM_query.clone(), &mut t).unwrap();
        t = EZTable::read_raw_binary("test", &bint_t).unwrap();
    }));
    group.bench_function("execute SELECT query", |b| b.iter(|| execute_select_query(parsed_SELECT_query.clone(), &mut t).unwrap()));
    let mut t = EZTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    group.bench_function("execute MEAN query"  , |b| b.iter(|| execute_summary_query(parsed_MEAN_query.clone(), &mut t).unwrap()));
    let mut t = EZTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    group.bench_function("execute MODE query"  , |b| b.iter(|| execute_summary_query(parsed_MODE_query.clone(), &mut t).unwrap()));
    let mut t = EZTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    group.bench_function("execute STDEV query" , |b| b.iter(|| execute_summary_query(parsed_STDEV_query.clone(), &mut t).unwrap()));
    let mut t = EZTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    group.bench_function("execute MEDIAN query", |b| b.iter(|| execute_summary_query(parsed_MEDIAN_query.clone(), &mut t).unwrap()));
    let t = EZTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    let insert_me_string = std::fs::read_to_string(format!("test_files{PATH_SEP}insert_me.csv")).unwrap();
    let insert_me = EZTable::from_csv_string(&insert_me_string, "insert_me", "QUERY").unwrap();
    group.bench_function("execute LEFT_JOIN query", |b| b.iter(|| execute_left_join_query(parsed_LEFT_JOIN_query.clone(), &t, &insert_me).unwrap()));

}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);