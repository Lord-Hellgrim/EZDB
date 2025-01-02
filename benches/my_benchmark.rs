
use std::sync::Arc;
use std::sync::Mutex;

use criterion::{criterion_group, criterion_main, Criterion};
use rand::Rng;
// use EZDB::compression::brotli_compress;
// use EZDB::compression::brotli_decompress;
use EZDB::compression::miniz_compress;
use EZDB::compression::miniz_decompress;
use EZDB::db_structure::*;
use EZDB::ezql::*;

use EZDB::testing_tools::create_fixed_table;
use EZDB::utilities::*;
use EZDB::PATH_SEP;

use ezcbor::cbor::Cbor;

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

    // let query = Query::SELECT { 
    //     table_name: ksf("good_table"),
    //     primary_keys: RangeOrListOrAll::All,
    //     columns: vec![ksf("id"), ksf("name"), ksf("price")],
    //     conditions: vec![
    //         OpOrCond::Cond(Condition{attribute: ksf("id"), op: TestOp::Equals, value: DbValue::Int(4)}),
    //         OpOrCond::Op(Operator::AND),
    //         OpOrCond::Cond(Condition{attribute: ksf("name"), op: TestOp::Equals, value: DbValue::Text(ksf("four"))}),
            
    //     ],
    // };

    // group.bench_function("Query::to_binary()", |b| b.iter(|| query.to_binary()));
    // group.bench_function("Query::inline_to_binary()", |b| b.iter(|| query.to_binary()));
    
    // let address = "127.0.0.1:3004";
    // let username = "admin";
    // let password = "admin";
    // group.bench_function("upload_table", |b| b.iter(|| upload_table(address, username, password, "large_csv", &bench_csv)));
    
    // let google_docs_csv = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
    
    // let t = ColumnTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    
    // group.bench_function("Write to raw binary", |b| b.iter(|| t.write_to_binary()));
    // group.bench_function("Write to csv string", |b| b.iter(|| t.to_string()));

    // let bint_t = t.write_to_binary();
    // let string_t = t.to_string();
    
    // group.bench_function("read from raw binary", |b| b.iter(|| ColumnTable::from_binary("bench", &bint_t)));
    // group.bench_function("read from csv string", |b| b.iter(|| ColumnTable::from_csv_string(&string_t, "bench2", "criterion")));

    // // header:
    // // vnr,i-P;heiti,t-N;magn,i-N;lager,t-N

    // // - INSERT(table_name: products, value_columns: (id, stock, location, price), new_values: ((0113035, 500, LAG15, 995), (0113000, 100, LAG30, 495)))
    // // - SELECT(table_name: products, primary_keys: *, columns: (price, stock), conditions: ((price greater-than 500) AND (stock less-than 1000)))
    // // - UPDATE(table_name: products, primary_keys: (0113035, 0113000), conditions: ((id starts-with 011)), updates: ((price += 100), (stock -= 100)))
    // // - DELETE(primary_keys: *, table_name: products, conditions: ((price greater-than 500) AND (stock less-than 1000)))
    // // - SUMMARY(table_name: products, columns: ((SUM stock), (MEAN price)))
    // // - LEFT_JOIN(left_table: products, right_table: warehouses, match_columns: (location, id), primary_keys: 0113000..18572054)
    // let SELECT_query = "SELECT(table_name: test, primary_keys: *, columns: (heiti, magn, lager), conditions: ((magn greater_than 30) AND (lager equals lag15)))";
    // let INSERT_query = "INSERT(table_name: test, value_columns: (vnr, heiti, magn, lager), new_values: ( (175, HAMMAR, 52, lag15), (173, HAMMAR, 51, lag20) ))";
    // let UPDATE_query = "UPDATE(table_name: test, primary_keys: *, conditions: ((magn greater_than 30) AND (lager equals lag15)), updates: ( (magn -= 30) ))";
    // let DELETE_query = "DELETE(table_name: test, primary_keys: *, conditions: ((lager equals lag30)))";
    // let SUM_query = "SUMMARY(table_name: test, columns: ( (SUM magn) ))";
    // let MEAN_query = "SUMMARY(table_name: test, columns: ( (MEAN magn) ))";
    // let MODE_query = "SUMMARY(table_name: test, columns: ( (MODE magn) ))";
    // let STDEV_query = "SUMMARY(table_name: test, columns: ( (STDEV magn) ))";
    // let MEDIAN_query = "SUMMARY(table_name: test, columns: ( (MEDIAN magn) ))";
    // let LEFT_JOIN_query = "LEFT_JOIN(left_table: test, right_table: insert_me, match_columns: (vnr, vnr), primary_keys: *)";

    // group.bench_function("parse SELECT query", |b| b.iter(|| parse_EZQL(&SELECT_query)));
    // group.bench_function("parse INSERT query", |b| b.iter(|| parse_EZQL(&INSERT_query)));
    // group.bench_function("parse UPDATE query", |b| b.iter(|| parse_EZQL(&UPDATE_query)));
    // group.bench_function("parse DELETE query", |b| b.iter(|| parse_EZQL(&DELETE_query)));
    // group.bench_function("parse SUM query"   , |b| b.iter(|| parse_EZQL(&SUM_query)));
    // group.bench_function("parse MEAN query"  , |b| b.iter(|| parse_EZQL(&MEAN_query)));
    // group.bench_function("parse MODE query"  , |b| b.iter(|| parse_EZQL(&MODE_query)));
    // group.bench_function("parse STDEV query" , |b| b.iter(|| parse_EZQL(&STDEV_query)));
    // group.bench_function("parse MEDIAN query", |b| b.iter(|| parse_EZQL(&MEDIAN_query)));
    // group.bench_function("parse LEFT_JOIN query", |b| b.iter(|| parse_EZQL(&LEFT_JOIN_query)));

    // let parsed_SELECT_query = parse_EZQL(&SELECT_query).unwrap();
    // let parsed_INSERT_query =     parse_EZQL(&INSERT_query).unwrap();
    // let parsed_UPDATE_query =     parse_EZQL(&UPDATE_query).unwrap();
    // let parsed_DELETE_query =     parse_EZQL(&DELETE_query).unwrap();
    // let parsed_SUM_query =     parse_EZQL(&SUM_query).unwrap();
    // let parsed_MEAN_query =    parse_EZQL(&MEAN_query).unwrap();
    // let parsed_MODE_query =    parse_EZQL(&MODE_query).unwrap();
    // let parsed_STDEV_query =      parse_EZQL(&STDEV_query).unwrap();
    // let parsed_MEDIAN_query =     parse_EZQL(&MEDIAN_query).unwrap();
    // let parsed_LEFT_JOIN_query =     parse_EZQL(&LEFT_JOIN_query).unwrap();

    // let mut t = ColumnTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    // group.bench_function("execute INSERT query", |b| b.iter(|| {
    //     execute_insert_query(parsed_INSERT_query.clone(), &mut t).unwrap();
    //     t = ColumnTable::from_binary("test", &bint_t).unwrap();
    // }));
    // group.bench_function("execute UPDATE query", |b| b.iter(|| {
    //     execute_update_query(parsed_UPDATE_query.clone(), &mut t).unwrap();
    //     t = ColumnTable::from_binary("test", &bint_t).unwrap();
    // }));
    // group.bench_function("execute DELETE query", |b| b.iter(|| {
    //     execute_delete_query(parsed_DELETE_query.clone(), &mut t).unwrap();
    //     t = ColumnTable::from_binary("test", &bint_t).unwrap();
    // }));
    // group.bench_function("execute SUM query"   , |b| b.iter(|| {
    //     execute_summary_query(parsed_SUM_query.clone(), &mut t).unwrap();
    //     t = ColumnTable::from_binary("test", &bint_t).unwrap();
    // }));
    // group.bench_function("execute SELECT query", |b| b.iter(|| execute_select_query(parsed_SELECT_query.clone(), &mut t).unwrap()));
    // let mut t = ColumnTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    // group.bench_function("execute MEAN query"  , |b| b.iter(|| execute_summary_query(parsed_MEAN_query.clone(), &mut t).unwrap()));
    // let mut t = ColumnTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    // group.bench_function("execute MODE query"  , |b| b.iter(|| execute_summary_query(parsed_MODE_query.clone(), &mut t).unwrap()));
    // let mut t = ColumnTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    // group.bench_function("execute STDEV query" , |b| b.iter(|| execute_summary_query(parsed_STDEV_query.clone(), &mut t).unwrap()));
    // let mut t = ColumnTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    // group.bench_function("execute MEDIAN query", |b| b.iter(|| execute_summary_query(parsed_MEDIAN_query.clone(), &mut t).unwrap()));
    // let t = ColumnTable::from_csv_string(&google_docs_csv, "test", "test").unwrap();
    // let insert_me_string = std::fs::read_to_string(format!("test_files{PATH_SEP}insert_me.csv")).unwrap();
    // let insert_me = ColumnTable::from_csv_string(&insert_me_string, "insert_me", "QUERY").unwrap();
    // group.bench_function("execute LEFT_JOIN query", |b| b.iter(|| execute_left_join_query(parsed_LEFT_JOIN_query.clone(), &t, &insert_me).unwrap()));

    // let table_string = std::fs::read_to_string(&format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
    // let table = ColumnTable::from_csv_string(&table_string, "basic_test", "test").unwrap();
    // let binary = table.write_to_binary();

    // // group.bench_function("brotli compress", |b| b.iter(|| brotli_compress(&binary).unwrap()));
    // group.bench_function("miniz compress", |b| b.iter(|| miniz_compress(&binary).unwrap()));

    // // let brotli_compressed = brotli_compress(&binary).unwrap();
    // let miniz_compressed = miniz_compress(&binary).unwrap();

    // // group.bench_function("brotli decompress", |b| b.iter(|| brotli_decompress(&brotli_compressed).unwrap()));
    // group.bench_function("miniz decompress", |b| b.iter(|| miniz_decompress(&miniz_compressed).unwrap()));
    
    // let i32_slice: Vec<i32> = (0..98304).collect();
    // let mut f32_slice: Vec<f32> = Vec::new();
    // for _ in &i32_slice {
    //     f32_slice.push(rand::random());
    // }
    // // let f32_slice: Vec<f32> = i32_slice.clone().iter().map(|n| *n as f32).collect();
    // group.bench_function("sum_slice_i32", |b| b.iter(|| sum_i32_slice(&i32_slice)));
    // group.bench_function("sum_slice_f32", |b| b.iter(|| sum_f32_slice(&f32_slice)));
    // group.bench_function("sum_slice_f32_raw", |b| b.iter(|| unsafe{ raw_sum_f32_slice(&f32_slice) }));

    // group.bench_function("mean_slice_i32", |b| b.iter(|| mean_i32_slice(&i32_slice)));
    // group.bench_function("mean_slice_f32", |b| b.iter(|| mean_f32_slice(&f32_slice)));

    // group.bench_function("stdev_slice_i32", |b| b.iter(|| stdev_i32_slice(&i32_slice)));
    // group.bench_function("stdev_slice_f32", |b| b.iter(|| stdev_f32_slice(&f32_slice)));

    // group.bench_function("median_slice_i32", |b| b.iter(|| median_i32_slice(&i32_slice)));
    // group.bench_function("median_slice_f32", |b| b.iter(|| median_f32_slice(&f32_slice)));

    // let mut mode_slice = Vec::new();  
    // let mut thread_rng = rand::thread_rng();     
    // for i in 0..98304 {
    //     mode_slice.push(thread_rng.gen_range(0..1000));
    // }
    // group.bench_function("mode_slice_i32", |b| b.iter(|| mode_i32_slice(&i32_slice)));
    // let mut list_to_be_printed = Vec::new();
    // for i in 0..98304 {
    //     let x = KeyString::from(format!("text{i}").as_str());
    //     list_to_be_printed.push(x);
    // }
    // group.bench_function("print_sep_list", |b| b.iter(|| print_sep_list(&list_to_be_printed, ",")));


    // let table_string = std::fs::read_to_string(&format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
    // let table = ColumnTable::from_csv_string(&table_string, "basic_test", "test").unwrap();
    // group.bench_function("binary_vs_CBOR: BINARY VERSION", |b| b.iter(|| table.write_to_binary()));
    // group.bench_function("binary_vs_CBOR: CBOR VERSION", |b| b.iter(|| table.to_cbor_bytes()));
    

    // let query = Query::SELECT { 
    //     table_name: KeyString::from("test"), 
    //     primary_keys: RangeOrListOrAll::All, 
    //     columns: vec![KeyString::from("id")], 
    //     conditions: vec![OpOrCond::Cond(Condition{ attribute: KeyString::from("id"), test: Test::Equals(KeyString::from("0113035")) })] 
    // };

    // group.bench_function("ezql vs binary: binary short", |b| b.iter(|| {
    //     let binary_query = query.to_binary();
    //     let parsed_query = Query::from_binary(&binary_query).unwrap();
    // }));

    // group.bench_function("ezql vs binary: ezql short", |b| b.iter(|| {
    //     let string_query = query.to_string();
    //     let parsed_query = parse_EZQL(&string_query).unwrap();
    // }));

    // enum TestEnum {
    //     Zero,
    //     One,
    //     Two,
    //     Three,
    // }

    // let mut byte_string: Vec<u8> = Vec::new();
    // let mut word_string: Vec<u8> = Vec::new();
    // for i in 0..1000 {
    //     let x: usize = rand::thread_rng().gen_range(0..4);
    //     byte_string.push(x as u8);
    //     word_string.extend_from_slice(&x.to_le_bytes());
    // }

    // group.bench_function("bytes_parse", |b| b.iter(|| {
    //     let mut new_vec = Vec::new();
    //     for byte in &byte_string {
    //         let n = match byte {
    //             0 => TestEnum::Zero,
    //             1 => TestEnum::One,
    //             2 => TestEnum::Two,
    //             3 => TestEnum::Three,
    //             _ => panic!()
    //         };
    //         new_vec.push(n);
    //     }
    // }));

    // group.bench_function("words_parse", |b| b.iter(|| {
    //     let mut new_vec = Vec::new();
    //     for chunk in word_string.chunks(8) {
    //         let thing = u64_from_le_slice(chunk);
    //         let n = match thing {
    //             0 => TestEnum::Zero,
    //             1 => TestEnum::One,
    //             2 => TestEnum::Two,
    //             3 => TestEnum::Three,
    //             _ => panic!()
    //         };
    //         new_vec.push(n);
    //     }
    // }));

}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);