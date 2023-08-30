use std::io::Write;

fn main() {
    let mut i = 0;
    let mut printer = String::from("vnr;heiti;magn\n");
    loop {
        if i%100 == 0 {
            println!("Still going i: {}", i);
        }
        if i > 1_000_000 {
            break;
        }
        printer.push_str(&format!("i{};product name;569\n", i));
        i+= 1;
    }
    let mut file = std::fs::File::create("large.csv").unwrap();
    file.write_all(printer.as_bytes());
}