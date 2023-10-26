use std::collections::HashMap;
use fnv::*;


pub fn lzw_compress_opt(input: &str) -> Vec<u16> {
    let input = input.as_bytes();
    let mut dictionary =  FnvHashMap::default();
    for i in 0..=255 {
        let c = i;
        dictionary.insert(vec![c as u8], i as u16);
    }
    
    let mut next_code = 256;
    let mut current_str = Vec::with_capacity(input.len()/2);
    let mut output = Vec::with_capacity(input.len()/2);

    let mut i = 0;
    while i < input.len() {
        current_str.push(input[i]);
        if !dictionary.contains_key(&current_str) {
            let previous_code = dictionary[&current_str[..current_str.len()-1]];
            output.push(previous_code);
            
            if next_code < u16::MAX {
                dictionary.insert(current_str.clone(), next_code);
                next_code += 1;
            }
            
            current_str.clear();
            current_str.push(input[i]);
        }
        i += 1;
    }
    
    if !current_str.is_empty() {
        let code = dictionary[&current_str];
        output.push(code);
    }
    
    output.shrink_to_fit();

    output
}


pub fn lzw_decompress_opt(compressed: Vec<u16>) -> String {
    let mut dictionary: FnvHashMap<u16, String> = FnvHashMap::default();
    for i in 0..=255 {
        let c = i as u8 as char;
        dictionary.insert(i as u16, c.to_string());
    }

    let mut next_code = 256;
    let mut current_str = dictionary[&compressed[0]].clone();
    let mut output = current_str.clone();

    for &code in &compressed[1..] {
        let entry = if dictionary.contains_key(&code) {
            dictionary[&code].clone()
        } else if code == next_code {
            current_str.clone() + &current_str.chars().next().unwrap().to_string()
        } else {
            panic!("Invalid compressed code");
        };

        output += &entry;

        if next_code < u16::MAX {
            dictionary.insert(next_code, current_str + &entry.chars().next().unwrap().to_string());
            next_code += 1;
        }

        current_str = entry;
    }

    output
}

#[cfg(test)]
mod tests {
    use std::io::Write;

    use super::*;

    #[test]
    fn test_compression() {
        let data = "vnr;heiti;magn\n0113000;undirlegg2;100\n0113035;undirlegg;200\n18572054;flísalím;42";
        let compressed = lzw_compress_opt(data);
        println!("Original: {}", data);
        println!("Compressed: {:?}", compressed);
    }

    #[test]
    fn test_decompression() {
        let compressed_data = vec![65, 66, 256, 257, 259];
        let decompressed = lzw_decompress_opt(compressed_data);
        println!("Decompressed: {}", decompressed);
    }

    #[test]
    fn test_large_file_compression() {
        let mut printer = String::from("vnr;heiti;magn\n");
        let mut i = 0;
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
        let big_csv = "vnr;heiti;magn\n0113000;undirlegg2;100\n0113035;undirlegg;200\n18572054;flísalím;42".to_owned();
        println!("size of uncompressed: {}", big_csv.as_bytes().len());
        let instant = std::time::Instant::now();
        let compressed = lzw_compress_opt(&big_csv);
        println!("time: {}", instant.elapsed().as_millis());
        println!("size of uncompressed: {}", big_csv.capacity()*2);
        println!("ratio: {}", (big_csv.as_bytes().len())/(compressed.capacity()*2));
    }

}