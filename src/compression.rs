use std::collections::HashMap;

pub fn lzw_compress(input: &str) -> Vec<u16> {
    let mut dictionary: HashMap<String, u16> = HashMap::new();
    for i in 0..=255 {
        let c = i as u8 as char;
        dictionary.insert(c.to_string(), i as u16);
    }
    
    let mut next_code = 256;
    let mut current_str = String::new();
    let mut output = Vec::new();

    for c in input.chars() {
        current_str.push(c);
        if !dictionary.contains_key(&current_str) {
            let previous_code = dictionary[&current_str[..current_str.len()-1]];
            output.push(previous_code);
            
            if next_code < u16::MAX {
                dictionary.insert(current_str.clone(), next_code);
                next_code += 1;
            }
            
            current_str.clear();
            current_str.push(c);
        }
    }
    
    if !current_str.is_empty() {
        let code = dictionary[&current_str];
        output.push(code);
    }
    
    output
}

pub fn lzw_decompress(compressed: Vec<u16>) -> String {
    let mut dictionary: HashMap<u16, String> = HashMap::new();
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
    use super::*;

    #[test]
    fn test_compression() {
        let data = "ABABABABA";
        let compressed = lzw_compress(data);
        println!("Original: {}", data);
        println!("Compressed: {:?}", compressed);
    }

    #[test]
    fn test_decompression() {
        let compressed_data = vec![65, 66, 256, 257, 259];
        let decompressed = lzw_decompress(compressed_data);
        println!("Decompressed: {}", decompressed);
    }

}