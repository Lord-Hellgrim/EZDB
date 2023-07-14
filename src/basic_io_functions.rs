use std::{path::Path, fs::read_to_string, collections::HashMap};

pub fn split_string(s: &String, sep: char) -> Vec<String> {
    let mut output = Vec::new();
    let mut temp = String::from("");
    for c in s.chars() {
        if c != sep {
            temp.push(c);
        } else {
            output.push(temp.clone());
            temp.clear();
        }
    }
    if temp != "" {
        output.push(temp);
    }
    output
}


pub fn map_row(line: Vec<String>, keys: &Vec<String>) -> HashMap<String, String> {
    let mut output = HashMap::new();

    let mut i: usize = 0;
    for cell in line {
        let key = keys[i].clone();
        output.insert(key, cell);
        i += 1;
    }

    output
}


pub fn read_path_to_hashmap(path: &Path, sep: char, pk: usize) -> (Vec<String>, HashMap<String, HashMap<String, String>>) {
    let s = match read_to_string(path) {
        Err(why) => panic!("Could not load file {} because {}", path.display(), why),
        Ok(content) => content,
    };
    let mut first_pass = split_string(&s, '\n');
    let header = split_string(&first_pass[0].clone(), sep);
    if pk > header.len() {
        panic!("pk: {pk} must be less than the length of the header")
    }
    let mut second_pass = HashMap::new();
    loop {
        match first_pass.pop() {
            Some(thing) => {
                let temp = split_string(&thing, sep);
                let key = temp[pk].clone();
                let inner = map_row(temp, &header);
                second_pass.insert(key, inner);
            },
            None => break,
        };
    }
    second_pass.remove(&header[pk]);
    (header, second_pass)
}


pub fn read_to_vec(path: &Path, sep: char) -> (Vec<String>, Vec<Vec<String>>) {
    let s = match read_to_string(path) {
        Err(why) => panic!("Could not load file {} because {}", path.display(), why),
        Ok(content) => content,
    };

    let lines = split_string(&s, '\n');

    let mut output = Vec::new();
    for line in lines {
        let temp = split_string(&line, sep);
        output.push(temp);
    }

    let header = output.remove(0);
    (header, output)
}

pub fn vec_string_to_str(vec: &Vec<String>) -> Vec<&str> {
    let v: Vec<&str> = vec.iter().map(|s| s as &str).collect();
    v
}


pub fn hashmap_to_string(map: &mut HashMap<String, HashMap<String, String>>, header: Vec<String>, sep: char) -> String {
    let mut printer = String::from("");
    let length = header.len();

    for item in &header {
        printer.push_str(&item);
        printer.push(sep);
    }
    printer.pop().unwrap();
    printer.push('\n');

    let mut i: usize = 0;

    for (topkey, _) in map.iter() {
        while i < length {
            printer.push_str(&map[topkey][&header[i]]);
            printer.push(sep);
            i += 1;
        }
        printer.pop().unwrap();
        printer.push('\n');
        i = 0;
    }

    printer
    }


#[cfg(test)]
mod tests {
    use std::{io::Write, fs::File};

    use super::*;

    #[test]
    fn test_split_string() {
        let result = split_string(&String::from("a;b;c\nd;e;f\n"), '\n');
        println!("result: {:?}", result);
        assert_eq!(result[0], String::from("a;b;c"));

    }

    #[test]
    fn test_read_path_to_hashmap() {
        let path = Path::new("sample_data.txt");
        let (head, result2) = read_path_to_hashmap(&path, '\t', 0);
        println!("Header: {:?}", head);
        println!("Result2: {:?}", result2);
        assert!(head[0] == String::from("vnr"));
        assert!(result2["0113035"]["heiti"] == "undirlegg");
    }

    #[test]
    #[should_panic]
    fn pk_bigger_than_header() {
        let path = Path::new("sample_data.txt");
        let (head, result2) = read_path_to_hashmap(&path, '\t', 500);
        println!("{:?}, {:?}", head, result2);
    }

    #[test]
    fn test_map_row() {
        let s = Vec::from([String::from("0113035"), String::from("undirlegg"), String::from("200")]);
        let head = Vec::from([String::from("vnr"), String::from("heiti"), String::from("magn")]);

        let map = map_row(s, &head);

        println!("{:?}", map);

    }

    #[test]
    fn test_hashmap_to_string() {
        let path = Path::new("sample_data.txt");
        let sep = '\t';
        let (header, mut map) = read_path_to_hashmap(&path, sep, 0);
        let s = hashmap_to_string(&mut map, header, sep);
        println!("{s}");
    }

    #[test]
    fn test_write() {
        let path = Path::new("sample_data.txt");
        let sep = '\t';
        let (header, mut map) = read_path_to_hashmap(&path, sep, 0);
        let s = hashmap_to_string(&mut map, header, ';');

        println!("{}", s);
        let mut file = File::create("sample_output.txt").unwrap();
        file.write_all(s.as_bytes()).unwrap();
    }

    #[test]
    fn test_read_to_vec() {
        let path = Path::new("sample_data.txt");
        let sep = '\t';

        let (header, q) = read_to_vec(path, sep);
        println!("The header is: \n{:?} \n and the content is: \n{:?}", header,  q);
    }

}