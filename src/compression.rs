use std::io::Write;

use miniz_oxide::deflate::compress_to_vec;
use miniz_oxide::inflate::decompress_to_vec;

use brotli::{self, CompressorWriter, DecompressorWriter};

use crate::networking_utilities::{usize_from_le_slice, ServerError};

pub const MAX_PACKET_SIZE: usize = 1_000_000_000;

pub fn miniz_compress(data: &[u8], level: u8) -> Vec<u8> {
    let mut output = Vec::with_capacity(data.len() + 8);
    output.extend_from_slice(&data.len().to_le_bytes());
    output.extend_from_slice(&compress_to_vec(data, level));
    output
}

pub fn miniz_decompress(data: &[u8]) -> Result<Vec<u8>, ServerError> {
    let len = usize_from_le_slice(&data[0..8]);
    if len > MAX_PACKET_SIZE {
        return Err(ServerError::OversizedData)
    }
    match decompress_to_vec(&data[8..]) {
        Ok(result) => Ok(result),
        Err(e) => {
            println!("failed to decompress because {e}");
            Err(ServerError::Decompression(e))
        },
    }
}

pub fn brotli_compress(data: &[u8]) -> Vec<u8> {
    let mut compressed_data = Vec::new();
        {
            let mut compressor = CompressorWriter::new(&mut compressed_data, 4096, 11, 22);
            compressor.write_all(data).unwrap();
        }
    compressed_data
}

pub fn brotli_decompress(data: &[u8]) -> Vec<u8> {
    let mut decompressed_data = Vec::new();
        {
            let mut decompressor = DecompressorWriter::new(&mut decompressed_data, 4096);
            decompressor.write_all(data).unwrap();
        }
    decompressed_data
}


#[cfg(test)]
mod tests {

    use std::{io::Write, string};

    use crate::db_structure::*;

    use super::*;

    use crate::PATH_SEP;

    use miniz_oxide::deflate::compress_to_vec;
    use miniz_oxide::inflate::decompress_to_vec;

    use brotli::{self, CompressorWriter};


    #[test]
    fn test_brotli() {

        let input = std::fs::read_to_string(format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
        let t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();

        let string_t = t.to_string();
        let binary_t = t.write_to_raw_binary();

        let compressed_string = brotli_compress(string_t.as_bytes());

        let compressed_binary = brotli_compress(&binary_t);

        println!("compressed string length: {}", compressed_string.len());
        println!("compressed binary length: {}", compressed_string.len());

        let decompressed_string = brotli_decompress(&compressed_string);

        let decompressed_binary = brotli_decompress(&compressed_binary);

        assert_eq!(string_t.as_bytes(), decompressed_string);
        assert_eq!(binary_t, decompressed_binary);
    }


    #[test]
    fn test_miniz_oxide() {
        
        let input = std::fs::read_to_string(format!("vorufletting_no_dups_for_export.txt")).unwrap();
        let t = ColumnTable::from_csv_string(&input, "test", "test").unwrap();

        let string_t = t.to_string();
        let binary_t = t.write_to_raw_binary();

        let string_time = std::time::Instant::now();
        let compressed_string = compress_to_vec(string_t.as_bytes(), 10);
        let x = string_time.elapsed();
        println!("time to compress string: {}", x.as_micros());

        let binary_time = std::time::Instant::now();
        let compressed_binary = compress_to_vec(&binary_t, 10);
        let y = binary_time.elapsed();
        println!("time to compress binary: {}", y.as_micros());

        println!("raw string length: {}", string_t.len());
        println!("compressed_string length: {}", compressed_string.len());
        println!("raw binary length: {}", binary_t.len());
        println!("compressed_binary length: {}", compressed_binary.len());

        let uncompressed_string = decompress_to_vec(&compressed_string).unwrap();
        let uncompressed_binary = decompress_to_vec(&compressed_binary).unwrap();

        assert_eq!(string_t.as_bytes(), uncompressed_string);
        assert_eq!(binary_t, uncompressed_binary);

    }



}