use miniz_oxide::deflate::compress_to_vec;
use miniz_oxide::inflate::decompress_to_vec;
// use brotli::{CompressorReader, Decompressor};
use std::io::{self, Read, Write};

use crate::utilities::{usize_from_le_slice, EzError};
use crate::PATH_SEP;


// // Function to compress data
// pub fn brotli_compress(input: &[u8]) -> Result<Vec<u8>, ServerError> {
//     if input.len() > MAX_PACKET_SIZE {
//         return Err(ServerError::OversizedData);
//     }
//     let mut compressed_data = Vec::new();
//     compressed_data.extend_from_slice(&input.len().to_le_bytes());
//     {
//         let mut compressor = CompressorReader::new(input, 4096, 11, 22);
//         io::copy(&mut compressor, &mut compressed_data).unwrap();
//     }
//     Ok(compressed_data)
// }

// // Function to decompress data
// pub fn brotli_decompress(input: &[u8]) -> Result<Vec<u8>, ServerError> {
//     if input.len() < 8 {
//         return Err(ServerError::Unimplemented("There should never be a packe less than 8 bytes".to_owned()));
//     }
//     let len = usize_from_le_slice(&input[0..8]);
//     println!("len: {}", len);
//     if len > MAX_PACKET_SIZE {
//         return Err(ServerError::OversizedData);
//     }
//     let mut decompressed_data = Vec::new();
//     {
//         let mut decompressor = Decompressor::new(&input[8..], 4096);
//         io::copy(&mut decompressor, &mut decompressed_data).unwrap();
//     }
//     Ok(decompressed_data)
// }


/// The maximum size of a package that will be processed in one transmission
pub const MAX_PACKET_SIZE: usize = 1_000_000_000;

/// Compresses a byte slice with miniz_oxide
/// Puts the uncompressed size of the package as a usize in the first 8 bytes
pub fn miniz_compress(data: &[u8]) -> Result<Vec<u8>, EzError> {
    if data.len() > MAX_PACKET_SIZE {
        return Err(EzError::OversizedData);
    }
    let mut output = Vec::with_capacity(data.len() + 8);
    output.extend_from_slice(&data.len().to_le_bytes());
    output.extend_from_slice(&compress_to_vec(data, 10));
    Ok(output)
}

/// decompresses a byte slice that was compressed with miniz_oxide
/// checks to see if decompressed size will be more than 1gb
pub fn miniz_decompress(data: &[u8]) -> Result<Vec<u8>, EzError> {
    if data.len() < 8 {
        return Err(EzError::Unimplemented("There should never be a packe less than 8 bytes".to_owned()));
    }
    let len = usize_from_le_slice(&data[0..8]);
    if len > MAX_PACKET_SIZE {
        return Err(EzError::OversizedData);
    }
    match decompress_to_vec(&data[8..]) {
        Ok(result) => Ok(result),
        Err(e) => {
            println!("failed to decompress because {e}");
            Err(EzError::Decompression(e))
        }
    }
}


#[cfg(test)]
mod tests {
    #![allow(unused)]

    use rand::Rng;

    use crate::{db_structure::EZTable, utilities::blake3_hash};

    use super::*;

    #[test]
    fn test_brotli() {
        let table_string = std::fs::read_to_string(&format!("test_files{PATH_SEP}test_csv_from_google_sheets_combined_sorted.csv")).unwrap();
        let table = EZTable::from_csv_string(&table_string, "basic_test", "test").unwrap();
        let binary = table.write_to_binary();
        // let brotli_compressed_table = brotli_compress(&binary).unwrap();
        let miniz_compressed_table = miniz_compress(&binary).unwrap();
        // println!("brotli: {}", brotli_compressed_table.len());
        println!("miniz: {}", miniz_compressed_table.len());

        // let brotli_decompressed = brotli_decompress(&brotli_compressed_table).unwrap();
        let miniz_decompressed = miniz_decompress(&miniz_compressed_table).unwrap();
        // let brotli_recovered_table = EZTable::from_binary("brotli", &brotli_decompressed).unwrap();
        let miniz_recovered_table = EZTable::from_binary("miniz", &miniz_decompressed).unwrap();

        // assert_eq!(table, brotli_recovered_table);
        assert_eq!(table, miniz_recovered_table);
    }

}