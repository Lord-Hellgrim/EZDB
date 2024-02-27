use miniz_oxide::deflate::compress_to_vec;
use miniz_oxide::inflate::decompress_to_vec;

use crate::networking_utilities::{usize_from_le_slice, ServerError};

/// The maximum size of a package that will be processed in one transmission
pub const MAX_PACKET_SIZE: usize = 1_000_000_000;

/// Compresses a byte slice with miniz_oxide
/// Puts the uncompressed size of the package as a usize in the first 8 bytes
pub fn miniz_compress(data: &[u8]) -> Result<Vec<u8>, ServerError> {
    if data.len() > MAX_PACKET_SIZE {
        return Err(ServerError::OversizedData);
    }
    let mut output = Vec::with_capacity(data.len() + 8);
    output.extend_from_slice(&data.len().to_le_bytes());
    output.extend_from_slice(&compress_to_vec(data, 10));
    Ok(output)
}

/// decompresses a byte slice that was compressed with miniz_oxide
/// checks to see if decompressed size will be more than 1gb
pub fn miniz_decompress(data: &[u8]) -> Result<Vec<u8>, ServerError> {
    let len = usize_from_le_slice(&data[0..8]);
    if len > MAX_PACKET_SIZE {
        return Err(ServerError::OversizedData);
    }
    match decompress_to_vec(&data[8..]) {
        Ok(result) => Ok(result),
        Err(e) => {
            println!("failed to decompress because {e}");
            Err(ServerError::Decompression(e))
        }
    }
}