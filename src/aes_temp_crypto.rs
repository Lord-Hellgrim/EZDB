use std::{io::{Read, Write}, net::TcpStream};

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng, generic_array::GenericArray},
    Aes256Gcm, Key // Or `Aes128Gcm`
};

use crate::{compression, utilities::{Connection, EzError, DATA_BUFFER, MAX_DATA_LEN}};

// TODO Add a handler for using the tag
pub fn encrypt_aes256(s: &[u8], key: &[u8]) -> (Vec<u8>, [u8;12]) {

    let key = Key::<Aes256Gcm>::from_slice(key);

    let cipher = Aes256Gcm::new(key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message
    let ciphertext = cipher.encrypt(&nonce, s).unwrap(); // safe because we generate the nonce here
    (ciphertext, nonce.into())
    
}

pub fn decrypt_aes256(s: &[u8], key: &[u8], nonce: &[u8] ) -> Result<Vec<u8>, EzError> {
    // TODO Add clause to handle the case where the nonce is not 12 bytes
    let key = Key::<Aes256Gcm>::from_slice(key);
    
    let cipher = Aes256Gcm::new(key);
    let nonce = GenericArray::clone_from_slice(nonce); // 96-bits; unique per message
    let plaintext = cipher.decrypt(&nonce, s)?;
    Ok(plaintext)
}

pub fn send_encrypted_data(data: &[u8], connection: &mut Connection) -> Result<(), EzError> {
    
    let data = compression::miniz_compress(data)?;
    let (encrypted_data, data_nonce) = encrypt_aes256(&data, &connection.aes_key);

    let mut encrypted_data_block = Vec::with_capacity(data.len() + 28);
    encrypted_data_block.extend_from_slice(&encrypted_data);
    encrypted_data_block.extend_from_slice(&data_nonce);


    // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
    // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
    let mut block = Vec::from(&(data.len() + 28).to_le_bytes());
    block.extend_from_slice(&encrypted_data_block);
    connection.stream.write_all(&block)?;
    connection.stream.flush()?;

    Ok(())
}

pub fn receive_encrypted_data(connection: &mut Connection) -> Result<Vec<u8>, EzError> {

    let mut size_buffer: [u8; 8] = [0; 8];
    connection.stream.read_exact(&mut size_buffer)?;

    let data_len = usize::from_le_bytes(size_buffer);
    if data_len > MAX_DATA_LEN {
        return Err(EzError::OversizedData)
    }
    
    let mut data = Vec::with_capacity(data_len);
    let mut buffer = [0; DATA_BUFFER];
    let mut total_read: usize = 0;
    
    while total_read < data_len {
        let to_read = std::cmp::min(DATA_BUFFER, data_len - total_read);
        let bytes_received = connection.stream.read(&mut buffer[..to_read])?;
        if bytes_received == 0 {
            return Err(EzError::Confirmation("Read failure".to_owned()));
        }
        data.extend_from_slice(&buffer[..bytes_received]);
        total_read += bytes_received;
        println!("Total read: {}", total_read);
    }

    let (ciphertext, nonce) = (&data[0..data.len()-12], &data[data.len()-12..]);
    let csv = decrypt_aes256(ciphertext, &connection.aes_key, nonce)?;

    let csv = compression::miniz_decompress(&csv)?;
    Ok(csv)

}

mod tests {
    #![allow(unused)]

    use super::*;

    #[test]
    fn test_encrypt_then_decrypt() {
        let key: [u8;32] = [42;32];

        let plaintext = String::from("This is the text");
        let (ciphertext, nonce) = encrypt_aes256(&plaintext.as_bytes(), &key);
        println!("ciphertext: {:x?}", ciphertext);
        let decrypted_ciphertext = decrypt_aes256(&ciphertext, &key, &nonce).unwrap();
        println!("Plaintext: {}", plaintext);
        assert_eq!(plaintext.as_bytes(), decrypted_ciphertext);
        println!("Decrypted: {}", String::from_utf8(decrypted_ciphertext).unwrap());
    }

    #[test]
    fn test_encryption() {

        // The encryption key can be generated randomly:
        let key = Aes256Gcm::generate_key(OsRng);

        // Transformed from a byte array:
        let key: &[u8; 32] = &[42; 32];
        let key: &Key<Aes256Gcm> = key.into();

        // Note that you can get byte array from slice using the `TryInto` trait:
        let key: &[u8] = &[42; 32];
        let key: [u8; 32] = key.try_into().unwrap();

        // Alternatively, the key can be transformed directly from a byte slice
        // (panicks on length mismatch):
        let key = Key::<Aes256Gcm>::from_slice(&key);
        
        let cipher = Aes256Gcm::new(&key);
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message
        let ciphertext = cipher.encrypt(&nonce, b"plaintext message".as_ref()).unwrap();
        let plaintext = cipher.decrypt(&nonce, ciphertext.as_ref()).unwrap();
    
        assert_eq!(&plaintext, b"plaintext message");
    }
}