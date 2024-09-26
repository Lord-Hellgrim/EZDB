use std::{io::{Read, Write}, net::TcpStream};

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng, generic_array::GenericArray},
    Aes256Gcm, Key // Or `Aes128Gcm`
};

use crate::{compression, utilities::{Connection, EzError, DATA_BUFFER, MAX_DATA_LEN}};

// TODO Add a handler for using the tag
pub fn encrypt_aes256(s: &[u8], key: &[u8]) -> (Vec<u8>, [u8;12]) {
    println!("calling: encrypt_aes256()");


    let key = Key::<Aes256Gcm>::from_slice(key);

    let cipher = Aes256Gcm::new(key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message
    let ciphertext = cipher.encrypt(&nonce, s).unwrap(); // safe because we generate the nonce here
    (ciphertext, nonce.into())
    
}


pub fn decrypt_aes256(s: &[u8], key: &[u8], nonce: &[u8] ) -> Result<Vec<u8>, EzError> {
    println!("calling: decrypt_aes256()");

    // TODO Add clause to handle the case where the nonce is not 12 bytes
    let key = Key::<Aes256Gcm>::from_slice(key);
    
    let cipher = Aes256Gcm::new(key);
    let nonce = GenericArray::clone_from_slice(nonce); // 96-bits; unique per message
    let plaintext = cipher.decrypt(&nonce, s)?;
    Ok(plaintext)
}

pub fn encrypt_aes256_nonce_prefixed(s: &[u8], key: &[u8]) -> Vec<u8> {
    println!("calling: encrypt_aes256_nonce_prefixed()");

    let key = Key::<Aes256Gcm>::from_slice(key);

    let cipher = Aes256Gcm::new(key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng).into(); // 96-bits; unique per message
    let ciphertext = cipher.encrypt(&nonce, s).unwrap();
    let mut result = Vec::new();
    result.extend_from_slice(&nonce); // safe because we generate the nonce here
    result.extend_from_slice(&ciphertext);
    result
}

pub fn decrypt_aes256_with_prefixed_nonce(s: &[u8], key: &[u8]) -> Result<Vec<u8>, EzError> {
    println!("calling: decrypt_aes256_with_prefixed_nonce()");
    if s.len() < 13 {
        return Err(EzError::Crypto("slice has no bytes to encrypt".to_owned()))
    }

    // TODO Add clause to handle the case where the nonce is not 12 bytes
    let key = Key::<Aes256Gcm>::from_slice(key);
    let cipher = Aes256Gcm::new(key);
    let nonce = GenericArray::clone_from_slice(&s[0..12]); // 96-bits; unique per message
    let plaintext = cipher.decrypt(&nonce, &s[12..])?;
    Ok(plaintext)
}

mod tests {
    #![allow(unused)]

    use super::*;

    #[test]
    fn test_prefixed_nonce() {
        let plaintext = String::from("OK");
        let key = [42 as u8;32];
        let ciphertext_with_nonce = encrypt_aes256_nonce_prefixed(plaintext.as_bytes(), &key);
        let deciphered = decrypt_aes256_with_prefixed_nonce(&ciphertext_with_nonce, &key).unwrap();
        assert_eq!(plaintext.as_bytes(), deciphered);
    }

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