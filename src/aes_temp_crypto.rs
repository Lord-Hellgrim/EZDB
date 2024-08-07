use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng, generic_array::GenericArray},
    Aes256Gcm, Key // Or `Aes128Gcm`
};

use crate::networking_utilities::ServerError;


pub fn encrypt_aes256(s: &[u8], key: &[u8]) -> (Vec<u8>, [u8;12]) {

    let key = Key::<Aes256Gcm>::from_slice(key);

    let cipher = Aes256Gcm::new(key);
    let nonce = Aes256Gcm::generate_nonce(&mut OsRng); // 96-bits; unique per message
    let ciphertext = cipher.encrypt(&nonce, s).unwrap(); // safe because we generate the nonce here
    (ciphertext, nonce.into())
    
}

pub fn decrypt_aes256(s: &[u8], key: &[u8], nonce: &[u8] ) -> Result<Vec<u8>, ServerError> {
    // TODO Add clause to handle the case where the nonce is not 12 bytes
    let key = Key::<Aes256Gcm>::from_slice(key);
    
    let cipher = Aes256Gcm::new(key);
    let nonce = GenericArray::clone_from_slice(nonce); // 96-bits; unique per message
    let plaintext = cipher.decrypt(&nonce, s)?;
    Ok(plaintext)
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