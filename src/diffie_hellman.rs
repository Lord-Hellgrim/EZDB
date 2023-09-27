extern crate num_bigint;
use std::array;

use num_bigint::BigUint;
use num_traits::{One, Zero};
use rand::Rng;


pub const PRIME: &str = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7EDEE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3BE39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF6955817183995497CEA956AE515D2261898FA051015728E5A8AACAA68FFFFFFFFFFFFFFFF";
pub const GENERATOR: &str = "2";


pub struct DiffieHellman {
    pub p: BigUint,
    pub g: BigUint,
    pub private_key: BigUint,
}

impl DiffieHellman {
    pub fn new() -> Self {
        let p = BigUint::parse_bytes(PRIME.as_bytes(), 16).unwrap();
        let g = BigUint::parse_bytes(GENERATOR.as_bytes(), 16).unwrap();
        let private_key = Self::random_key(&p);

        DiffieHellman { p, g, private_key }
    }

    pub fn random_key(p: &BigUint) -> BigUint {
        let mut rng = rand::thread_rng();
        loop {
            let random = BigUint::from_bytes_le(&rng.gen::<[u8; 16]>());
            if &random < p && random > BigUint::zero() {
                return random;
            }
        }
    }

    pub fn public_key(&self) -> BigUint {
        self.g.modpow(&self.private_key, &self.p)
    }

    pub fn shared_secret(&self, other_public: &BigUint) -> BigUint {
        other_public.modpow(&self.private_key, &self.p)
    }

}

pub fn public_key_from_private_key(private_key: &BigUint) -> BigUint {
    let g = BigUint::parse_bytes(GENERATOR.as_bytes(), 16).unwrap();
    let p = BigUint::parse_bytes(PRIME.as_bytes(), 16).unwrap();
    g.modpow(private_key, &p)
}

pub fn shared_secret(other_public: &BigUint, local_private_key: &BigUint) -> BigUint {
    let p = BigUint::parse_bytes(PRIME.as_bytes(), 16).unwrap();

    other_public.modpow(&local_private_key, &p)
}

pub fn aes256key(shared_secret: &[u8]) -> Vec<u8> {

    blake3::hash(&shared_secret).as_bytes().to_vec()

}



#[cfg(test)]
mod tests {

    use crate::{aes, networking_utilities::hash_function};

    use super::*;
    
    #[test]
    fn test_hash_password() {
        let s = "admin";
        println!("{:x?}", aes256key(s.as_bytes()));
        println!("{:x?}", hash_function(s));
    }

    #[test]
    fn test_diffie_hellman() {
        // Sample prime (p) and generator (g) values
        let alice = DiffieHellman::new();
        let bob = DiffieHellman::new();

        let alice_pub_key = alice.public_key();
        let bob_pub_key = bob.public_key();

        let alice_secret = alice.shared_secret(&bob_pub_key);
        let bob_secret = bob.shared_secret(&alice_pub_key);

        assert_eq!(alice_secret, bob_secret);
        println!("Shared secret: {}", alice_secret);

        let alice_aes_key = aes256key(&alice_secret.to_bytes_le());
        let bob_aes_key = aes256key(&bob_secret.to_bytes_le());
        assert_eq!(alice_aes_key, bob_aes_key);
        assert_eq!(alice_aes_key.len(), 32);
        println!("bob_aes_key: {:x?}\nalice_aes_key: {:x?}", bob_aes_key, alice_aes_key);
    }

    #[test]
    fn test_blake3() {
        // Hash an input all at once.
        let hash1 = blake3::hash(b"foobarbaz");

        // Hash an input incrementally.
        let mut hasher = blake3::Hasher::new();
        hasher.update(b"foo");
        hasher.update(b"bar");
        hasher.update(b"baz");
        let hash2 = hasher.finalize();
        assert_eq!(hash1, hash2);

        // Extended output. OutputReader also implements Read and Seek.
        let mut output = [0; 1000];
        let mut output_reader = hasher.finalize_xof();
        output_reader.fill(&mut output);
        assert_eq!(hash1, output[..32]);

        // Print a hash as hex.
        println!("{}", hash1);
    }
}