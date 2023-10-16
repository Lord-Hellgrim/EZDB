use std::array;

use rug::{Integer, Complete};
use rand::Rng;


pub const PRIME: &str = "FFFFFFFFFFFFFFFFC90FDAA22168C234C4C6628B80DC1CD129024E088A67CC74020BBEA63B139B22514A08798E3404DDEF9519B3CD3A431B302B0A6DF25F14374FE1356D6D51C245E485B576625E7EC6F44C42E9A637ED6B0BFF5CB6F406B7EDEE386BFB5A899FA5AE9F24117C4B1FE649286651ECE45B3DC2007CB8A163BF0598DA48361C55D39A69163FA8FD24CF5F83655D23DCA3AD961C62F356208552BB9ED529077096966D670C354E4ABC9804F1746C08CA18217C32905E462E36CE3BE39E772C180E86039B2783A2EC07A28FB5C55DF06F4C52C9DE2BCBF6955817183995497CEA956AE515D2261898FA051015728E5A8AACAA68FFFFFFFFFFFFFFFF";
pub const GENERATOR: &str = "2";


// TODO: Replace num-bigint with rug


pub struct DiffieHellman {
    pub p: Integer,
    pub g: Integer,
    pub private_key: Integer,
}

impl DiffieHellman {
    pub fn new() -> Self {
        let p = Integer::parse_radix(PRIME.as_bytes(), 16).unwrap().complete();
        let g = Integer::parse_radix(GENERATOR.as_bytes(), 16).unwrap().complete();
        let private_key = Self::random_key(&p);

        DiffieHellman { p, g, private_key }
    }

    pub fn random_key(p: &Integer) -> Integer {
        let mut rng = rand::thread_rng();
        loop {
            let rng = rng.gen::<[u8; 16]>();
            println!("rng: {:x?}", rng);
            let random = Integer::from_digits(&rng, rug::integer::Order::Lsf);
            if &random < p && random > Integer::from(0){
                return random;
            }
        }
    }

    pub fn public_key(&self) -> Integer {
        self.g.clone().pow_mod(&self.private_key, &self.p).unwrap() // safe since we will always pass positive numbers
    }

    pub fn shared_secret(&self, other_public: &Integer) -> Integer {
        other_public.clone().pow_mod(&self.private_key, &self.p).unwrap() // safe since we will always pass positive numbers
    }

}

pub fn public_key_from_private_key(private_key: &Integer) -> Integer {
    let g = Integer::parse_radix(GENERATOR.as_bytes(), 16).unwrap().complete();
    let p = Integer::parse_radix(PRIME.as_bytes(), 16).unwrap().complete();
    g.pow_mod(private_key, &p).unwrap() // safe since we will always pass positive numbers
}

pub fn shared_secret(other_public: &Integer, local_private_key: &Integer) -> Integer {
    let p = Integer::parse_radix(PRIME.as_bytes(), 16).unwrap().complete();

    other_public.clone().pow_mod(&local_private_key, &p).unwrap() // safe since we will always pass positive numbers
}

pub fn blake3_hash(s: &[u8]) -> Vec<u8> {

    blake3::hash(s).as_bytes().to_vec()

}



#[cfg(test)]
mod tests {

    use rug::integer::Order;

    use crate::{aes, networking_utilities::hash_function};

    use super::*;
    
    #[test]
    fn test_hash_password() {
        let s = "admin";
        println!("{:x?}", blake3_hash(s.as_bytes()));
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

        let alice_aes_key = blake3_hash(&alice_secret.to_digits::<u8>(Order::Lsf));
        let bob_aes_key = blake3_hash(&bob_secret.to_digits::<u8>(Order::Lsf));
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