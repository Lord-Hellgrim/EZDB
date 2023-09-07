use std::arch::x86_64::{__m128i, _mm_loadu_si128, _mm_setzero_si128, _mm_aesenc_si128, _mm_xor_si128, _mm_aesenclast_si128, _mm_storeu_si128, _mm_aesdec_si128, _mm_aesdeclast_si128, _mm_aesimc_si128};


// This is the AES substitution box. Source "NIST.FIPS.197-upd1.pdf"
const SBOX: [[u8;16];16] = [/*  0     1     2     3     4     5     6     7     8     9     A     B     C     D     E     F */
                        /*0*/ [0x63, 0x7c, 0x77, 0x7b, 0xf2, 0x6b, 0x6f, 0xc5, 0x30, 0x01, 0x67, 0x2b, 0xfe, 0xd7, 0xab, 0x76],
                        /*1*/ [0xca, 0x82, 0xc9, 0x7d, 0xfa, 0x59, 0x47, 0xf0, 0xad, 0xd4, 0xa2, 0xaf, 0x9c, 0xa4, 0x72, 0xc0],
                        /*2*/ [0xb7, 0xfd, 0x93, 0x26, 0x36, 0x3f, 0xf7, 0xcc, 0x34, 0xa5, 0xe5, 0xf1, 0x71, 0xd8, 0x31, 0x15],
                        /*3*/ [0x04, 0xc7, 0x23, 0xc3, 0x18, 0x96, 0x05, 0x9a, 0x07, 0x12, 0x80, 0xe2, 0xeb, 0x27, 0xb2, 0x75],
                        /*4*/ [0x09, 0x83, 0x2c, 0x1a, 0x1b, 0x6e, 0x5a, 0xa0, 0x52, 0x3b, 0xd6, 0xb3, 0x29, 0xe3, 0x2f, 0x84],
                        /*5*/ [0x53, 0xd1, 0x00, 0xed, 0x20, 0xfc, 0xb1, 0x5b, 0x6a, 0xcb, 0xbe, 0x39, 0x4a, 0x4c, 0x58, 0xcf],
                        /*6*/ [0xd0, 0xef, 0xaa, 0xfb, 0x43, 0x4d, 0x33, 0x85, 0x45, 0xf9, 0x02, 0x7f, 0x50, 0x3c, 0x9f, 0xa8],
                        /*7*/ [0x51, 0xa3, 0x40, 0x8f, 0x92, 0x9d, 0x38, 0xf5, 0xbc, 0xb6, 0xda, 0x21, 0x10, 0xff, 0xf3, 0xd2],
                        /*8*/ [0xcd, 0x0c, 0x13, 0xec, 0x5f, 0x97, 0x44, 0x17, 0xc4, 0xa7, 0x7e, 0x3d, 0x64, 0x5d, 0x19, 0x73],
                        /*9*/ [0x60, 0x81, 0x4f, 0xdc, 0x22, 0x2a, 0x90, 0x88, 0x46, 0xee, 0xb8, 0x14, 0xde, 0x5e, 0x0b, 0xdb],
                        /*A*/ [0xe0, 0x32, 0x3a, 0x0a, 0x49, 0x06, 0x24, 0x5c, 0xc2, 0xd3, 0xac, 0x62, 0x91, 0x95, 0xe4, 0x79],
                        /*B*/ [0xe7, 0xc8, 0x37, 0x6d, 0x8d, 0xd5, 0x4e, 0xa9, 0x6c, 0x56, 0xf4, 0xea, 0x65, 0x7a, 0xae, 0x08],
                        /*C*/ [0xba, 0x78, 0x25, 0x2e, 0x1c, 0xa6, 0xb4, 0xc6, 0xe8, 0xdd, 0x74, 0x1f, 0x4b, 0xbd, 0x8b, 0x8a],
                        /*D*/ [0x70, 0x3e, 0xb5, 0x66, 0x48, 0x03, 0xf6, 0x0e, 0x61, 0x35, 0x57, 0xb9, 0x86, 0xc1, 0x1d, 0x9e],
                        /*E*/ [0xe1, 0xf8, 0x98, 0x11, 0x69, 0xd9, 0x8e, 0x94, 0x9b, 0x1e, 0x87, 0xe9, 0xce, 0x55, 0x28, 0xdf],
                        /*F*/ [0x8c, 0xa1, 0x89, 0x0d, 0xbf, 0xe6, 0x42, 0x68, 0x41, 0x99, 0x2d, 0x0f, 0xb0, 0x54, 0xbb, 0x10],
                        ]; 

// These are the round constants for the AES key expansion algorithm. Source: "NIST.FIPS.197-upd1.pdf"
const RCON: [u32;10] = [0x01000000, 0x02000000, 0x04000000, 0x08000000, 0x10000000, 0x20000000, 0x40000000, 0x80000000, 0x1b000000, 0x36000000];

#[cfg(not(target_feature="sse"))]
pub fn array_xor(a: [u8;16], b: [u8;16]) -> [u8;16] {
    let mut c = [0u8;16];
    let mut i = 0;
    while i < 15 {
        c[i] = a[i] ^ b[i];
        i += 1;
    }
    c
}   


#[cfg(any(target_feature="sse", target_feature="avx", target_feature="avx2"))]
pub unsafe fn array_xor(a: [u8;16], b: [u8;16]) -> [u8;16] {
    let a = _mm_loadu_si128(a.as_ptr() as *const __m128i);
    let b = _mm_loadu_si128(b.as_ptr() as *const __m128i);
    let c = _mm_xor_si128(a, b);
    let mut output = [0u8;16];
    _mm_storeu_si128(output.as_mut_ptr() as *mut __m128i, c);
    output

}


fn array16_from_slice(slice: &[u8]) -> [u8;16] {
    if slice.len() != 16 {
        panic!("Slice is not 16 bytes long\nSlice: {:x?}", slice);
    }
    let mut output = [0u8;16];
    let mut i = 0;
    while i < 16 {
        output[i] = slice[i];
        i += 1;
    }
    output
}


fn pkcs_pad16(a: &[u8]) -> Vec<u8> {
    let mut output = Vec::new();
    if a.len()%16 == 0 {
        output.extend_from_slice(a);
    } else {
        output.extend_from_slice(a);
        let mut i = 0;
        let pad: u8 = 16-((a.len()%16) as u8);
        while i < pad {
            output.push(pad);
            i += 1;
        }
    }
    assert!(output.len()%16 == 0);
    output
}

fn pkcs_unpad(mut a: Vec<u8>) -> Vec<u8> {
    let num: usize = a[a.len()-1] as usize;
    
    for _ in 0..num {
        a.pop();
    }
    a

}


fn ROTWORD(a: u32) -> u32 {
    let a = a.to_be_bytes();
    let output: u32 = word_from_bytes([a[1], a[2], a[3], a[0]]);
    output
}

fn word_from_bytes(bytes: [u8;4]) -> u32 {
    ((bytes[0] as u32) << 24) |
    ((bytes[1] as u32) << 16) |
    ((bytes[2] as u32) << 8)  |
    (bytes[3] as u32)
}

fn SUBWORD(a: u32) -> u32 {
    let a = a.to_be_bytes();
    let s0 = (((a[0]&0xF0) >> 4) as usize, (a[0] & 0x0F) as usize);
    let s1 = (((a[1]&0xF0) >> 4) as usize, (a[1] & 0x0F) as usize);
    let s2 = (((a[2]&0xF0) >> 4) as usize, (a[2] & 0x0F) as usize);
    let s3 = (((a[3]&0xF0) >> 4) as usize, (a[3] & 0x0F) as usize);
    //println!("s: {:x}{:x},{:x}{:x},{:x}{:x},{:x}{:x}", s0.0, s0.1, s1.0, s1.1, s2.0, s2.1, s3.0, s3.1);
    let output: u32 = word_from_bytes([SBOX[s0.0][s0.1], SBOX[s1.0][s1.1], SBOX[s2.0][s2.1], SBOX[s3.0][s3.1]]);
    //println!("SBOX: {:x},{:x},{:x},{:x},", SBOX[s0.0][s0.1], SBOX[s1.0][s1.1], SBOX[s2.0][s2.1], SBOX[s3.0][s3.1]);

    output
}

pub fn expand_key(key: &[u8;16]) -> [u8; 176] {
    let mut i = 0;
    let Nk = 4;
    let Nr = 10;
    let mut w: [u32;44] = [0;44];
    let key = key.clone();
    while i <= Nk - 1 {
        w[i] = word_from_bytes([key[4*i], key[4 * i+1], key[4 * i+2], key[4 * i+3]]);
        //println!("w[{}]: {:x}", i, w[i]);
        //println!("{:x}", w[i]);
        i = i+1;
    }
    while i <= 4 * Nr + 3 {
        let mut temp = w[i - 1];
        if i % Nk == 0 {
            //println!("temp{}: {:x}", i, temp);
            //let rot = ROTWORD(temp);
            //println!("rot{}: {:x}", i, rot);
            temp = SUBWORD(ROTWORD(temp)) ^ RCON[i/Nk - 1];
            //println!("sub{}: {:x}",i, temp);
        }else if (Nk > 6) && (i%Nk == 4) {
            temp = SUBWORD(temp);
        }
        w[i] = w[i - Nk] ^ temp;
        //println!("{:x}", w[i]);
        i = i+1;
    }
    let mut output: [u8; 176] = [0;176];
    let mut index = 0;
    for word in w {
        let temp = word.to_be_bytes();
        for byte in temp {
            output[index] = byte;
            index += 1;
        }
    }
    output

}



// AES128 encryption
fn encrypt_one_block_128(plaintext: [u8;16], key: &[u8;16]) -> [u8;16] {
    // println!("plaintext at start: {:x?}", plaintext);
    let exp_key = expand_key(key);
    let mut round_keys: [__m128i;11] = unsafe { [_mm_setzero_si128();11] };
    let mut i = 0;
    // putting the expanded key into an array of 128bit words
    while i < exp_key.len()-15 {
        let temp = array16_from_slice(&exp_key[i..i+16]);
        let round_key = unsafe { _mm_loadu_si128(temp.as_ptr() as *const __m128i) };
        
        round_keys[i/16] = round_key;
        i += 16;
    }

    // The main body of the AES128 algorithm starts here
    let plaintext = unsafe { _mm_loadu_si128(plaintext.as_ptr() as *const __m128i) };
    
    // { // This is a SIMD print statement
    //     let mut value: [u8;16] = [0;16];
    //     unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, plaintext) };
    //     println!("Plaintext as _m128i: {:x?}", value);
    // }

    let mut ciphertext = unsafe { _mm_xor_si128(plaintext, round_keys[0]) };
    // { // This is a SIMD print statement
    //     let mut value: [u8;16] = [0;16];
    //     unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, ciphertext) };
    //     println!("state0: {:x?}", value);
    // }
    
    
    let mut i = 1;
    while i < 10 {
        ciphertext = unsafe { _mm_aesenc_si128(ciphertext, round_keys[i]) };
        // { // This is a SIMD print statement
        //     let mut value: [u8;16] = [0;16];
        //     unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, ciphertext) };
        //     println!("state{i}: {:x?}", value);
        // }
        
        i += 1;
    }
    ciphertext = unsafe { _mm_aesenclast_si128(ciphertext, round_keys[10]) };
    // { // This is a SIMD print statement
    //     let mut value: [u8;16] = [0;16];
    //     unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, ciphertext) };
    //     println!("state10: {:x?}", value);
    // }
   
    let mut value: [u8;16] = [0;16];
    unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, ciphertext) };

    value
}


fn decrypt_one_block_128(ciphertext: [u8;16], key: &[u8;16]) -> [u8;16] {
    let exp_key = expand_key(key);
    let mut round_keys: [__m128i;11] = unsafe { [_mm_setzero_si128();11] };
    let mut i = 0;
    // putting the expanded key into an array of 128bit words
    while i < exp_key.len()-15 {
        let temp = array16_from_slice(&exp_key[i..i+16]);
        // println!("Round key {i}: {:x?}", temp);
        let round_key = unsafe { _mm_loadu_si128(temp.as_ptr() as *const __m128i) };
        
        round_keys[i/16] = round_key;
        i += 16;
    }

    // The main body of the AES128 algorithm
    let ciphertext = unsafe { _mm_loadu_si128(ciphertext.as_ptr() as *const __m128i) };
   
    let mut plaintext = unsafe { _mm_xor_si128(ciphertext, round_keys[10]) };
    // { // This is a SIMD print statement
    //     let mut value: [u8;16] = [0;16];
    //     unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, plaintext) };
    //     println!("state10: {:x?}", value);
    // }
    // println!("Going into loop");
    
    for i in 1..10 {
        let round_key = unsafe { _mm_aesimc_si128(round_keys[10-i]) };
        plaintext = unsafe { _mm_aesdec_si128(plaintext, round_key) };
        // {// This is a SIMD print statement
        //     let mut value: [u8;16] = [0;16];
        //     unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, plaintext) };
        //     println!("state{}: {:x?}", 10-i, value);
        // }
        
    }
    plaintext = unsafe { _mm_aesdeclast_si128(plaintext, round_keys[0]) };
    // {// This is a SIMD print statement
    //     let mut value: [u8;16] = [0;16];
    //     unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, plaintext) };
    //     println!("state0: {:x?}", value);
    // }
   
    let mut value: [u8;16] = [0;16];
    unsafe { _mm_storeu_si128(value.as_mut_ptr() as *mut __m128i, plaintext) };
    value
}


pub fn encrypt_128(data: &[u8], key: &[u8;16]) -> Vec<u8> {

    let data = pkcs_pad16(data);
    let mut output: Vec<u8> = Vec::new();
    
    let mut i = 0;
    while i < data.len() {
        let temp = encrypt_one_block_128(array16_from_slice(&data[i..i+16]), key);
        output.extend_from_slice(&temp);
        i += 16;
    }

    output

}


pub fn decrypt_128(data: &[u8], key: &[u8;16]) -> Vec<u8> {
    let data = pkcs_pad16(data);
    let mut output: Vec<u8> = Vec::with_capacity(data.len() + 1);
    
    let mut i = 0;
    while i < data.len() {
        let temp = decrypt_one_block_128(array16_from_slice(&data[i..i+16]), key);
        output.extend_from_slice(&temp);
        i += 16;
    }

    pkcs_unpad(output)
}


#[cfg(test)]
mod tests {
    use std::time::Instant;

    use crate::networking_utilities::bytes_to_str;

    use super::*;

    #[test]
    fn test_generic_encryption_decryption() {
        let Plaintext = "This is some plaintext......!";
        let Key: [u8;16] = [0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c];
        let ciphertext = encrypt_128(Plaintext.as_bytes(), &Key);
        let decrypted_ciphertext = decrypt_128(&ciphertext, &Key);
        let text = String::from_utf8(decrypted_ciphertext).unwrap();
        println!("Plaintext: {}", Plaintext);
        println!("Text: {}", text);
        assert_eq!(Plaintext, text);
    }

    #[test]
    fn test_array_align() {
        let mut vec:Vec<u8> = Vec::new();
        for _ in 0..16 {
            vec.push(0xFF);
            let aligned_vec = pkcs_pad16(&vec);
            println!("vec.len(): {}", vec.len());
            assert!(aligned_vec.len() == 16);
        }
    }


    #[test]
    fn test_encrypt_then_decrypt_one_block() {
        let Plaintext: [u8;16] = [0x32, 0x43, 0xf6, 0xa8, 0x88, 0x5a, 0x30, 0x8d, 0x31, 0x31, 0x98, 0xa2, 0xe0, 0x37, 0x07, 0x34];
        let Key: [u8;16] = [0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c];
        println!("Encrypting...");
        let ciphertext = encrypt_one_block_128(Plaintext, &Key);
        println!("Decrypting...");
        let plaintext = decrypt_one_block_128(ciphertext, &Key);
        println!("Plaintext: {:x?}", Plaintext);
        println!("Encrypted: {:x?}", ciphertext);
        println!("Decrypted: {:x?}", plaintext);
        assert_eq!(Plaintext, plaintext)
    }

    #[test]
    fn easy_test_unaligned_block() {
        let Plaintext: [u8;32] = [
            0x32, 0x43, 0xf6, 0xa8, 
            0x88, 0x5a, 0x30, 0x8d, 
            0x31, 0x31, 0x98, 0xa2, 
            0xe0, 0x37, 0x07, 0x34,
            0x32, 0x43, 0xf6, 0xa8, 
            0x88, 0x5a, 0x30, 0x8d, 
            0x31, 0x31, 0x98, 0xa2, 
            0xe0, 0x37, 0x07, 0x34,
            ];
        let Key: [u8;16] = [0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c];
        let Ciphertext = Vec::from([
                                   0x39, 0x25, 0x84, 0x1d, 
                                   0x02, 0xdc, 0x09, 0x0fb,
                                   0xdc, 0x11, 0x85, 0x97,
                                   0x19, 0x6a, 0x0b, 0x32,
                                   0x39, 0x25, 0x84, 0x1d, 
                                   0x02, 0xdc, 0x09, 0x0fb,
                                   0xdc, 0x11, 0x85, 0x97,
                                   0x19, 0x6a, 0x0b, 0x32,

                                  ]);
        assert_eq!(encrypt_128(&Plaintext, &Key), Ciphertext);
    }

    #[test]
    fn test_double_block() {
        let Plaintext: [u8;32] = [
            0x32, 0x43, 0xf6, 0xa8, 
            0x88, 0x5a, 0x30, 0x8d, 
            0x31, 0x31, 0x98, 0xa2, 
            0xe0, 0x37, 0x07, 0x34,
            0x32, 0x43, 0xf6, 0xa8, 
            0x88, 0x5a, 0x30, 0x8d, 
            0x31, 0x31, 0x98, 0xa2, 
            0xe0, 0x37, 0x07, 0x34,
            
            ];
        let Key: [u8;16] = [0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c];
        let Ciphertext = Vec::from([
                                   0x39, 0x25, 0x84, 0x1d, 
                                   0x02, 0xdc, 0x09, 0x0fb,
                                   0xdc, 0x11, 0x85, 0x97,
                                   0x19, 0x6a, 0x0b, 0x32,
                                   0x39, 0x25, 0x84, 0x1d, 
                                   0x02, 0xdc, 0x09, 0x0fb,
                                   0xdc, 0x11, 0x85, 0x97,
                                   0x19, 0x6a, 0x0b, 0x32,
                                  ]);
        assert_eq!(encrypt_128(&Plaintext, &Key), Ciphertext);
    }

    #[test]
    fn test_encrypt_one_block() {
        let Plaintext: [u8;16] = [0x32, 0x43, 0xf6, 0xa8, 0x88, 0x5a, 0x30, 0x8d, 0x31, 0x31, 0x98, 0xa2, 0xe0, 0x37, 0x07, 0x34];
        let Key: [u8;16] = [0x2b, 0x7e, 0x15, 0x16, 0x28, 0xae, 0xd2, 0xa6, 0xab, 0xf7, 0x15, 0x88, 0x09, 0xcf, 0x4f, 0x3c];
        let Ciphertext: [u8;16] = [0x39, 0x25, 0x84, 0x1d, 
                                   0x02, 0xdc, 0x09, 0x0fb,
                                   0xdc, 0x11, 0x85, 0x97,
                                   0x19, 0x6a, 0x0b, 0x32
                                  ];
        assert_eq!(encrypt_one_block_128(Plaintext, &Key), Ciphertext);
    }

    #[test]
    fn test_key_expansion() {
        //these keys are from the official NIST AES standard
        let key: [u8; 16] = [
            0x2b,0x7e,0x15,0x16,
            0x28,0xae,0xd2,0xa6,
            0xab,0xf7,0x15,0x88,
            0x09,0xcf,0x4f,0x3c,
        ];
        let official_expanded_key: [u8; 176] = [
            0x2b,0x7e,0x15,0x16,
            0x28,0xae,0xd2,0xa6,
            0xab,0xf7,0x15,0x88,
            0x09,0xcf,0x4f,0x3c,

            0xa0,0xfa,0xfe,0x17,
            0x88,0x54,0x2c,0xb1,
            0x23,0xa3,0x39,0x39,
            0x2a,0x6c,0x76,0x05,

            0xf2,0xc2,0x95,0xf2,
            0x7a,0x96,0xb9,0x43,
            0x59,0x35,0x80,0x7a,
            0x73,0x59,0xf6,0x7f,

            0x3d,0x80,0x47,0x7d,
            0x47,0x16,0xfe,0x3e,
            0x1e,0x23,0x7e,0x44,
            0x6d,0x7a,0x88,0x3b,

            0xef,0x44,0xa5,0x41,
            0xa8,0x52,0x5b,0x7f,
            0xb6,0x71,0x25,0x3b,
            0xdb,0x0b,0xad,0x00,

            0xd4,0xd1,0xc6,0xf8,
            0x7c,0x83,0x9d,0x87,
            0xca,0xf2,0xb8,0xbc,
            0x11,0xf9,0x15,0xbc,

            0x6d,0x88,0xa3,0x7a,
            0x11,0x0b,0x3e,0xfd,
            0xdb,0xf9,0x86,0x41,
            0xca,0x00,0x93,0xfd,

            0x4e,0x54,0xf7,0x0e,
            0x5f,0x5f,0xc9,0xf3,
            0x84,0xa6,0x4f,0xb2,
            0x4e,0xa6,0xdc,0x4f,

            0xea,0xd2,0x73,0x21,
            0xb5,0x8d,0xba,0xd2,
            0x31,0x2b,0xf5,0x60,
            0x7f,0x8d,0x29,0x2f,

            0xac,0x77,0x66,0xf3,
            0x19,0xfa,0xdc,0x21,
            0x28,0xd1,0x29,0x41,
            0x57,0x5c,0x00,0x6e,

            0xd0,0x14,0xf9,0xa8,
            0xc9,0xee,0x25,0x89,
            0xe1,0x3f,0x0c,0xc8,
            0xb6,0x63,0x0c,0xa6,

        ];
        let ekey = expand_key(&key);
        
        assert_eq!(official_expanded_key, ekey);

    }

}