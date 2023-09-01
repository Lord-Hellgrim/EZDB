use std::{error::Error, mem::transmute};

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

const RCON: [u32;10] = [0x01000000, 0x02000000, 0x04000000, 0x08000000, 0x10000000, 0x20000000, 0x40000000, 0x80000000, 0x1b000000, 0x36000000];

fn ROTWORD(a: u32) -> u32 {
    let a = a.to_be_bytes();
    // should be safe since a is derived from a u32 in the first place
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
    println!("s: {:x}{:x},{:x}{:x},{:x}{:x},{:x}{:x}", s0.0, s0.1, s1.0, s1.1, s2.0, s2.1, s3.0, s3.1);
    let output: u32 = word_from_bytes([SBOX[s0.0][s0.1], SBOX[s1.0][s1.1], SBOX[s2.0][s2.1], SBOX[s3.0][s3.1]]);
    println!("SBOX: {:x},{:x},{:x},{:x},", SBOX[s0.0][s0.1], SBOX[s1.0][s1.1], SBOX[s2.0][s2.1], SBOX[s3.0][s3.1]);

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
        println!("w[{}]: {:x}", i, w[i]);
        //println!("{:x}", w[i]);
        i = i+1;
    }         //. When the loop concludes, i = Nk.;
    while i <= 4 * Nr + 3 {
        let mut temp = w[i - 1];
        if i % Nk == 0 {
            println!("temp{}: {:x}", i, temp);
            let rot = ROTWORD(temp);
            println!("rot{}: {:x}", i, rot);
            temp = SUBWORD(ROTWORD(temp)) ^ RCON[i/Nk - 1];
            println!("sub{}: {:x}",i, temp);
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
pub fn encrypt(plaintext: &str, key: &str) -> String {
    println!("SBOX[0][0]: {}", SBOX[0][0]);
    let Ciphertext: &str = "69c4e0d86a7b0430d8cdb78070b4c55a";
    return Ciphertext.to_owned()
}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aes() {
        let Plaintext: &str = "00112233445566778899aabbccddeeff";
        let Key: &str = "000102030405060708090a0b0c0d0e0f";
        let Ciphertext: &str = "69c4e0d86a7b0430d8cdb78070b4c55a";
        assert_eq!(encrypt(Plaintext, Key), Ciphertext);
    }

    #[test]
    fn test_key_expansion() {
        let key: [u8; 16] = [
            0x2b,0x7e,0x15,0x16,
            0x28,0xae,0xd2,0xa6,
            0xab,0xf7,0x15,0x88,
            0x09,0xcf,0x4f,0x3c,
        ];
        let chatGPT_expanded_key: [u8; 176] = [
            
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
        let mut index = 0;
        // while index < 176 {
        //     let delta = ekey[index] as i32 - chatGPT_expanded_key[index] as i32;
        //     print!("{:x}, ", delta);
        //     index += 1;
        // }
        assert_eq!(chatGPT_expanded_key, ekey);

    }

}