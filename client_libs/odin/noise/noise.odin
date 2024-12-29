package noise

import "core:crypto"
import "core:crypto/x25519"
import "core:crypto/aead"
import "core:crypto/sha2"

import "core:slice"

import "core:simd"

import "core:unicode/utf8"

/// A constant specifying the size in bytes of public keys and DH outputs. For security reasons, DHLEN must be 32 or greater.
DHLEN : uintptr :  32;
/// A constant specifying the size in bytes of the hash output. Must be 32 or 64.
HASHLEN: uintptr : 32;

/// A constant specifying the size in bytes that the hash function uses internally to divide its input for iterative processing. 
/// This is needed to use the hash function with HMAC (BLOCKLEN is B in [3]).
BLOCKLEN: uintptr : 128;
/// The HMAC padding strings
IPAD: [BLOCKLEN]u8 : 0x36
OPAD: [BLOCKLEN]u8 : 0x5c

MAX_PACKET_SIZE: uintptr : 65535;

PROTOCOL_NAME :: "Noise_NK_25519_AESGCM_SHA512";


KeyPair :: struct {
    public_key: [HASHLEN]u8,
    private_key: [HASHLEN]u8,
}

keypair_empty :: proc() -> KeyPair {
    public : [HASHLEN]u8
    private: [HASHLEN]u8
    return KeyPair {
        public_key = public, 
        private_key = private,
    }
    
}

keypair_random :: proc() -> KeyPair {
    private_key: [HASHLEN]u8;
    crypto.rand_bytes(private_key[:])

    public_key : [HASHLEN]u8;
    x25519.scalarmult_basepoint(public_key[:], private_key[:])

    return KeyPair {
        private_key = private_key,
        public_key = public_key,
    }
}


NoiseError :: enum {
    NoError,
    WrongState,
    Io,
}



/// Generates a new Diffie-Hellman key pair. A DH key pair consists of public_key and private_key elements. 
/// A public_key represents an encoding of a DH public key into a byte sequence of length DHLEN. 
/// The public_key encoding details are specific to each set of DH functions.
GENERATE_KEYPAIR :: proc() -> KeyPair {
    return keypair_random()
}


/// Performs a Diffie-Hellman calculation between the private key in key_pair and the public_key 
/// and returns an output sequence of bytes of length DHLEN. 
/// For security, the Gap-DH problem based on this function must be unsolvable by any practical cryptanalytic adversary [2].

/// The public_key either encodes some value which is a generator in a large prime-order group 
/// (which value may have multiple equivalent encodings), or is an invalid value. 
/// Implementations must handle invalid public keys either by returning some output which is purely a function of the public key 
/// and does not depend on the private key, or by signaling an error to the caller. 
/// The DH function may define more specific rules for handling invalid values.
DH :: proc(key_pair: KeyPair, public_key: [HASHLEN]u8) -> [32]u8 {
    key_pair := key_pair
    public_key := public_key
    assert(key_pair.private_key != 0 && key_pair.public_key != 0);
    x25519.scalarmult_basepoint(public_key[:], key_pair.private_key[:])
    shared_secret : [32]u8
    x25519.scalarmult(shared_secret[:], key_pair.private_key[:], public_key[:])
    return shared_secret
} 


/// Encrypts plaintext using the cipher key k of 32 bytes and an 8-byte unsigned integer nonce n which must be unique for the key k. 
/// Returns the ciphertext. Encryption must be done with an "AEAD" encryption mode with the associated data ad 
/// (using the terminology from [1]) and returns a ciphertext that is the same size as the plaintext plus 16 bytes for authentication data. 
/// The entire ciphertext must be indistinguishable from random if the key is secret 
/// (note that this is an additional requirement that isn't necessarily met by all AEAD schemes).
ENCRYPT :: proc(k: [HASHLEN]u8, n: u64, ad: []u8, plaintext: []u8) -> ([]u8, NoiseError) {

    k := k
    tag : [16]u8
    
    ciphertext_buffer := make_slice([]byte, len(plaintext)+28, context.temp_allocator)

    ctx : aead.Context
    iv := nonce_from_u64(n)
    crypto.rand_bytes(iv[:])
    copy_slice(ciphertext_buffer[0:12], iv[:])

    aead.init(&ctx, aead.Algorithm.AES_GCM_256, k[:])
    aead.seal_ctx(&ctx, ciphertext_buffer[12:len(ciphertext_buffer)-16], tag[:], iv[:], ad, plaintext)

    copy_slice(ciphertext_buffer[len(ciphertext_buffer)-16:], tag[:])

    return ciphertext_buffer, .NoError
}


/// Decrypts ciphertext using a cipher key k of 32 bytes, an 8-byte unsigned integer nonce n,
/// and associated data ad. Returns the plaintext, unless authentication fails, 
/// in which case an error is signaled to the caller.
DECRYPT :: proc(k: [HASHLEN]u8, n: u64, ad: []u8, ciphertext: []u8) -> ([]u8, NoiseError) {
    
    k := k

    plaintext_buffer := make_slice([]byte, len(ciphertext) - 28)
    
    ctx : aead.Context
    iv := nonce_from_u64(n)
    copy_slice(iv[:], ciphertext[0:12])
    tag : [16]u8
    copy_slice(tag[:], ciphertext[len(ciphertext)-16:])

    aead.init(&ctx, aead.Algorithm.AES_GCM_256, k[:])
    if aead.open_ctx(&ctx, plaintext_buffer, iv[:], ad, ciphertext[12:len(ciphertext)-16], tag[:]) {
        return plaintext_buffer, .NoError
    } else {
        return plaintext_buffer, .WrongState
    }
}

/// Hashes some arbitrary-length data with a collision-resistant cryptographic hash function and returns an output of HASHLEN bytes.
HASH :: proc(data: []u8) -> [HASHLEN]u8 {
    ctx : sha2.Context_512
    sha2.init_512(&ctx)

    sha2.update(&ctx, data)
    hash : [HASHLEN]u8
    sha2.final(&ctx, hash[:])

    return hash
}


/// Returns a new 32-byte cipher key as a pseudorandom function of k. If this function is not specifically defined for some set of cipher functions, 
/// then it defaults to returning the first 32 bytes from ENCRYPT(k,    maxnonce, zerolen, zeros), 
/// where maxnonce equals 264-1, zerolen is a zero-length byte sequence, and zeros is a sequence of 32 bytes filled with zeros.
REKEY :: proc(k: [HASHLEN]u8) -> [HASHLEN]u8 {
    zeros : [HASHLEN]u8
    n :u64 = 0xFFFFFFFFFFFFFFFF
    ENCRYPT(k, n, nil, zeros[:])
    new_key : [HASHLEN]u8
    copy(new_key[:], zeros[:])
    return new_key
}

HMAC_HASH :: proc(K: [HASHLEN]u8, text: []u8) -> [HASHLEN]u8 {
    K := K
    new_K := zeropad128(K[:])
    temp1 := array_xor(new_K, IPAD)
    temp2 := array_xor(new_K, OPAD)

    inner: [HASHLEN]u8 = HASH(concat_bytes(temp1[:], text));
    outer: [HASHLEN]u8 = HASH(concat_bytes(temp2[:], inner[:]));
    return outer
}

/// Takes a chaining_key byte sequence of length HASHLEN, and an input_key_material byte sequence with length either zero bytes, 
/// 32 bytes, or DHLEN bytes. Returns a pair or triple of byte sequences each of length HASHLEN, depending on whether num_outputs is two or three:
///  - Sets temp_key = HMAC-HASH(chaining_key, input_key_material).
///  - Sets output1 = HMAC-HASH(temp_key, byte(0x01)).
///  - Sets output2 = HMAC-HASH(temp_key, output1 || byte(0x02)).
///  - If num_outputs == 2 then returns the pair (output1, output2).
///  - Sets output3 = HMAC-HASH(temp_key, output2 || byte(0x03)).
///  - Returns the triple (output1, output2, output3).
///  - Note that temp_key, output1, output2, and output3 are all HASHLEN bytes in length. Also note that the HKDF() function is simply HKDF from [4] with the chaining_key as HKDF salt, and zero-length HKDF info.
HKDF :: proc(chaining_key: [HASHLEN]u8, input_key_material: []u8) -> ([HASHLEN]u8, [HASHLEN]u8, [HASHLEN]u8) {
    assert(len(input_key_material) == 0 || len(input_key_material) == 32);
    temp_key := HMAC_HASH(chaining_key, input_key_material);
    output1 := HMAC_HASH(temp_key, {0x01});
    output2 := HMAC_HASH(temp_key, concat_bytes(output1[:], {0x02}));
    output3 := HMAC_HASH(temp_key, concat_bytes(output2[:], {0x03}));

    return output1, output2, output3
} 


Token :: enum {
    e,
    s,
    ee,
    es,
    se,
    ss,
}

CipherState :: struct {
    k: [HASHLEN]u8,
    n: u64,
}

SymmetricState :: struct {
    cipherstate: CipherState,
    ck: [HASHLEN]u8,
    h: [HASHLEN]u8,
}

HandshakeState :: struct {
    symmetricstate: SymmetricState,
    s: KeyPair,
    e: KeyPair, 
    rs: [HASHLEN]u8,
    re: [HASHLEN]u8,
    initiator: bool,
    message_patterns: [][]Token
}


/// Sets k = key. Sets n = 0.
cipherstate_InitializeKey :: proc(key: [HASHLEN]u8) -> CipherState {
    return CipherState {
        k = key,
        n = 0
    }
}

/// Returns true if k is non-empty, false otherwise.
cipherstate_HasKey :: proc(self: ^CipherState) -> bool {
    zeroslice : [HASHLEN]u8
    if slice.equal(self.k[:], zeroslice[:]) {
        return false
    } else {
        return true
    }
}

/// Sets n = nonce. This function is used for handling out-of-order transport messages, as described in Section 11.4.
cipherstate_SetNonce :: proc(self: ^CipherState, nonce: u64) {
    self.n = nonce
}

///If k is non-empty returns ENCRYPT(k, n++, ad, plaintext). Otherwise returns plaintext.
cipherstate_EncryptWithAd :: proc(self: ^CipherState, ad: []u8, plaintext: []u8) -> []u8 {
    if cipherstate_HasKey(self) {
        temp, encrypt_error := ENCRYPT(self.k, self.n, ad, plaintext)
        self.n += 1;
        return temp
    } else {
        return plaintext
    }
}

/// If k is non-empty returns DECRYPT(k, n++, ad, ciphertext). Otherwise returns ciphertext. 
/// If an authentication failure occurs in DECRYPT() then n is not incremented and an error is signaled to the caller.
cipherstate_DecryptWithAd :: proc(self: ^CipherState, ad: []u8, ciphertext: []u8) -> ([]u8, NoiseError) {
    if cipherstate_HasKey(self) {
        plaintext, decrypt_error := DECRYPT(self.k, self.n, ad, ciphertext)
        self.n += 1;
        return plaintext, decrypt_error
    } else {
        return ciphertext, .NoError
    }
}

/// Sets k = REKEY(k).
cipherstate_Rekey :: proc(self: ^CipherState) {
    if cipherstate_HasKey(self) {
        self.k = REKEY(self.k)
    }
}

// impl SymmetricState {

/// : Takes an arbitrary-length protocol_name byte sequence (see Section 8). Executes the following steps:

/// If protocol_name is less than or equal to HASHLEN bytes in length, sets h equal to protocol_name with zero bytes appended to make HASHLEN bytes. 
/// Otherwise sets h = HASH(protocol_name).

/// Sets ck = h.

/// Calls InitializeKey(empty).
symmetricstate_InitializeSymmetric :: proc(protocol_name: string) -> SymmetricState {
    zeroslice : [HASHLEN]u8
    if len(protocol_name) < 32 {
        protocol_name_bytes := zeropad32(str_to_slice(protocol_name))
        h := HASH(protocol_name_bytes[:]);
        cipherstate := cipherstate_InitializeKey(zeroslice);
        return SymmetricState {
            cipherstate = cipherstate,
            ck = h,
            h = h,
        }
    } else {
        h := HASH(str_to_slice(protocol_name));
        cipherstate := cipherstate_InitializeKey(zeroslice);
        return SymmetricState {
            cipherstate = cipherstate,
            ck = h,
            h = h,
        }
    }
}

/// Sets h = HASH(h || data).
symmetricstate_MixHash :: proc(self: ^SymmetricState, data: []u8) {
    self.h = HASH(concat_bytes(self.h[:], data))
}

///     : Executes the following steps:

/// Sets ck, temp_k = HKDF(ck, input_key_material, 2).
/// If HASHLEN is 64, then truncates temp_k to 32 bytes.
/// Calls InitializeKey(temp_k).
symmetricstate_MixKey :: proc(self: ^SymmetricState, input_key_material: [HASHLEN]u8) {
    input_key_material := input_key_material
    ck, temp_k, _ := HKDF(self.ck, input_key_material[:])
    self.ck = ck
    self.cipherstate = cipherstate_InitializeKey(temp_k)
}

/// This function is used for handling pre-shared symmetric keys, as described in Section 9. It executes the following steps:

/// Sets ck, temp_h, temp_k = HKDF(ck, input_key_material, 3).
/// Calls MixHash(temp_h).
/// If HASHLEN is 64, then truncates temp_k to 32 bytes.
/// Calls InitializeKey(temp_k).
MixKeyAndHash :: proc(self: ^SymmetricState, input_key_material: [HASHLEN]u8) {
    input_key_material := input_key_material
    ck, temp_h, temp_k := HKDF(self.ck, input_key_material[:])
    self.ck = ck
    symmetricstate_MixHash(self, temp_h[:])
    self.cipherstate = cipherstate_InitializeKey(temp_k);
}

/// Returns h. This function should only be called at the end of a handshake, i.e. after the Split() function has been called. 
/// This function is used for channel binding, as described in Section 11.2
symmetricstate_GetHandshakeHash :: proc(self: ^SymmetricState) -> [HASHLEN]u8 {
    return self.h
}

/// Sets ciphertext = EncryptWithAd(h, plaintext), calls MixHash(ciphertext), and returns ciphertext. 
/// Note that if k is empty, the EncryptWithAd() call will set ciphertext equal to plaintext.
EncryptAndHash :: proc(self:  ^SymmetricState, plaintext: []u8) -> []u8{
    ciphertext := cipherstate_EncryptWithAd(&self.cipherstate, self.h[:], plaintext)
    symmetricstate_MixHash(self, ciphertext)
    return ciphertext
}

/// Sets plaintext = DecryptWithAd(h, ciphertext), calls MixHash(ciphertext), and returns plaintext. 
/// Note that if k is empty, the DecryptWithAd() call will set plaintext equal to ciphertext.
DecryptAndHash :: proc(self:  ^SymmetricState, ciphertext: []u8) -> ([]u8, NoiseError) {
    result, decrypt_error := cipherstate_DecryptWithAd(&self.cipherstate, self.h[:], ciphertext)
    symmetricstate_MixHash(self, ciphertext)
    return result, .NoError
}

/// Returns a pair of CipherState objects for encrypting transport messages. Executes the following steps, where zerolen is a zero-length byte sequence:
/// Sets temp_k1, temp_k2 = HKDF(ck, zerolen, 2).
/// If HASHLEN is 64, then truncates temp_k1 and temp_k2 to 32 bytes.
/// Creates two new CipherState objects c1 and c2.
/// Calls c1.InitializeKey(temp_k1) and c2.InitializeKey(temp_k2).
/// Returns the pair (c1, c2).
Split :: proc(self: ^SymmetricState) -> (CipherState, CipherState) {
    temp_k1, temp_k2, _ := HKDF(self.ck, nil)
    c1 := cipherstate_InitializeKey(temp_k1)
    c2 := cipherstate_InitializeKey(temp_k2)
    return c1, c2
}


// impl HandshakeState {
/// : Takes a valid handshake_pattern (see Section 7) and an initiator boolean specifying this party's role as either initiator or responder.

/// Takes a prologue byte sequence which may be zero-length, or which may contain context information that both parties want to confirm is identical 
/// (see Section 6).

/// Takes a set of DH key pairs (s, e) and public keys (rs, re) for initializing local variables, any of which may be empty. 
/// Public keys are only passed in if the handshake_pattern uses pre-messages (see Section 7). The ephemeral values (e, re) are typically left empty, 
/// since they are created and exchanged during the handshake; but there are exceptions (see Section 10).

/// Performs the following steps:

/// Derives a protocol_name byte sequence by combining the names for the handshake pattern and crypto functions, as specified in Section 8. 
/// Calls InitializeSymmetric(protocol_name).

/// Calls MixHash(prologue).

/// Sets the initiator, s, e, rs, and re variables to the corresponding arguments.

/// Calls MixHash() once for each public key listed in the pre-messages from handshake_pattern, 
/// with the specified public key as input (see Section 7 for an explanation of pre-messages). 
/// If both initiator and responder have pre-messages, the initiator's public keys are hashed first. 
/// If multiple public keys are listed in either party's pre-message, the public keys are hashed in the order that they are listed.

/// Sets message_patterns to the message patterns from handshake_pattern.
handshakestate_Initialize :: proc(
    initiator: bool,
    prologue: []u8,
    s: KeyPair,
    e: KeyPair,
    rs: [HASHLEN]u8,
    re: [HASHLEN]u8,
) -> HandshakeState {
    handshake_pattern_NK : [][]Token= {
        {.e},
        {.e, .ee, .s, .es},
        {.s, .se}
    };

    symmetricstate := symmetricstate_InitializeSymmetric(PROTOCOL_NAME)
    symmetricstate_MixHash(&symmetricstate, prologue)
    output := HandshakeState {
        symmetricstate = symmetricstate,
        s = s,
        e = e,
        rs = rs,
        re = re,
        initiator = initiator,
        message_patterns = handshake_pattern_NK,
    };

    return output
}

//     /// Takes a payload byte sequence which may be zero-length, and a message_buffer to write the output into. 
//     /// Performs the following steps, aborting if any EncryptAndHash() call returns an error:
    
//     /// Fetches and deletes the next message pattern from message_patterns, then sequentially processes each token from the message pattern:
    
//     /// For "e": Sets e (which must be empty) to GENERATE_KEYPAIR(). Appends e.public_key to the buffer. Calls MixHash(e.public_key).
    
//     /// For "s": Appends EncryptAndHash(s.public_key) to the buffer.
    
//     /// For "ee": Calls MixKey(DH(e, re)).
    
//     /// For "es": Calls MixKey(DH(e, rs)) if initiator, MixKey(DH(s, re)) if responder.
    
//     /// For "se": Calls MixKey(DH(s, re)) if initiator, MixKey(DH(e, rs)) if responder.
    
//     /// For "ss": Calls MixKey(DH(s, rs)).
    
//     /// Appends EncryptAndHash(payload) to the buffer.
    
//     /// If there are no more message patterns returns two new CipherState objects by calling Split().
//     pub fn WriteMessage(&mut self, mut message_buffer: impl Write) -> Result<Option<(CipherState, CipherState)>, NoiseError> {
//         let pattern = self.message_patterns.pop_front().expect(&format!("Should never be empty: Line: {}, Column: {}", line!(), column!()));
//         for token in pattern {
//             match token {
//                 Token::e => {
//                     self.e = GENERATE_KEYPAIR();
//                     message_buffer.write_all(&self.e.public_key.unwrap())?;
//                     self.symmetricstate.MixHash(&self.e.public_key.unwrap());
//                 },
//                 Token::s => {
//                     let temp = self.symmetricstate.EncryptAndHash(&self.s.public_key.unwrap());
//                     message_buffer.write_all(&temp)?;
//                 },
                
//                 Token::ee => self.symmetricstate.MixKey(DH(self.e.clone(), self.re.unwrap())),

//                 Token::es => {
//                     if self.initiator {
//                         self.symmetricstate.MixKey(DH(self.e.clone(), self.rs.unwrap()));
//                     } else {
//                         self.symmetricstate.MixKey(DH(self.s.clone(), self.re.unwrap()));
//                     }
//                 },
                
//                 Token::se => {
//                     if self.initiator {
//                         self.symmetricstate.MixKey(DH(self.s.clone(), self.re.unwrap()));
//                     } else {
//                         self.symmetricstate.MixKey(DH(self.e.clone(), self.rs.unwrap()));
                        
//                     }
//                 },
                
//                 Token::ss => self.symmetricstate.MixKey(DH(self.s.clone(), self.rs.unwrap())),
//             };
//         }
        
//         if self.message_patterns.is_empty() {
//             let (sender, receiver) = self.symmetricstate.Split();
//             Ok(Some((sender, receiver)))
//         } else {
//             Ok(None)
//         }
//     }

//     /// Takes a byte sequence containing a Noise handshake message, and a payload_buffer to write the message's plaintext payload into. 
//     /// Performs the following steps, aborting if any DecryptAndHash() call returns an error:
    
//     /// Fetches and deletes the next message pattern from message_patterns, then sequentially processes each token from the message pattern:
    
//     /// For "e": Sets re (which must be empty) to the next DHLEN bytes from the message. Calls MixHash(re.public_key).
    
//     /// For "s": Sets temp to the next DHLEN + 16 bytes of the message if HasKey() == True, or to the next DHLEN bytes otherwise. 
//     /// Sets rs (which must be empty) to DecryptAndHash(temp).
    
//     /// For "ee": Calls MixKey(DH(e, re)).
    
//     /// For "es": Calls MixKey(DH(e, rs)) if initiator, MixKey(DH(s, re)) if responder.
    
//     /// For "se": Calls MixKey(DH(s, re)) if initiator, MixKey(DH(e, rs)) if responder.
    
//     /// For "ss": Calls MixKey(DH(s, rs)).
    
//     /// Calls DecryptAndHash() on the remaining bytes of the message and stores the output into payload_buffer.
    
//     /// If there are no more message patterns returns two new CipherState objects by calling Split().
//     pub fn ReadMessage(&mut self, mut message: impl Read)  -> Result<Option<(CipherState, CipherState)>, NoiseError> {
//         let pattern = self.message_patterns.pop_front().expect(&format!("Should never be empty: Line: {}, Column: {}", line!(), column!()));
//         for token in pattern {
//             match token {
//                 Token::e => {
//                     let mut e = [0u8;DHLEN];
//                     message.read_exact(&mut e)?;
//                     if self.re.is_some() {
//                         return Err(NoiseError::WrongState);
//                     } else {
//                         self.re = Some(PublicKey::from(e));
//                         self.symmetricstate.MixHash(self.re.unwrap().as_bytes());
//                     }
//                 },
//                 Token::s => {
//                     if self.symmetricstate.cipherstate.HasKey() {
//                         let mut rs = [0u8;DHLEN+16];
//                         message.read_exact(&mut rs)?;
//                         let rs = array32_from_slice(&self.symmetricstate.DecryptAndHash(&rs).unwrap());
//                         if self.rs.is_none() {
//                             self.rs = Some(PublicKey::from(rs));
//                         } else {
//                             return Err(NoiseError::WrongState)
//                         }
//                     }
//                 },
                
//                 Token::ee => self.symmetricstate.MixKey(DH(self.e.clone(), self.re.unwrap())),

//                 Token::es => {
//                     if self.initiator {
//                         self.symmetricstate.MixKey(DH(self.e.clone(), self.rs.unwrap()));  
//                     } else {
//                         self.symmetricstate.MixKey(DH(self.s.clone(), self.re.unwrap()));
//                     }
//                 },
                
//                 Token::se => {
//                     if self.initiator {
//                         self.symmetricstate.MixKey(DH(self.s.clone(), self.re.unwrap()));  
//                     } else {
//                         self.symmetricstate.MixKey(DH(self.e.clone(), self.rs.unwrap()));
//                     }
//                 },
                
//                 Token::ss => self.symmetricstate.MixKey(DH(self.s.clone(), self.rs.unwrap())),
//             };
//         }
//         if self.message_patterns.is_empty() {
//             let (sender, receiver) = self.symmetricstate.Split();
//             Ok(Some((sender, receiver)))
//         } else {
//             Ok(None)
//         }
//     }
// }

// pub fn array32_from_slice(slice: &[u8]) -> [u8;32] {
//     let mut buf = [0u8;32];
//     buf.copy_from_slice(&slice[0..std::cmp::min(slice.len(), 32)]);
//     buf
// }

// pub struct Connection {
//     pub c1: CipherState,
//     pub c2: CipherState,
//     pub stream: TcpStream,
//     pub peer: String,
// }

// enum Cstate {
//     C1,
//     C2
// }

// impl Connection {
//     fn __send(&mut self, message: &[u8], state: Cstate) -> Result<(), NoiseError> {
//         let mut buffer = Vec::with_capacity(message.len() + 16);
//         let ciphertext = match state {
//             Cstate::C1 => self.c1.EncryptWithAd(&[], message),
//             Cstate::C2 => self.c2.EncryptWithAd(&[], message),
//         } ;
//         buffer.extend_from_slice(&ciphertext.len().to_le_bytes());
//         buffer.extend_from_slice(&ciphertext);
//         self.stream.write_all(&buffer)?;
//         self.stream.flush()?;
//         Ok(())
//     }

//     fn __receive(&mut self, state: Cstate) -> Result<Vec<u8>, NoiseError> {
//         let mut size_buffer: [u8; 8] = [0; 8];
//         self.stream.read_exact(&mut size_buffer)?;
    
//         let data_len = uintprt::from_le_bytes(size_buffer);
//         if data_len >  MAX_PACKET_SIZE {
//             return Err(NoiseError::Io)
//         }
//         let mut data = Vec::with_capacity(data_len);
//         let mut buffer = [0; 4096];
//         let mut total_read: uintprt = 0;
        
//         while total_read < data_len {
//             let to_read = std::cmp::min(4096, data_len - total_read);
//             let bytes_received = self.stream.read(&mut buffer[..to_read])?;
//             if bytes_received == 0 {
//                 return Err(NoiseError::Io);
//             }
//             data.extend_from_slice(&buffer[..bytes_received]);
//             total_read += bytes_received;
//         }

//         let data = match state {
//             Cstate::C1 => self.c1.DecryptWithAd(&[], &data)?,
//             Cstate::C2 => self.c2.DecryptWithAd(&[], &data)?,

//         };

//         Ok(data)
//     }

//     pub fn SEND_C1(&mut self, message: &[u8]) -> Result<(), NoiseError> {
//         self.__send(message, Cstate::C1)
//     }

//     pub fn SEND_C2(&mut self, message: &[u8]) -> Result<(), NoiseError> {
//         self.__send(message, Cstate::C2)
//     }

//     pub fn RECEIVE_C1(&mut self) -> Result<Vec<u8>, NoiseError> {
//         self.__receive(Cstate::C1)
//     }

//     pub fn RECEIVE_C2(&mut self) -> Result<Vec<u8>, NoiseError> {
//         self.__receive(Cstate::C2)
//     }

// }

// pub fn initiate_connection(address: &str) -> Result<Connection, NoiseError> {
//     let mut stream = TcpStream::connect(address)?;
//     let s = KeyPair::random();
//     let mut handshake_state = HandshakeState::Initialize(
//         true,
//         &[],
//         s,
//         KeyPair::empty(),
//         None,
//         None
//     );
    
//     // -> e
//     handshake_state.WriteMessage(&mut stream)?;

//     // <- e, ee, s, es
//     handshake_state.ReadMessage(&mut stream)?;

//     // -> s, se
//     let res = handshake_state.WriteMessage(&mut stream)?;

//     match res {
//         Some((c1, c2)) => {
//             Ok(
//                 Connection {
//                     c1,
//                     c2,
//                     stream,
//                     peer: String::new()
//                 }
//             )
//         },
//         None => Err(NoiseError::Io),
//     }
// }

// pub fn ESTABLISH_CONNECTION(mut stream: TcpStream, s: KeyPair) -> Result<Connection, NoiseError> {
//     let handshakestate = ESTABLISH_CONNECTION_STEP_1(&mut stream, s)?;

//     let handshakestate = ESTABLISH_CONNECTION_STEP_2(&mut stream, handshakestate)?;

//     let connection = ESTABLISH_CONNECTION_STEP_3(stream, handshakestate)?;

//     Ok(connection)

// }

// pub fn ESTABLISH_CONNECTION_STEP_1(stream: &mut TcpStream, s: KeyPair) -> Result<HandshakeState, NoiseError> {
//     let mut handshakestate = HandshakeState::Initialize(false, &[], s, KeyPair::empty(), None, None);

//     // <- e
//     handshakestate.ReadMessage(stream)?;

//     Ok(handshakestate)
// }

// pub fn ESTABLISH_CONNECTION_STEP_2(stream: &mut TcpStream, mut handshakestate: HandshakeState) -> Result<HandshakeState, NoiseError> {
    
//     handshakestate.WriteMessage(stream)?;

//     Ok(handshakestate)
// }

// pub fn ESTABLISH_CONNECTION_STEP_3(mut stream: TcpStream, mut handshakestate: HandshakeState) -> Result<Connection, NoiseError> {
//     // <- s, se
//     let res = handshakestate.ReadMessage(&mut stream)?;

//     println!("returning Connection!!");
//     match res {
//         Some((c1, c2)) => {
//             Ok(
//                 Connection {
//                     c1,
//                     c2,
//                     stream,
//                     peer: String::new()
//                 }
//             )
//         },
//         None => Err(NoiseError::Io),
//     }
// }



/// Creates a uintptr from a &[u8] of length 8. Panics if len is different than 8.
u64_from_le_slice :: proc(slice: []u8) -> u64 {
    assert(len(slice) >= 8)
    l: u64 = u64(slice[0]) | u64(slice[1])<<8 | u64(slice[2])<<16 | u64(slice[3])<<24 | u64(slice[4])<<32 | u64(slice[5])<<40 | u64(slice[6])<<48 | u64(slice[7])<<56
    return l
}

/// Creates a uintptr from a &[u8] of length 8. Panics if len is different than 8.
u64_from_be_slice :: proc(slice: []u8) -> u64 {
    assert(len(slice) >= 8)
    l: u64 = u64(slice[7]) | u64(slice[6])<<8 | u64(slice[5])<<16 | u64(slice[4])<<24 | u64(slice[3])<<32 | u64(slice[2])<<40 | u64(slice[1])<<48 | u64(slice[0])<<56
    return l
}


str_to_slice :: proc(s: string) -> []byte {
    return transmute([]byte)s
}

slice_to_str :: proc(s: []byte) -> (string, bool) {
    output := transmute(string)s
    if utf8.valid_string(output) {
        return output, true
    } else {
        return output, false
    }
}


to_be_bytes :: proc(n: u64) -> [8]u8 {
    n0 := u8(n >> 0)
    n1 := u8(n >> 8)
    n2 := u8(n >> 16)
    n3 := u8(n >> 24)
    n4 := u8(n >> 32)
    n5 := u8(n >> 40)
    n6 := u8(n >> 48)
    n7 := u8(n >> 56)
    return {n7, n6, n5, n4, n3, n2, n1, n0}
}

to_le_bytes :: proc(n: u64) -> [8]u8 {
    n0 := u8(n >> 0)
    n1 := u8(n >> 8)
    n2 := u8(n >> 16)
    n3 := u8(n >> 24)
    n4 := u8(n >> 32)
    n5 := u8(n >> 40)
    n6 := u8(n >> 48)
    n7 := u8(n >> 56)
    return {n0, n1, n2, n3, n4, n5, n6, n7}
}

nonce_from_u64 :: proc(n: u64) -> [12]u8 {
    n := to_be_bytes(n)
    return {0,0,0,0,n[0], n[1], n[2], n[3], n[4], n[5], n[6], n[7]}
}


array_xor :: proc(a: [BLOCKLEN]u8, b: [BLOCKLEN]u8) -> [BLOCKLEN]u8 {
    a := a
    b := b
    output: [BLOCKLEN]u8
    for i in 0..<8 {
        blocka : simd.u8x16 = simd.from_slice(simd.u8x16, a[i*16:i*16+16]);
        blockb : simd.u8x16 = simd.from_slice(simd.u8x16, b[i*16:i*16+16]);
        temp := simd.to_array(blocka ~ blockb)
        copy(output[i*16 : i*16+16], temp[:])
    }
    return output
}

zeropad128 :: proc(input: []u8) -> [BLOCKLEN]u8 {
    assert(uintptr(len(input)) <= BLOCKLEN)
    output : [BLOCKLEN]u8
    copy(output[:], input[:])
    return output
}

zeropad32 :: proc(input: []u8) -> [HASHLEN]u8 {
    assert(uintptr(len(input)) <= HASHLEN)
    output : [HASHLEN]u8
    copy(output[:], input[:])
    return output
}

concat_bytes :: proc(b1: []u8, b2: []u8) -> []u8 {
    output := make_slice([]u8, len(b1) + len(b2));
    copy(output[0:len(b1)], b1)
    copy(output[len(b1):], b2)
    return output
}