package noise

import "core:crypto"
import "core:crypto/x25519"
import "core:crypto/aead"
import "core:crypto/sha2"

import "core:slice"

import "core:simd"

import "core:fmt"

import "core:bytes"
import "core:net"

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

MAX_PACKET_SIZE: u64 : 65535;

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
    message_patterns: [][]Token,
    current_pattern: int,
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
symmetricstate_MixKeyAndHash :: proc(self: ^SymmetricState, input_key_material: [HASHLEN]u8) {
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
symmetricstate_EncryptAndHash :: proc(self:  ^SymmetricState, plaintext: []u8) -> []u8{
    ciphertext := cipherstate_EncryptWithAd(&self.cipherstate, self.h[:], plaintext)
    symmetricstate_MixHash(self, ciphertext)
    return ciphertext
}

/// Sets plaintext = DecryptWithAd(h, ciphertext), calls MixHash(ciphertext), and returns plaintext. 
/// Note that if k is empty, the DecryptWithAd() call will set plaintext equal to ciphertext.
symmetricstate_DecryptAndHash :: proc(self:  ^SymmetricState, ciphertext: []u8) -> ([]u8, NoiseError) {
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
symmetricstate_Split :: proc(self: ^SymmetricState) -> (CipherState, CipherState) {
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
    handshake_pattern_NK : [][]Token = {
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
        current_pattern = 0,
    };

    return output
}

/// Takes a payload byte sequence which may be zero-length, and a message_buffer to write the output into. 
/// Performs the following steps, aborting if any EncryptAndHash() call returns an error:

/// Fetches and deletes the next message pattern from message_patterns, then sequentially processes each token from the message pattern:

/// For "e": Sets e (which must be empty) to GENERATE_KEYPAIR(). Appends e.public_key to the buffer. Calls MixHash(e.public_key).

/// For "s": Appends EncryptAndHash(s.public_key) to the buffer.

/// For "ee": Calls MixKey(DH(e, re)).

/// For "es": Calls MixKey(DH(e, rs)) if initiator, MixKey(DH(s, re)) if responder.

/// For "se": Calls MixKey(DH(s, re)) if initiator, MixKey(DH(e, rs)) if responder.

/// For "ss": Calls MixKey(DH(s, rs)).

/// Appends EncryptAndHash(payload) to the buffer.

/// If there are no more message patterns returns two new CipherState objects by calling Split().
handshakestate_WriteMessage :: proc(self: ^HandshakeState, message_buffer: net.TCP_Socket) -> (Maybe(CipherState), Maybe(CipherState), NoiseError) {
    pattern := self.message_patterns[self.current_pattern]
    self.current_pattern += 1;
    for token in pattern {
        switch token {
            case .e: {
                self.e = GENERATE_KEYPAIR()
                net.send_tcp(message_buffer, self.e.public_key[:])
                symmetricstate_MixHash(&self.symmetricstate, self.e.public_key[:])
            }
            case .s: {
                temp := symmetricstate_EncryptAndHash(&self.symmetricstate, self.s.public_key[:])
                net.send_tcp(message_buffer, temp)
            }
            case .ee: {
                symmetricstate_MixKey(&self.symmetricstate, DH(self.e, self.re))
            }

            case .es: {
                if self.initiator {
                    symmetricstate_MixKey(&self.symmetricstate, DH(self.e, self.rs))
                } else {
                    symmetricstate_MixKey(&self.symmetricstate, DH(self.s, self.re))
                }
            }
            
            case .se: {
                if self.initiator {
                    symmetricstate_MixKey(&self.symmetricstate, DH(self.s, self.re))
                } else {
                    symmetricstate_MixKey(&self.symmetricstate, DH(self.e, self.rs))
                    
                }
            }
            
            case .ss: {
                symmetricstate_MixKey(&self.symmetricstate, DH(self.s, self.rs))
            }
        };
    }
    
    if self.current_pattern > len(self.message_patterns) {
        sender, receiver := symmetricstate_Split(&self.symmetricstate)
        return sender, receiver, .NoError
    } else {
        return nil, nil, .NoError
    }
}

/// Takes a byte sequence containing a Noise handshake message, and a payload_buffer to write the message's plaintext payload into. 
/// Performs the following steps, aborting if any DecryptAndHash() call returns an error:

/// Fetches and deletes the next message pattern from message_patterns, then sequentially processes each token from the message pattern:

/// For "e": Sets re (which must be empty) to the next DHLEN bytes from the message. Calls MixHash(re.public_key).

/// For "s": Sets temp to the next DHLEN + 16 bytes of the message if HasKey() == True, or to the next DHLEN bytes otherwise. 
/// Sets rs (which must be empty) to DecryptAndHash(temp).

/// For "ee": Calls MixKey(DH(e, re)).

/// For "es": Calls MixKey(DH(e, rs)) if initiator, MixKey(DH(s, re)) if responder.

/// For "se": Calls MixKey(DH(s, re)) if initiator, MixKey(DH(e, rs)) if responder.

/// For "ss": Calls MixKey(DH(s, rs)).

/// Calls DecryptAndHash() on the remaining bytes of the message and stores the output into payload_buffer.

/// If there are no more message patterns returns two new CipherState objects by calling Split().
handshakestate_ReadMessage :: proc(self: ^HandshakeState, message: net.TCP_Socket)  -> (Maybe(CipherState), Maybe(CipherState), NoiseError) {
    zeroslice: [HASHLEN]u8
    pattern := self.message_patterns[self.current_pattern]
    self.current_pattern += 1
    for token in pattern {
        switch token {
            case .e: {
                e : [DHLEN]u8
                net.recv_tcp(message, e[:])
                if self.re != zeroslice {
                    return nil, nil, .WrongState
                } else {
                    self.re = e
                    symmetricstate_MixHash(&self.symmetricstate, self.re[:])
                }
            }
            case .s: {
                if cipherstate_HasKey(&self.symmetricstate.cipherstate) {
                    rs : [DHLEN+16]u8
                    net.recv_tcp(message, rs[:])
                    temp, temp_err := symmetricstate_DecryptAndHash(&self.symmetricstate, rs[:])
                    new_rs := array32_from_slice(temp[:])
                    if self.rs == zeroslice {
                        self.rs = new_rs
                    } else {
                        return nil, nil, .WrongState
                    }
                }
            }
            
            case .ee: {
                symmetricstate_MixKey(&self.symmetricstate, DH(self.e, self.re))
            }

            case .es: {
                if self.initiator {
                    symmetricstate_MixKey(&self.symmetricstate, DH(self.e, self.rs));  
                } else {
                    symmetricstate_MixKey(&self.symmetricstate, DH(self.s, self.re));
                }
            }
            
            case .se: {
                if self.initiator {
                    symmetricstate_MixKey(&self.symmetricstate, DH(self.s, self.re));  
                } else {
                    symmetricstate_MixKey(&self.symmetricstate, DH(self.e, self.rs));
                }
            }
            
            case .ss: {
                symmetricstate_MixKey(&self.symmetricstate, DH(self.s, self.rs))
            }
        };
    }
    if self.current_pattern > len(self.message_patterns) {
        sender, receiver := symmetricstate_Split(&self.symmetricstate)
        return sender, receiver, .NoError
    } else {
        return nil, nil, .NoError
    }
}

array32_from_slice :: proc(slice: []u8) -> [32]u8 {
    buf : [32]u8
    copy(buf[:], slice[0 : min(len(slice), 32)])
    return buf
}

Connection :: struct {
    c1: CipherState,
    c2: CipherState,
    stream: net.TCP_Socket,
    peer: string,
}

Cstate :: enum {
    C1,
    C2
}

__connection_send :: proc(self: ^Connection, message: []u8, state: Cstate) -> NoiseError {
        buffer := make_dynamic_array([dynamic]u8)
        defer delete_dynamic_array(buffer)
        ciphertext : []u8
        switch state {
            case .C1: {
                ciphertext = cipherstate_EncryptWithAd(&self.c1, nil, message)
            }
            case .C2: {
                ciphertext = cipherstate_EncryptWithAd(&self.c2, nil, message)
            }
        }
        ciphertext_len := to_le_bytes(u64(len(ciphertext)))
        extend_from_slice(&buffer, ciphertext_len[:])
        extend_from_slice(&buffer, ciphertext[:])
        net.send_tcp(self.stream, buffer[:])
        return .NoError
    }

__connection_receive :: proc(self: ^Connection, state: Cstate) -> ([]u8, NoiseError) {
        size_buffer : [8]u8
        net.recv_tcp(self.stream, size_buffer[:])
    
        data_len := u64_from_le_slice(size_buffer[:])
        if data_len >  MAX_PACKET_SIZE {
            return nil, .Io
        }
        data := make_dynamic_array([dynamic]u8)
        defer delete(data)
        buffer : [4096]u8
        total_read: u64 = 0
        
        for total_read < data_len {
            to_read := min(4096, data_len - total_read)
            bytes_received, _ := net.recv_tcp(self.stream, buffer[:to_read])
            if bytes_received == 0 {
                return nil, .Io
            }
            extend_from_slice(&data, buffer[:bytes_received])
            total_read += u64(bytes_received)
        }

        decrypted_data: []u8
        switch state {
            case .C1: {
                decrypted_data, _ = cipherstate_DecryptWithAd(&self.c1, nil, data[:])
            }
            case .C2: {
                decrypted_data, _ = cipherstate_DecryptWithAd(&self.c2, nil, data[:])
            }

        };

        return decrypted_data, .NoError
    }

    connection_SEND_C1 :: proc(self: ^Connection, message: []u8) -> NoiseError {
        return __connection_send(self, message, Cstate.C1)
    }

    connection_SEND_C2 :: proc(self: ^Connection, message: []u8) -> NoiseError {
        return __connection_send(self, message, Cstate.C2)
    }

    connection_RECEIVE_C1 :: proc(self: ^Connection) -> ([]u8, NoiseError) {
        return __connection_receive(self, Cstate.C1)
    }

    connection_RECEIVE_C2 :: proc(self: ^Connection) -> ([]u8, NoiseError) {
        return __connection_receive(self, Cstate.C2)
    }


initiate_connection :: proc(address: string) -> (Connection, NoiseError) {
    zeroslice : [HASHLEN]u8
    stream, _ := net.dial_tcp_from_hostname_and_port_string(address)
    s := keypair_random()
    handshake_state := handshakestate_Initialize(
        true,
        nil,
        s,
        keypair_empty(),
        zeroslice,
        zeroslice
    )
    
    // -> e
    handshakestate_WriteMessage(&handshake_state, stream)

    // <- e, ee, s, es
    handshakestate_ReadMessage(&handshake_state, stream)

    // -> s, se
    res1, res2, connection_error := handshakestate_WriteMessage(&handshake_state, stream)

    switch res1 {
        case res1.?: {
            return Connection {
                    c1 = res1.?,
                    c2 = res2.?,
                    stream = stream,
                    peer = ""
                }, .NoError
            }
        case nil: {
            return Connection{c1 = cipherstate_InitializeKey(zeroslice), c2 = cipherstate_InitializeKey(zeroslice), stream = net.TCP_Socket(0), peer = ""}, .Io
        } 
    }

    return Connection{c1 = cipherstate_InitializeKey(zeroslice), c2 = cipherstate_InitializeKey(zeroslice), stream = net.TCP_Socket(0), peer = ""}, .WrongState
}

ESTABLISH_CONNECTION :: proc(stream: net.TCP_Socket, s: KeyPair) -> (Connection, NoiseError) {
    handshakestate, _ := ESTABLISH_CONNECTION_STEP_1(stream, s)

    ESTABLISH_CONNECTION_STEP_2(stream, &handshakestate)

    connection, _ := ESTABLISH_CONNECTION_STEP_3(stream, &handshakestate)

    return connection, .NoError
}

ESTABLISH_CONNECTION_STEP_1 :: proc(stream: net.TCP_Socket, s: KeyPair) -> (HandshakeState, NoiseError) {
    zeroslice : [HASHLEN]u8
    handshakestate := handshakestate_Initialize(false, nil, s, keypair_empty(), zeroslice, zeroslice);

    // <- e
    handshakestate_ReadMessage(&handshakestate, stream)

    return handshakestate, .NoError
}

ESTABLISH_CONNECTION_STEP_2 :: proc(stream: net.TCP_Socket, handshakestate: ^HandshakeState) -> NoiseError {
    
    handshakestate_WriteMessage(handshakestate, stream)

    return .NoError
}

ESTABLISH_CONNECTION_STEP_3 :: proc(stream: net.TCP_Socket, handshakestate: ^HandshakeState) -> (Connection, NoiseError) {
    // <- s, se
    res1, res2, _ := handshakestate_ReadMessage(handshakestate, stream)

    fmt.println("returning Connection!!")
    switch res1 {
        case res1.?:  {
            return Connection {
                c1 = res1.?,
                c2 = res2.?,
                stream = stream,
                peer = ""
            }, .NoError
        }
        case nil: {
            return connection_nullcon(), .Io
        }
    }
    return connection_nullcon(), .WrongState
}

extend_from_slice :: proc(array: ^[dynamic]u8, slice: []u8) {
    for byte in slice {
        append(array, byte)
    }
}

connection_nullcon :: proc() -> Connection {
    zeroslice : [HASHLEN]u8
    return Connection{c1 = cipherstate_InitializeKey(zeroslice), c2 = cipherstate_InitializeKey(zeroslice), stream = net.TCP_Socket(0), peer = ""}
}

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
