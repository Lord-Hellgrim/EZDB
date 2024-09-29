package ezdb_client


import "core:fmt"
import "core:net"
import "core:slice"
import "core:strings"
import "core:unicode/utf8"
import "core:crypto/x25519"
import "core:crypto"
import "core:crypto/aead"
import "core:crypto/aes"
import "core:crypto/sha2"
import "core:compress/gzip"


Connection :: struct {
    stream: net.TCP_Socket,
    aes_key: [32]u8,
    user: string,
}

EzError :: enum {
    no_error,
    no_server,
    crypto,
    net,
    auth,
    utf8,
}


make_connection :: proc(address: [4]u8, port: int, username: string, password: string) -> (Connection, EzError) {

    connection : Connection
    if len(username) > 512 || len(password) > 512 {
        return connection, EzError.auth
    }
    shared_secret : [32]u8

    net_address : net.IP4_Address = {address[0], address[1], address[2], address[3]}
    socket, socket_error := net.dial_tcp_from_address_and_port(net_address, port)
    if socket_error != nil {
        fmt.println("Failed to create socket")
        return connection, EzError.net
    }

    self_private_key : [32]u8
    crypto.rand_bytes(self_private_key[:])
    self_public_key : [32]u8;
    x25519.scalarmult_basepoint(self_public_key[:], self_private_key[:])

    server_public_key : [32]u8
    nr_of_bytes_read, recv_error := net.recv_tcp(socket, server_public_key[:])
    if nr_of_bytes_read != 32 || recv_error == nil {
        fmt.println("error reading server public key")
    }
    net.send_tcp(socket, self_public_key[:])
    fmt.println(server_public_key)

    x25519.scalarmult(shared_secret[:], self_private_key[:], server_public_key[:])

    aes_key := simple_hash(shared_secret[:])

    connection.aes_key = aes_key
    connection.stream = socket

    auth_buffer : [1024]u8
    copy_slice(auth_buffer[0 : len(username)], str_to_slice(username))
    copy_slice(auth_buffer[512 : len(password)], str_to_slice(password))

    encrypted_block, encryption_error := aes256gcm_encrypt(auth_buffer[:], connection.aes_key[:])
    if encryption_error != EzError.no_error {
        return connection, .crypto
    }

    net.send_tcp(connection.stream, encrypted_block)

    return connection, nil

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

bytes_from_strings :: proc(strings: ..string) -> []byte {
    length := 64 * len(strings)
    output := make_slice([]byte, length)
    pos := 0
    for s in strings {
        m := min(64, len(s))
        copy_slice(output[pos*64:pos*64+m], str_to_slice(s))
        pos += 1
    }

    return output
}

main :: proc() {

    localhost :[4]u8 = {127,0,0,1}
    
    test_key : [32]u8

    plaintext := "hellope"

    ciphertext, encrypt_error := aes256gcm_encrypt(str_to_slice(plaintext), test_key[:])
    decrypted_plaintext, decrypt_error := aes256gcm_decrypt(ciphertext, test_key[:])

    decrypted_string, success := slice_to_str(decrypted_plaintext)
    if !success {
        panic("AAAAAAA")
    }
    fmt.println(decrypted_string)



// scalarmult_basepoint with a random 32-byte scalar writes a 32-byte public key to dst
// (said random 32-byte scalar is the private key)
// scalarmut(shared_sekrit, your_private_key, their_public_key)
// basepoint is the canonical generator of the group
// in theory this only needs scalarmult since scalarmult(your_public, your_private, basepoint) is equivalent


}

/// Uploads a given csv string to the EZDB server at the given address.
/// Will return an error if the string is not strictly formatted
upload_csv :: proc(address: [4]u8, port: int, username: string, password: string, table_name: string, csv: string) -> EzError {
    fmt.println("calling: upload_csv()")

    connection := make_connection(address, port, username, password) or_return


    // instruction = Instruction::Upload(KeyString::from(table_name));
    // send_instruction_with_associated_data(instruction, username, csv.as_bytes(), &mut connection)?;

    // let response = receive_decrypt(&mut connection)?;
    // let response = String::from_utf8(response)?;

    // parse_response(&response, username, table_name)
    return .no_error

}

send_instruction_with_associated_data :: proc(instruction: string, username: &str, associated_data: &[u8], connection: &mut Connection) -> Result<(), EzError> {
    let instruction = encrypt_aes256_nonce_prefixed(&instruction.to_bytes(username), &connection.aes_key);
    println!("instruction lnght: {} bytes", instruction.len());
    
    let associated_data = miniz_compress(associated_data)?;
    let associated_data = encrypt_aes256_nonce_prefixed(&associated_data, &connection.aes_key);
    println!("associated_data.len(): {}", associated_data.len());
    let mut package = Vec::new();
    package.extend_from_slice(&instruction);
    package.extend_from_slice(&(associated_data.len()).to_le_bytes());
    package.extend_from_slice(&associated_data);
    println!("package len: {}", package.len()-284);

    connection.stream.write_all(&package)?;

    Ok(())
}

simple_hash :: proc(plaintext: []u8) -> [32]u8 {
    ctx : sha2.Context_256;
    sha2.init_256(&ctx)

    sha2.update(&ctx, plaintext)
    hash : [32]u8
    sha2.final(&ctx, hash[:])

    return hash
}

aes256gcm_encrypt :: proc(plaintext: []u8, key: []u8) -> ( []byte , EzError ) {
    tag : [16]u8
    
    ciphertext_buffer := make_slice([]byte, len(plaintext)+28, context.temp_allocator)

    ctx : aead.Context
    iv : [12]u8
    crypto.rand_bytes(iv[:])
    copy_slice(ciphertext_buffer[0:12], iv[:])

    aead.init(&ctx, aead.Algorithm.AES_GCM_256, key)
    aead.seal(&ctx, ciphertext_buffer[12:len(ciphertext_buffer)-16], tag[:], iv[:], nil, plaintext)

    copy_slice(ciphertext_buffer[len(ciphertext_buffer)-16:], tag[:])

    fmt.println(ciphertext_buffer)

    return ciphertext_buffer, .no_error
}

aes256gcm_decrypt :: proc(ciphertext: []u8, key: []u8) -> ( []byte, EzError ) {
    
    plaintext_buffer := make_slice([]byte, len(ciphertext) - 28)
    
    ctx : aead.Context
    iv : [12]u8
    copy_slice(iv[:], ciphertext[0:12])
    tag : [16]u8
    copy_slice(tag[:], ciphertext[len(ciphertext)-16:])

    aead.init(&ctx, aead.Algorithm.AES_GCM_256, key)
    if aead.open_ctx(&ctx, plaintext_buffer, iv[:], nil, ciphertext[12:len(ciphertext)-16], tag[:]) {
        return plaintext_buffer, EzError.no_error
    } else {
        return plaintext_buffer, EzError.crypto
    }
}



Response :: struct {
    response_code: int,
    data: EzTable,
}

