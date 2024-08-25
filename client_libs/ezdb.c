#include <stdio.h> 
#include <netdb.h> 
#include <netinet/in.h> 
#include <stdlib.h> 
#include <string.h> 
#include <sys/socket.h> 
#include <sys/types.h> 
#include <unistd.h> // read(), write(), close()
#include <openssl/dh.h>
#include <openssl/engine.h>
#include <openssl/err.h>
#include <openssl/aes.h>

#define MAX 80 
#define PORT 8080 
#define SA struct sockaddr 


typedef struct Result {
    int response_code;
    void* data;
} Result;


typedef struct Connection {
    int stream;
    char* user;
    char* aes_key;
} Connection;


int connect_to_db_server(Connection* connection, char* address, char* username, char* password) {
    if (strlen(username) > 512 || strlen(password) > 512) {
        return NULL;
    }

    DH* dh = DH_new();
    if (dh == NULL) {
        return 1;
    }

    if (!DH_generate_parameters_ex(dh, 2048, DH_GENERATOR_2, NULL)) {
        return 1;
    }

     if (!DH_generate_key(dh)) {
        DH_free(dh);
        return 1;
    }

    const BIGNUM *private_key = DH_get0_priv_key(dh);
    if (private_key == NULL) {
        return 1;
    }

    const BIGNUM *public_key = DH_get0_priv_key(dh);
    if (public_key == NULL) {
        return 1;
    }

    int stream_id = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in socket_address;
    socket_address.sin_family = AF_INET;
    socket_address.sin_port = 3004;
    socket_address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    connect(stream_id, &socket_address, sizeof(socket_address));


    char key_buffer[32];
    read(stream_id, key_buffer, 32);
    



    DH_free(dh);

}


/// A connection to a peer. The client side uses the same struct.
// pub struct Connection {
//     pub stream: TcpStream,
//     pub user: String,
//     pub aes_key: [u8;32],   
// }

// impl Connection {
//     /// Initialize a connection. This means doing diffie hellman key exchange and establishing a shared secret
//     pub fn connect(address: &str, username: &str, password: &str) -> Result<Connection, EzError> {

//         if username.len() > 512 || password.len() > 512 {
//             return Err(EzError::Authentication(AuthenticationError::TooLong))
//         }

//         let client_private_key = EphemeralSecret::random();
//         let client_public_key = PublicKey::from(&client_private_key);

//         let mut stream = TcpStream::connect(address)?;
//         let mut key_buffer: [u8; 32] = [0u8;32];
//         stream.read_exact(&mut key_buffer)?;
//         let server_public_key = PublicKey::from(key_buffer);
//         stream.write_all(client_public_key.as_bytes())?;
//         let shared_secret = client_private_key.diffie_hellman(&server_public_key);
//         let aes_key = blake3_hash(&shared_secret.to_bytes());

//         let mut auth_buffer = [0u8; 1024];
//         auth_buffer[0..username.len()].copy_from_slice(username.as_bytes());
//         auth_buffer[512..512+password.len()].copy_from_slice(password.as_bytes());
//         // println!("auth_buffer: {:x?}", auth_buffer);
        
//         let (encrypted_data, data_nonce) = encrypt_aes256(&auth_buffer, &aes_key);
//         println!("data_nonce: {:x?}", data_nonce);
//         // The reason for the +28 in the length checker is that it accounts for the length of the nonce (IV) and the authentication tag
//         // in the aes-gcm encryption. The nonce is 12 bytes and the auth tag is 16 bytes
//         let mut encrypted_data_block = Vec::with_capacity(encrypted_data.len() + 28);
//         encrypted_data_block.extend_from_slice(&encrypted_data);
//         encrypted_data_block.extend_from_slice(&data_nonce);
//         // println!("Encrypted auth string: {:x?}", encrypted_data_block);
//         // println!("Encrypted auth string.len(): {}", encrypted_data_block.len());
        
//         // println!("Sending data...");
//         stream.write_all(&encrypted_data_block)?;
//         stream.flush()?;
//         stream.set_read_timeout(Some(Duration::from_secs(20)))?;

//         let user = username.to_owned();
//         Ok(
//             Connection {
//                 stream: stream,
//                 user: user,
//                 aes_key: aes_key,
//             }
//         )

//     }
// }



Result query_table(char* address, char* username, char* password, char* query) {

}

// /// Send an EZQL query to the database server
// Result query_table(
//     address: &str,
//     username: &str,
//     password: &str,
//     query: &str,
// ) -> Result<Response, EzError> {
//     let mut connection = Connection::connect(address, username, password)?;

//     let response = instruction_send_and_confirm(
//         Instruction::Query(query.to_owned()),
//         &mut connection,
//     )?;
//     println!("HERE 1!!!");
//     let data: Vec<u8>;
//     match response.as_str() {
        
//         // THIS IS WHERE YOU SEND THE BULK OF THE DATA
//         //########## SUCCESS BRANCH #################################
//         "OK" => data = receive_data(&mut connection)?,
//         //###########################################################
//         "Username is incorrect" => {
//             return Err(EzError::Authentication(AuthenticationError::WrongUser(
//                 connection.user,
//             )))
//         }
//         "Password is incorrect" => {
//             return Err(EzError::Authentication(
//                 AuthenticationError::WrongPassword,
//             ))
//         }
//         e => panic!("Need to handle error: {}", e),
//     };
//     println!("HERE 2!!!");
//     println!("received data:\n{}", bytes_to_str(&data)?);

//     match connection.stream.write("OK".as_bytes()) {
//         Ok(n) => println!("Wrote 'OK' as {n} bytes"),
//         Err(e) => {
//             return Err(EzError::Io(e.kind()));
//         }
//     };

//     match String::from_utf8(data.clone()) {
//         Ok(x) => Ok(Response::Message(x)),
//         Err(_) => match ColumnTable::from_binary("RESULT", &data) {
//             Ok(table) => Ok(Response::Table(table)),
//             Err(e) => Err(e.into()),
//         },
//     }
// }