#include <stdio.h> 
#include <netdb.h> 
#include <netinet/in.h> 
#include <arpa/inet.h>
#include <stdlib.h> 
#include <string.h> 
#include <sys/socket.h> 
#include <sys/types.h> 
#include <unistd.h> // read(), write(), close()
#include <openssl/ec.h>
#include <openssl/rand.h>
#include <openssl/engine.h>
#include <openssl/err.h>
#include <openssl/aes.h>
#include <openssl/evp.h>
#include <openssl/pem.h>

#define MAX 80 
#define PORT 3004
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

uint32_t address_from_array(uint8_t* numbers) {
    uint32_t result = 0;
    result += numbers[0] << 24;
    result += numbers[1] << 16;
    result += numbers[2] << 8;
    result += numbers[3] << 0;
    return result;
}

int make_connection(char* host_address) {
    int sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == -1) {
        return 0;
    }

    // uint32_t server_address = address_from_array(host_address);

    uint8_t localhost[4] = {127,0,0,1};
    uint32_t localhost_address = address_from_array(localhost);

    struct sockaddr_in serv_addr;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_port = htons(PORT);
    serv_addr.sin_addr.s_addr = localhost_address;

    printf("about to make connection\n");
    int check = connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
    if (check == -1) {
        return 0;
    }
    printf("Connection made\n");


    return sock;

}


void handleErrors() {
    ERR_print_errors_fp(stderr);
    abort();
}



unsigned char* diffie_hellman(int connected_stream) {
    // Initialize OpenSSL's error strings and algorithms (if not done globally)
    ERR_load_crypto_strings();
    OpenSSL_add_all_algorithms();

    EVP_PKEY_CTX *pctx = NULL;
    EVP_PKEY *pkey = NULL;
    unsigned char *secret;
    size_t secret_len;

    // Create a new PKEY context for X25519 key generation
    pctx = EVP_PKEY_CTX_new_id(EVP_PKEY_X25519, NULL);
    if (!pctx) handleErrors();

    // Generate a private key
    if (EVP_PKEY_keygen_init(pctx) <= 0) handleErrors();    
    if (EVP_PKEY_keygen(pctx, &pkey) <= 0) handleErrors();

    // Extract and print the public key in PEM format
    BIO *pub = BIO_new(BIO_s_mem());
    if (PEM_write_bio_PUBKEY(pub, pkey) <= 0) handleErrors();
    char *pub_key_pem = NULL;
    size_t pub_len = BIO_get_mem_data(pub, &pub_key_pem);
    printf("Generated Public Key:\n%.*s\n", (int)pub_len, pub_key_pem);

    // In a real scenario, you would exchange the public key with the peer
    // For demonstration, let's assume the peer's public key is the same as our own
    char server_public_key[32];
    memset(server_public_key, 0, 32);

    printf("about to read from server\n");
    int read_error_check = read(connected_stream, &server_public_key, 32);
    if (read_error_check != 0) {
        return NULL;
    }
    printf("successfully read from server\n");

    EVP_PKEY* peerkey;
    if (!EVP_PKEY_set1_tls_encodedpoint(peerkey, server_public_key, sizeof(server_public_key))) {
        fprintf(stderr, "Failed to set public key\n");
        ERR_print_errors_fp(stderr);
        EVP_PKEY_free(pkey);
        return NULL;
    }
    peerkey = pkey;

    // Derive the shared secret using the private key and the peer's public key
    EVP_PKEY_CTX *derive_ctx = EVP_PKEY_CTX_new(pkey, NULL);
    if (!derive_ctx) handleErrors();
    if (EVP_PKEY_derive_init(derive_ctx) <= 0) handleErrors();
    if (EVP_PKEY_derive_set_peer(derive_ctx, peerkey) <= 0) handleErrors();

    // Determine the buffer length for the shared secret
    if (EVP_PKEY_derive(derive_ctx, NULL, &secret_len) <= 0) handleErrors();

    // Allocate memory for the shared secret
    secret = (unsigned char *)OPENSSL_malloc(secret_len);
    if (!secret) handleErrors();

    // Derive the shared secret
    if (EVP_PKEY_derive(derive_ctx, secret, &secret_len) <= 0) handleErrors();

    // Print the shared secret
    printf("Derived Shared Secret:\n");
    for (size_t i = 0; i < secret_len; i++) {
        printf("%02x", secret[i]);
    }
    printf("\n");

    // Clean up
    EVP_PKEY_CTX_free(pctx);
    EVP_PKEY_CTX_free(derive_ctx);
    EVP_PKEY_free(pkey);
    BIO_free(pub);

    // Cleanup OpenSSL
    EVP_cleanup();
    ERR_free_strings();

    return secret;
}



int main() {
    

    Connection* connection;

    uint8_t address[4] = {127,0,0,1};
    char* username = "admin";
    char* password = "admin";

    int connection_to_server = make_connection(address);

    unsigned char* secret = diffie_hellman(connection_to_server);
    OPENSSL_free(secret);
    return 0;

}


