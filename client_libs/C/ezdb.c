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
#include <openssl/evp.h>
#include <openssl/rsa.h>
#include <openssl/pem.h>

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



void handleErrors() {
    ERR_print_errors_fp(stderr);
    abort();
}

EVP_PKEY* generate_ecdh_private_key() {
    EVP_PKEY* key;

    return key;
}

int diffie_hellman() {
    // Initialize OpenSSL's error strings and algorithms (if not done globally)
    ERR_load_crypto_strings();
    OpenSSL_add_all_algorithms();

    EVP_PKEY_CTX *pctx = NULL;
    EVP_PKEY *pkey = NULL;
    EVP_PKEY *peerkey = NULL;
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
    OPENSSL_free(secret);

    // Cleanup OpenSSL
    EVP_cleanup();
    ERR_free_strings();

    return 0;
}



int main() {
    
    Connection* connection;

    char* address = "127.0.0.1:3004";
    char* username = "admin";
    char* password = "admin";

    EVP_PKEY* pkey = NULL;
    diffie_hellman();
    return 0;

}


