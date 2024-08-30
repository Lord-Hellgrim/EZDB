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


int make_dh_private_key(EVP_PKEY* pkey) {
    EVP_PKEY_CTX *ctx;

    /* Initialize OpenSSL */
    OpenSSL_add_all_algorithms();
    ERR_load_crypto_strings();

    /* Create context for key generation */
    ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_DH, NULL);
    if (!ctx) {
        fprintf(stderr, "Error creating context\n");
        ERR_print_errors_fp(stderr);
        return 1;
    }

    /* Initialize key generation */
    if (EVP_PKEY_keygen_init(ctx) <= 0) {
        fprintf(stderr, "Error initializing keygen\n");
        ERR_print_errors_fp(stderr);
        EVP_PKEY_CTX_free(ctx);
        return 1;
    }

    /* Set the RSA key length */
    if (EVP_PKEY_CTX_set_dh_keygen_bits(ctx, 2048) <= 0) {
        fprintf(stderr, "Error setting DH key length\n");
        ERR_print_errors_fp(stderr);
        EVP_PKEY_CTX_free(ctx);
        return 1;
    }

    /* Generate the key */
    if (EVP_PKEY_keygen(ctx, &pkey) <= 0) {
        fprintf(stderr, "Error generating key\n");
        ERR_print_errors_fp(stderr);
        EVP_PKEY_CTX_free(ctx);
        return 1;
    }

    printf("Key generated successfully!\n");
    printf("key: %s\n", (char*)pkey);

    /* Cleanup */
    EVP_PKEY_free(pkey);
    EVP_PKEY_CTX_free(ctx);
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
    make_dh_private_key(pkey);
    return 0;

}


