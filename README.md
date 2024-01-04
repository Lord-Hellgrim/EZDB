# EZDB

A small database for small bespoke apps. Meant to be fast, easy to use with .csv files, and lightweight to host.

Absolutely minimal dependencies, aiming for only the Rust standard library. May include some encryption crates for security later.

Future features, in order, are planned as:
 - Uploading and querying csv tables, strictly or casually (schema or schemaless) --- basic functionality ready!
 - Encrypted connections --- fully encrypted with AES256 and diffie-hellman key exchange!
 - Command Line Interface --- no code yet
 - Graphical interface --- no code yet
 - Scaling solutions for larger datasets and more queries --- Spawns a thread per request. Scales ok but not great. Untested.

This is not meant to be a replacement for Postgres or other SQL monsters, just a easy little database for co-ordinating
data in a small application.

## How to read

Ths repository currently contains two packages mixed together, a server binary that runs a database server, and a client
library that enables client side communication with the server. Cargo adding EZDB to your project will include both packages.

The server binary part is mostly defined by "server_networking.rs", "db_structure.rs", and "auth.rs". The client library is mostly
defined by "client_networking.rs". Both make heavy use of "networking_utilities.rs". Encryption is implemented in "aes_temp_crypto.rs"
(so called because I am planning to implement my own version and not depend on "aes-gcm", see aes.rs for progress there) and "diffie_hellman.rs". 
"logger.rs" will handle logging once that's implemented. The various .txt files in the root directory are for testing purposes 
and should probably be in their own separate folder. 

EZDB has two main capabilities, storage and handling of tables with a strict schema and arbitrary key-value storage. The value can be
any blob of bytes (it just stores a &[u8]). Currently there are limited queries available on the schema tables. I am planning on making
more queries but I am not planning on implementing a SQL parser or any other scripting language for writing queries. All queries will
be simple non-async rust functions. If you want async functionality I may implement an async wrapper at some point but generally I 
don't like async.

To understand the codebase, it is probably best to start with "db_structure.rs" which is where the main data structure is defined.
Once you have a handle on that you can move on to "client_networking.rs" and "server_networking.rs" which have to be read together
since all their functions are talking to each other, Both rely heavily on "networking_utilities.rs" but if you just want to see the
big picture structure of the code then you won't need to see the implementation details there. If you're curious about encryption
primitives you can check out "aes.rs" which is currently unused since it doesn't implement GCM but it is a working implementation of
aes128 with hardware acceleration (X86 only) written in a very straightforward linear way. There are no wrapper structs or other 
obfuscations that make the algorithm harder to grok. I am pretty proud of it and am planning to use it to encrypt the datastreams once
I implement GCM.

THIS CRATE IS NOT MEANT FOR PRODUCTION USE! ALL ENCRYPTION IS IMPLEMENTED BY AN AMATEUR AND HAS NOT BEEN REVIEWED! DO NOT USE!
