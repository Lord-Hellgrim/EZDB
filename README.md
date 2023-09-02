# EZDB

A small database for small bespoke apps. Meant to be fast, easy to use with .csv files, and lightweight to host.

Absolutely minimal dependencies, aiming for only the Rust standard library. May include some encryption crates for security later.

Future features, in order, are planned as:
 - Uploading and querying csv tables, strictly or casually (schema or schemaless) --- just needs update
 - Encrypted connections --- almost finished
 - Command Line Interface --- no code yet
 - Graphical interface --- no code yet
 - Scaling solutions for larger datasets and more queries --- small baseline multithreading, nowhere near ready

This is not meant to be a replacement for Postgres or other SQL monsters, just a easy little database for co-ordinating
data in a small application.

## How to read

Ths repository currently contains two packages mixed together, a server binary that will run a database server, and a client
library that will enable client side communication with the server. As of right now they are both bundled together since they
share a lot of dependencies that may be extracted into a separate EZDB_core package.

The server binary part is mostly defined by "server_networking.rs", "db_structure.rs", and "auth.rs". The client library is mostly
defined by "client_networking.rs". Both make heavy use of "networking_utilities.rs". "logger.rs" will handle logging once that's
implemented. "basic_io_functions.rs" is essentially deprecated and will probably be deleted. The various .txt files in the root
directory are for testing purposes and should probably be in their own separate folder. The "utility_scripts" directory is not
used currently but is an interesting case study in the relative speed of naive rust and naive python. It's quite shocking.

The main data structure of EZDB (currently, planning schemaless tables in future) is the StrictTable (db_structure.rs), which is
essentially a BtreeMap with some tacked on metadata and identifiers and some methods for creating it to ensure it maintains the
schema. All of the rest of the code is for sending, receiving, updating, querying, securing, and (soon) encrypting StrictTable
structs.

Ok, you should totally read aes.rs, I'm kinda proud of that. Encryption passing currently. Decryption coming soon!
