# THIS PROJECT IS RETIRED
Tl;dr
I've learned enough to know that I would need a total rewrite. Not doing that. Other plans.

I started this project at a time that I thought all a database did was store .csv files.
I had just gotten sick of Python abstracting away all the actual information of how a 
computer works and figured I should do a "real" program. A database seemed like a decent
place to start, since it touched on all the different aspects of programming except graphics,
which I am still not very interested in. My original intention was to write all the code from
scratch in the lowest level language I could find. Many lessons have been learned.

I didn't know any rust when I started this. I didn't know what TCP was, or really anything
about internet protocols at all. I had a passing familiarity with cryptography terms, but
no idea how they were implemented.

In hindsight, rust was a good choice for this project. While it took learning C to actually
understand what the & operator is really doing, I don't think I could have gotten this far
if I had to start by implementing my own Vec, HashMap, Locking primitives, and so on. Rust
allowed me to build the stupid version of what I wanted while still making me aware of memory
to a degree (Vec::with_capacity(n), I love you forever).
At this point, however, rust is mostly just annoying and won't prevent the most serious integrity issues that I foresee being the biggest problem of a database engine, and the
compile time has become unacceptable.

Therefore I am going to move on to my next project, leaving this unfinished. I have learned
what I needed to learn from EZDB, but the codebase is too big of a mess to fix without a
total rewrite anyway. Rest in peace, EZDB. You served me well.


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
