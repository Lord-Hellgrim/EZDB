# EZDB

A small database for small bespoke apps. Meant to be fast, easy to use with .csv files, and lightweight to host.

Absolutely minimal dependencies, aiming for only the Rust standard library. May include some encryption crates for security later.

Future features, in order, are planned as:
 - Uploading and querying csv tables, strictly or casually (schema or schemaless) --- close to completion
 - Encrypted connections --- no code yet
 - Command Line Interface --- no code yet
 - Graphical interface --- no code yet
 - Scaling solutions for larger datasets and more queries --- small baseline multithreading, nowhere near ready

This is not meant to be a replacement for Postgres or other SQL monsters, just a easy little database for co-ordinating
data in a small application.

STOP IT!
