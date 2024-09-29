package tests

import "core:testing"
import "../ezdb_client"
import "core:fmt"
import "core:log"

@(test)
test_bytes_from_strings :: proc(t: ^testing.T) {
    a := "0"
    b := "0"
    c := "0"
    d := "0"
    pack := ezdb_client.bytes_from_strings(a,b,c,d)
    log.info(pack)
}