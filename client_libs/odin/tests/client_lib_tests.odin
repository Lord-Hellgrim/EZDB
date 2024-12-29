package tests

import "core:testing"
import "../ezdb_client"
import "core:fmt"
import "core:log"
import "../noise"

@(test)
test_bytes_from_strings :: proc(t: ^testing.T) {
    a := "0"
    b := "0"
    c := "0"
    d := "0"
    pack := ezdb_client.bytes_from_strings(a,b,c,d)
    log.info(pack)
}

@(test)
test_u64_from_le_slice :: proc(t: ^testing.T) {
    num :u64 = 0x123456
    bytes := noise.to_le_bytes(num)
    parsed := noise.u64_from_le_slice(bytes[:])
    fmt.println(bytes)
    fmt.println(num)
    fmt.println(parsed)
    testing.expect(t, num == parsed)

}