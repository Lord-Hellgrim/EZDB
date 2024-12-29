package noise

import "core:fmt"
import "core:crypto"
import "core:slice"
import "core:strings"

test_u64_from_slice :: proc() {
    le_num :u64 = 0x123456
    le_bytes := to_le_bytes(le_num)
    le_parsed := u64_from_le_slice(le_bytes[:])
    assert(le_num == le_parsed)


    be_num :u64 = 0x123456
    be_bytes := to_le_bytes(be_num)
    be_parsed := u64_from_le_slice(be_bytes[:])
    assert(be_num == be_parsed)
}

main :: proc() {
    k : [32]u8
    n : u64 = 5
    ad := str_to_slice("Double check me!")
    unencrypted := str_to_slice("This is an unencrypted block of text that is longer than 128 bits!!!")
    buffer := unencrypted
    encrypted, enc_error := ENCRYPT(k, n, ad, buffer)
    decrypted, dec_error := DECRYPT(k, n, ad, encrypted)
    assert(slice.equal(unencrypted, decrypted))

    test_u64_from_slice()


    fmt.println("SUCCESS!")

}