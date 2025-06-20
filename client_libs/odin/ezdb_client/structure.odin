package ezdb_client

import str "core:strings"
import utf "core:unicode/utf8"
import "core:slice"
import "core:log"
import "core:mem"
import "base:runtime"


ORDER :: 20

KeyString :: distinct [64]u8

KeyString_from_string :: proc(s: string) -> KeyString {
    str := str_to_slice(s)
    length := min(len(str), 64)
    output : KeyString
    copy_slice(output[:], str[0:length])
    return output
}

KeyStringAlert :: enum {
    InvalidUtf8,
    Ok,
    Cutoff,
    Empty
}


string_from_keystring :: proc(ks: KeyString, allocator := context.temp_allocator) -> (string, EzError) {
    temp_ks := ks
    s: string = str.clone_from(string(temp_ks[:]), allocator)
    
    return s, .no_error
}

DbType :: enum {
    Int,
    Float,
    Text,
}

TableKey :: enum {
    None,
    Primary,
    Foreign
}

HeaderItem :: struct {
    name: KeyString,
    kind: DbType,
    key: TableKey,
}

EzTable :: struct {
    name: KeyString,
    columns: map[KeyString]DbColumn,
}

DbColumn :: union {
    [dynamic]i32,
    [dynamic]f32,
    [dynamic]KeyString,
}


dbcolumn_destroy :: proc(column: DbColumn) {
    switch t in column {
        case [dynamic]i32: 
            delete(t)
        case [dynamic]f32: 
            delete(t)
        case [dynamic]KeyString: 
            delete(t)
    }
}

Metadata :: struct {
    last_access: u32,
    times_accessed: u32,
}

eztable_create :: proc(name: KeyString) -> EzTable {
    table := EzTable {
        name = name,
    }
    return table
}

eztable_destroy :: proc(table: EzTable) {
    for name, column in table.columns {
        dbcolumn_destroy(column)
    }
    delete(table.columns)
}

// sort_eztable_by_column :: proc(table: ^EzTable, sort_column: KeyString) -> bool {
//     length := eztable_length(table^)

//     indices := make([]int, length, context.temp_allocator)

//     switch col in table.columns[sort_column] {
//         case [dynamic]i32: indices = slice.sort_with_indices(col[:])
//         case [dynamic]f32: indices = slice.sort_with_indices(col[:])
//         case [dynamic]KeyString: indices = slice.sort_with_indices(col[:])
//     }

//     return true
// }

rearrange_by_index :: proc(col: $T/[]$E, indices: []int) {
    assert(len(col) == len(indices))

    for i in 0..<len(col) {
        for indices[i] != i {
            slice.swap(col, i, indices[i])
            slice.swap(indices, indices[i], i)
        }
    }
}

eztable_length :: proc(table: EzTable) -> int {
    length := 0
    for _, column in table.columns {
        switch t in column {
            case [dynamic]i32: length = len(t)
            case [dynamic]f32: length = len(t)
            case [dynamic]KeyString: length = len(t)
        }
        break
    }
    return length
}

Hallocator :: struct {
    memory: [dynamic]byte,
    block_size: u32,
    free_list: [dynamic]uint,
}

new_hallocator :: proc(block_size: u32) -> Hallocator {
    
    hallocator: Hallocator

    current_allocator := context.allocator

    memory : = make_dynamic_array([dynamic]byte)

    return hallocator
}

BPlusTreeNode :: struct($Key: typeid) {
    parent: ^BPlusTreeNode(Key),
    keys: [ORDER]Key,
    children: [ORDER+1]^rawptr,
    is_leaf: bool,
}

BPlusTree :: struct($Key: typeid) {
    allocator: mem.Allocator,
    root_node: BPlusTreeNode(Key),
}


new_BPlusTree_i32 :: proc(allocator := context.allocator) -> BPlusTree(i32) {
    root_node : BPlusTreeNode(i32)
    
    tree := BPlusTree(i32){
        allocator = allocator,
        root_node = root_node,
    }

    return tree
}

new_BPlusTree_keystring :: proc(allocator := context.allocator) -> BPlusTree(KeyString) {
    root_node : BPlusTreeNode(KeyString)
    
    tree := BPlusTree(KeyString){
        allocator = allocator,
        root_node = root_node,
    }

    return tree
}

bplustree_insert_key :: proc(tree: ^BPlusTree($Key), key: Key) {
    tree.keys
}