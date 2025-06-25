package ezdb_client

import str "core:strings"
import utf "core:unicode/utf8"
import "core:slice"
import "core:log"
import "core:mem"
import "base:runtime"
import smarr "../smarray"

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

BPlusTreeStatusCode :: enum {
    Ok,
    Error,
}

BPlusTreeNode :: struct($Key: typeid) {
    keys: smarr.Small_Array(ORDER, Key),
    parent: ^BPlusTreeNode(Key),
    children: smarr.Small_Array(ORDER+1, rawptr),
    is_leaf: bool,
}

BPlusTreeLeaf :: struct($Key: typeid) {
    keys: smarr.Small_Array(ORDER, Key),
    parent: ^BPlusTreeNode(Key),
    children: smarr.Small_Array(ORDER, rawptr),
    next: ^BPlusTreeLeaf(Key),
    is_leaf: bool,
}

new_BPlusTree_leaf :: proc($Key: typeid, allocator := context.allocator) -> ^BPlusTreeNode(Key) {
    node := new(BPlusTreeNode(Key), allocator)
    node.is_leaf = true
    return node
}

new_BPlusTree_branch :: proc($Key: typeid, allocator := context.allocator) -> ^BPlusTreeNode(Key) {
    node := new(BPlusTreeNode(Key), allocator)
    return node
}

BPlusTree :: struct($Key: typeid) {
    allocator: mem.Allocator,
    root_node: ^BPlusTreeNode(Key),
}


new_BPlusTree :: proc($Key: typeid, allocator := context.allocator) -> BPlusTree(Key) {
    root_node := new_BPlusTree_leaf(Key)
    
    tree := BPlusTree(Key){
        allocator = allocator,
        root_node = root_node,
    }

    return tree
}

bplus_tree_find_leaf :: proc(tree: BPlusTree($Key), key: Key) -> ^BPlusTreeNode(Key) {
    node := tree.root_node
    i := 0
    for !node.is_leaf {
        i = 0
        for i < smarr.len(node.keys) {
            if key >= smarr.get(node.keys, i) {
                i += 1;
            }
            else {
                break;
            }
        }
        node: ^BPlusTreeNode = transmute(^BPlusTreeNode)smarr.get(node.children, i)
    }
    return node
}

cut :: proc(x: $NUMBER) -> NUMBER {
    if x%2 == 0 {
        return x/2
    } else {
        return (x/2) + 1
    }
}

linear_search :: proc(haystack: smarr.Small_Array($N, $T), needle: T) -> int {
    for i in 0..< smarr.len(haystack) {
        if smarr.get(haystack, i) == needle {
            return i
        }
    }

    return -1
}

split_small_array :: proc(array: smarr.Small_Array($N, $T)) -> (smarr.Small_Array(N, T), smarr.Small_Array(N, T)) {
    first_array : smarr.Small_Array(N, T)
    second_array : smarr.Small_Array(N, T)

    for i in 0..<cut(smarr.len(array)) {
        smarr.push_back(&first_array, smarr.get(array, i))
    }

    for i in cut(smarr.len(array)) ..< smarr.len(array) {
        smarr.push_back(&second_array, smarr.get(array, i))
    }

    return first_array, second_array
}

bplustree_insert_key :: proc(tree: ^BPlusTree($Key), key: Key, value: rawptr) -> BPlusTreeStatusCode {
    context.allocator = tree.allocator
    defer context.allocator = default_allocator
    
    node := bplus_tree_find_leaf(tree^, key)

    key_index := linear_search(node.keys, key)
    smarr.push_back(&node.keys, key)
    smarr.push_back(&node.children, value)

    if smarr.len(node.keys) > ORDER -1 {
        panic("Node somehow has more keys than the tree ORDER")
    } else if smarr.len(node.keys) == ORDER-1 {

        left_node := new_BPlusTree_leaf(Key)
        right_node := new_BPlusTree_leaf(Key)

        split_key := smarr.get(node.keys, cut(ORDER))

        left_keys, right_keys := split_small_array(node.keys)
        left_children, right_children := split_small_array(node.children)

        parent := node.parent
        for i in 0 ..< 100_000 {
            
            if parent == nil {
                parent = new_BPlusTree_branch(Key)
                smarr.push_back(&parent.keys, split_key)
                smarr.push_back(&parent.children, left_node)
                smarr.push_back(&parent.children, right_node)
                break
            } else {
                key_index := linear_search(parent.children, node)
                smarr.inject_at(&parent.keys, key_index+1, split_key)
                smarr.set(&parent.children, key_index, left_node)
                smarr.inject_at(&parent.children, right_node, key_index + 1)
                if smarr.len(parent.keys) == ORDER - 1 {
                    left_node := new_BPlusTree_branch(Key)
                    right_node := new_BPlusTree_branch(Key)

                    split_key := smarr.get(node.keys, cut(ORDER))

                    left_keys, right_keys := split_small_array(node.keys)
                    left_children, right_children := split_small_array(node.children)

                    parent := node.parent
                } else {
                    break
                }
            }
        }
    }

    return .Ok
}