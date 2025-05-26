use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Debug, Display};
use std::io::Write;
use std::slice::{ChunksExact, ChunksExactMut};


use crate::db_structure::{DbKey, DbType};
use crate::{db_structure::{DbValue, HeaderItem}, utilities::*};


pub const ZEROES: [u8;4096] = [0u8;4096];
pub const CHUNK_SIZE: usize = 4096;

pub const ORDER: usize = 20;

const NULL: Pointer = Pointer{pointer: usize::MAX};



#[derive(Clone, PartialEq)]
pub struct BPlusTreeNode<T: Null + Clone + Debug + Ord + Eq + Sized> {
    keys: FixedList<T, 20>,
    parent: Pointer,
    children: FixedList<Pointer, 21>,
    is_leaf: bool,
}

impl<T: Null + Clone + Debug + Ord + Eq + Sized> Display for BPlusTreeNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "children: {:?}", self.keys)
    }
}

impl <T: Null + Clone + Debug + Ord + Eq + Sized> BPlusTreeNode<T> {
    pub fn new(key: &T, pointer: Pointer) -> BPlusTreeNode<T> {
        let mut keys: FixedList<T, 20> = FixedList::new();
        keys.push(key.clone());
        let mut children = FixedList::new();
        children.push(pointer);
        BPlusTreeNode { keys, children, parent: ptr(usize::MAX), is_leaf: true }
    }

    pub fn blank() -> BPlusTreeNode<T> {
        BPlusTreeNode { keys: FixedList::new(), parent: NULL, children: FixedList::new(), is_leaf: false }
    }

    pub fn clear(&mut self) {
        self.children = FixedList::new();
        self.keys = FixedList::new();
    }

}

impl<T: Null + Clone + Debug + Ord + Eq + Sized> Null for BPlusTreeNode<T> {
    fn null() -> Self {
        BPlusTreeNode::blank()
    }
}



pub struct BPlusTree<K: Null + Clone + Debug + Ord + Eq + Sized> {
    root_node: Pointer,
    nodes: FreeListVec<BPlusTreeNode<K>>,
    allocator: Hallocator,
}

impl<K: Null + Clone + Debug + Ord + Eq + Sized> BPlusTree<K> {
    pub fn new(value_size: usize) -> BPlusTree<K> {
        BPlusTree { 
            root_node: NULL, 
            nodes: FreeListVec::new(),
            allocator: Hallocator::new(value_size),
        }
    }

    pub fn find_leaf(&self, key: &K) -> Pointer {
        if self.root_node.is_null() {
            return NULL
        }

        let mut node = &self.nodes[self.root_node];
        let mut node_pointer = NULL;
        let mut i: usize;
        while !node.is_leaf {
            i = 0;
            while i < node.keys.len() {
                if key >= node.keys.get(i).unwrap() {
                    i += 1;
                }
                else {
                    break;
                }
            }
            node_pointer = *node.children.get(i).unwrap();
            node = &self.nodes[node_pointer];
        }

        node_pointer
    }

    pub fn insert(&mut self, key: &K, value: Pointer) {
        let node_pointer = self.find_leaf(key);
        self.insert_into_node(key, value, node_pointer);
    }

    fn insert_into_node(&mut self, key: &K, value: Pointer, node_pointer: Pointer) {

        let node = &mut self.nodes[node_pointer];

        if node.keys.len() > ORDER {
            panic!()
        }

        let index = node.keys.find(key);
        node.keys.insert_before(key, index);
        node.children.insert_before(&value, index);

        if node.keys.len() == ORDER - 1 {
            
            let mut left_node = BPlusTreeNode::blank();
            let mut right_node = BPlusTreeNode::blank();

            for i in 0 .. node.keys.len() {
                let k = node.keys.get(i).unwrap().clone();
                let p = node.children.get(i).unwrap().clone();
                if i < cut(ORDER) {
                    left_node.keys.push(k);
                    left_node.children.push(p);
                } else if i == cut(ORDER) {
                    continue
                } else {
                    right_node.keys.push(k);
                    right_node.children.push(p);
                }
            }

            let key = node.keys.get(cut(ORDER)).unwrap().clone();

            let parent_pointer = node.parent;
            // drop(node);
            self.nodes.remove(node_pointer);

            self.insert_into_node(&key, value, parent_pointer);

        }
    }

    

    

}


pub fn cut(length: usize) -> usize {
    if length % 2 == 0 {
        return length / 2;
    }
    else {
        return length / 2 + 1;
    }
}

pub struct RowTable {
    pub name: KeyString,
    pub header: BTreeSet<HeaderItem>,
    pub primary_tree: BTreeMap<DbKey, usize>,
    pub hash_indexes: HashMap<KeyString, BTreeMap<DbKey, usize>>,
    pub row_size: usize,
    pub allocator: Hallocator,
}

impl RowTable {
    pub fn new(name: KeyString, header: BTreeSet<HeaderItem>) -> RowTable {
        let mut row_size = 0;
        for item in &header {
            row_size += match item.kind {
                DbType::Int => 4,
                DbType::Text => 64,
                DbType::Float => 4,
            }
        }
        RowTable {
            name,
            header,
            primary_tree: BTreeMap::new(),
            hash_indexes: HashMap::new(),
            row_size, 
            allocator: Hallocator::new(row_size) 
        }
    }

    pub fn insert_row(&mut self, key: impl Into<DbKey>, row: &[DbValue]) -> Result<(), EzError> {

        let mut i = 0;
        let mut checked_row = Vec::new();
        for item in &self.header {
            match item.kind {
                DbType::Int => {
                    if !row[i].is_int() {
                        return Err(EzError { tag: ErrorTag::Query, text: format!("Column: {} can only contain values of type Int", item.name) })
                    }
                    checked_row.extend_from_slice(&row[i].to_i32().to_le_bytes());
                },
                DbType::Text => {
                    if !row[i].is_text() {
                        return Err(EzError { tag: ErrorTag::Query, text: format!("Column: {} can only contain values of type Text", item.name) })
                    }
                    checked_row.extend_from_slice(row[i].to_keystring().as_bytes());

                },
                DbType::Float => {
                    if !row[i].is_float() {
                        return Err(EzError { tag: ErrorTag::Query, text: format!("Column: {} can only contain values of type Float", item.name) })
                    }
                    checked_row.extend_from_slice(&row[i].to_f32().to_le_bytes());

                },
            }

            i += 1;
        }

        let key: DbKey = key.into();

        let pointer = self.allocator.alloc();
        match self.allocator.get_block_mut(pointer).write(&checked_row) {
            Ok(_) => (),
            Err(e) => return Err(EzError { tag: ErrorTag::Structure, text: e.to_string() }),
        };
        self.primary_tree.insert(key, pointer.pointer);


        let mut offset: usize = match key {
            DbKey::Int(_) => 4,
            DbKey::Text(_) => 64,
        };
        for item in &self.header {
            if self.hash_indexes.contains_key(&item.name) {
                match item.kind {
                    crate::db_structure::DbType::Int => {
                        let num = i32_from_le_slice(&checked_row[offset..offset+4]);
                        let index_tree = self.hash_indexes.get_mut(&item.name).expect("Will never panic because of previous check");
                        index_tree.insert(num.into(), pointer.pointer);
                        offset += 4;
                    },
                    crate::db_structure::DbType::Float => {
                        unreachable!("There cannot be a float index on a table. If we got here, there has been a consistency error in the code. Alert the maintainers asap.")
                    },
                    crate::db_structure::DbType::Text => {
                        let num = KeyString::try_from(&checked_row[offset..offset+64]).unwrap();
                        let index_tree = self.hash_indexes.get_mut(&item.name).expect("Will never panic because of previous check");
                        index_tree.insert(num.into(), pointer.pointer);
                        offset += 64;
                    },
                }
            }
        }

        Ok(())
    }

    pub fn add_index(&mut self, index: KeyString) -> Result<(), EzError> {

        let mut index_is_in_header = false;
        let mut index_offset = 0;
        let mut index_type: DbType = DbType::Int;
        for item in &self.header {

            if index == item.name {
                index_is_in_header = true;
                index_type = item.kind;
                match index_type {
                    DbType::Int => (),
                    DbType::Text => (),
                    DbType::Float => return Err(EzError { tag: ErrorTag::Query, text: format!("Cannot have indexes on floats") }),
                };
                break
            }
            index_offset += item.offset();
        }
        
        if !index_is_in_header {
            return Err(EzError { tag: ErrorTag::Query, text: format!("There is no column: {} in table: {}", index, self.name) })
        }

        let mut new_index_tree: BTreeMap<DbKey, usize> = BTreeMap::new();
        for (_primary_key, pointer) in &self.primary_tree {
            match index_type {
                DbType::Int => {
                    let row = self.allocator.get_block(ptr(*pointer));
                    let num = i32_from_le_slice(&row[index_offset..index_offset+4]);
                    new_index_tree.insert(num.into(), *pointer);
                },
                DbType::Text => {
                    let row = self.allocator.get_block(ptr(*pointer));
                    let ks = KeyString::try_from(&row[index_offset..index_offset+4]).unwrap();
                    new_index_tree.insert(ks.into(), *pointer);
                },
                DbType::Float => unreachable!(),
            };
        }

        self.hash_indexes.insert(index, new_index_tree);

        Ok(())
    }

    pub fn iter(&self) -> RowTableIterator {
        RowTableIterator {
            chunks: self.allocator.buffer.chunks_exact(self.row_size),
        }
    }

    pub fn iter_mut(&mut self) -> RowTableIteratorMut {
        RowTableIteratorMut {
            chunks: self.allocator.buffer.chunks_exact_mut(self.row_size),
        }
    }

}

pub struct RowTableIterator<'a> {
    chunks: ChunksExact<'a, u8>,
}

impl<'a> Iterator for RowTableIterator<'a> {
    type Item = &'a [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.chunks.nth(n)
    }
}

pub struct RowTableIteratorMut<'a> {
    chunks: ChunksExactMut<'a, u8>,
}

impl<'a> Iterator for RowTableIteratorMut<'a> {
    type Item = &'a mut [u8];

    fn next(&mut self) -> Option<Self::Item> {
        self.chunks.next()
    }

    fn nth(&mut self, n: usize) -> Option<Self::Item> {
        self.chunks.nth(n)
    }
}



#[cfg(test)]
mod tests {

    use crate::db_structure::TableKey;

    use super::*;

    #[test]
    fn test_block_allocator() {
        let mut buffer = BlockAllocator::new(64).unwrap();
        let mut pointers = Vec::new();
        let mut nums = Vec::new();
        for i in 0..1000 {
            let slice = buffer.alloc();
            unsafe { (slice.pointer as *mut usize).write(i) };
            pointers.push(slice.pointer);
            nums.push(i);
        }

        println!("num sum: {}", nums.iter().sum::<usize>());

        let mut sum = 0;
        for pointer in pointers {
            let num = unsafe { *(pointer as *mut usize) };
            sum += num;
        }

        println!("pointer_sum: {}", sum);
    }

    #[test]
    fn test_hallocator() {
        let mut buffer = Hallocator::new(64);
        let mut pointers = Vec::new();
        let mut nums = Vec::new();
        for i in 0..10 as i32 {
            let pointer = buffer.alloc();
            pointers.push(pointer);
            let pointer = buffer.get_block_mut(pointer);
            unsafe { (std::mem::transmute::<*mut u8, *mut KeyString>(pointer.as_mut_ptr())).write(ksf(&format!("Hello_world: {}!", i))) };
            nums.push(i);
        }

        println!("num sum: {}", nums.iter().sum::<i32>());

        // let mut sum = 0;
        for pointer in pointers {
            let ks = buffer.read_keystring(pointer, 0);
            println!("{}", ks);
        }

        // println!("pointer_sum: {}", sum);
    }

    #[test]
    fn test_row_table() {
        let mut header = BTreeSet::new();
        header.insert(HeaderItem {
            name: ksf("ints"),
            kind: DbType::Int,
            key: TableKey::Primary,
        });
        header.insert(HeaderItem {
            name: ksf("texts"),
            kind: DbType::Text,
            key: TableKey::None,
        });
        header.insert(HeaderItem {
            name: ksf("floats"),
            kind: DbType::Float,
            key: TableKey::None,
        });
        let mut table = RowTable::new( ksf("test_table"), header);

        for i in 0..10 as i32 {
            let mut row = Vec::new();
            row.push(DbValue::Float(i as f32));
            row.push(DbValue::Int(i));
            row.push(DbValue::Text(ksf(&i.to_string())));

            table.insert_row(i, &row).unwrap();
        }

        for item in table.iter() {
            let mut offset = 0;
            for head in &table.header {
                match head.kind {
                    DbType::Int => {
                        let int = read_i32(item, offset);
                        println!("{int}");
                        offset += 4;
                    },
                    DbType::Text => {
                        let ksf = read_keystring(item, offset);
                        println!("{ksf}");
                        offset += 64;
                    },
                    DbType::Float => {
                        let int = read_f32(item, offset);
                        println!("{int}");
                        offset += 4;
                    },
                }
            }
        }

    }


    #[test]
    fn test_BPlusTree() {
        let sum = 5;
        println!("sum: {}", sum);
    }


}