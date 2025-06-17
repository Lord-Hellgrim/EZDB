use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Debug, Display};
use std::io::Write;
use std::slice::{ChunksExact, ChunksExactMut};


use crate::db_structure::{DbKey, DbType};
use crate::{db_structure::{DbValue, HeaderItem}, utilities::*};


pub const ZEROES: [u8;4096] = [0u8;4096];
pub const CHUNK_SIZE: usize = 4096;

pub const ORDER: usize = 10;
pub const ORDER_PLUS_ONE: usize = ORDER + 1;


#[derive(Clone, PartialEq)]
pub struct BPlusTreeNode<T: Null + Clone + Debug + Ord + Eq + Sized> {
    keys: FixedList<T, ORDER>,
    parent: Pointer,
    children: FixedList<Pointer, ORDER_PLUS_ONE>,
    is_leaf: bool,
}

impl<T: Null + Clone + Debug + Ord + Eq + Sized> Null for BPlusTreeNode<T> {
    fn null() -> Self {
        BPlusTreeNode::blank()
    }
}

impl<T: Null + Clone + Debug + Display + Ord + Eq + Sized> Display for BPlusTreeNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "parent: {}\nis_leaf: {}\nkeys: {}\nchildren: {}", self.parent, self.is_leaf, self.keys, self.children)
    }
}


impl <K: Null + Clone + Debug + Ord + Eq + Sized> BPlusTreeNode<K> {
    pub fn new(key: &K, pointer: Pointer) -> BPlusTreeNode<K> {
        let mut keys: FixedList<K, ORDER> = FixedList::new();
        keys.push(key.clone());
        let mut children = FixedList::new();
        children.push(pointer);
        BPlusTreeNode { keys, children, parent: ptr(usize::MAX), is_leaf: true}
    }

    pub fn blank() -> BPlusTreeNode<K> {
        BPlusTreeNode { keys: FixedList::new(), parent: NULLPTR, children: FixedList::new(), is_leaf: false }
    }

    pub fn new_leaf() -> BPlusTreeNode<K> {
        BPlusTreeNode { keys: FixedList::new(), parent: NULLPTR, children: FixedList::new(), is_leaf: true }
    }

    pub fn clear(&mut self) {
        self.children = FixedList::new();
        self.keys = FixedList::new();
    }

    pub fn get_child(&self, key: &K) -> Pointer {
        let mut node_pointer = NULLPTR;
        let mut i = 0;
        while i < self.keys.len() {
            let node_key = self.keys.get(i).unwrap();
            if key >= node_key {
                node_pointer = *self.children.get(i).unwrap();
                break
            }
            i += 1;
        }
        if node_pointer.is_null() {
            node_pointer = *self.children.get(i).unwrap();
        }

        node_pointer  
    }

    pub fn get_right_sibling_pointer(&self) -> Pointer {
        self.children.get_end_slot()
    }

    pub fn set_right_sibling_pointer(&mut self, pointer: Pointer) {
        self.children.set_end_slot(pointer);
    }

    pub fn get_left_sibling_pointer(&self) -> Pointer {
        self.children.get_end_slot()
    }

}




pub struct BPlusTreeMap<K: Null + Clone + Debug + Ord + Eq + Sized> {
    name: KeyString,
    root_node: Pointer,
    nodes: FreeListVec<BPlusTreeNode<K>>,
}

impl<K: Null + Clone + Debug + Ord + Eq + Sized> BPlusTreeMap<K> {
    pub fn new(name: KeyString) -> BPlusTreeMap<K> {
        let mut root: BPlusTreeNode<K> = BPlusTreeNode::blank();
        root.is_leaf = true;
        let mut nodes = FreeListVec::new();
        let root_pointer = nodes.add(root);
        BPlusTreeMap {
            name,
            root_node: root_pointer, 
            nodes,
        }
    }

    pub fn name(&self) -> KeyString {
        self.name
    }

    pub fn find_leaf(&self, key: &K) -> Pointer {

        let mut node = &self.nodes[self.root_node];
        
        let mut node_pointer = self.root_node;
        while !node.is_leaf {
            let mut i = 0;
            while i < node.keys.len() {
                let node_key = node.keys.get(i).unwrap();
                if key >= node_key {
                    node_pointer = *node.children.get(i).unwrap();
                    break
                }
                i += 1;
            }
            if node_pointer.is_null() {
                node_pointer = *node.children.get(i).unwrap();
            }
            node = &self.nodes[node_pointer];
        }
        node_pointer
    }

    pub fn insert(&mut self, key: &K, value: Pointer) {
        let node_pointer = self.find_leaf(key);
        
        self.insert_into_node(key, value, node_pointer);
    }

    fn insert_into_node(&mut self, key: &K, value_pointer: Pointer, target_node_pointer: Pointer) {

        let node = &mut self.nodes[target_node_pointer];
        // println!("node: {}\n{}", node_pointer, node);

        if node.keys.len() > ORDER {
            panic!()
        }

        let index = node.keys.search(key);
        node.keys.insert_at(index, key).unwrap();
        if node.is_leaf {
            node.children.insert_at(index, &value_pointer).unwrap();
            
        } 
        // else {
        //     if index == node.children.len()-1 {
        //         node.children.push(value_pointer);
        //     } else if index < node.children.len() -1 {
        //         node.children.insert_at(index+1, &value_pointer).unwrap();
        //     } else {
        //         panic!("Received an index of {} for a BPlusTree of order {}.", index, ORDER)
        //     }
        // }

        if node.keys.len() == ORDER {
            
            let mut left_node = BPlusTreeNode::new_leaf();
            let mut right_node = BPlusTreeNode::new_leaf();
            // let old_sibling = node.next;
            for i in 0 .. node.keys.len() {
                let k = node.keys.get(i).unwrap().clone();
                let p = node.children.get(i).unwrap().clone();
                if i < cut(ORDER) {
                    left_node.keys.push(k);
                    left_node.children.push(p);
                } else {
                    right_node.keys.push(k);
                    right_node.children.push(p);
                }
            }
            let key = node.keys.get(cut(ORDER)).unwrap().clone();

            let mut parent_pointer = node.parent;
            if parent_pointer == NULLPTR {
                assert!(self.root_node == target_node_pointer);
                let new_root_node: BPlusTreeNode<K> = BPlusTreeNode::blank();
                
                parent_pointer = self.nodes.add(new_root_node);
                self.root_node = parent_pointer;
                left_node.parent = parent_pointer;
                right_node.parent = parent_pointer;
                self.nodes.remove(target_node_pointer);
                
                // right_node.next = old_sibling;
                let right_pointer = self.nodes.add(right_node);
                // left_node.next = right_pointer;
                let left_pointer = self.nodes.add(left_node);
                
                let new_root_node = &mut self.nodes[parent_pointer];
                new_root_node.keys.push(key);
                new_root_node.children.push(left_pointer);
                new_root_node.children.push(right_pointer);
            } else {
                left_node.parent = parent_pointer;
                right_node.parent = parent_pointer;
                self.nodes.remove(target_node_pointer);

                // right_node.next = old_sibling;
                let right_pointer = self.nodes.add(right_node);
                // left_node.next = right_pointer;
                let left_pointer = self.nodes.add(left_node);
                
                // self.update_keys(parent_pointer, left_pointer, &lower_key, &upper_key);
                self.insert_into_node(&key, left_pointer, parent_pointer);
            }
            // drop(node);
        }
    }


    pub fn get(&self, key: &K) -> Pointer {
        let node = self.find_leaf(key);
        if node.is_null() {
            return NULLPTR
        }
        let node = &self.nodes[node];
        let index = node.keys.find(key).unwrap();
        let value = node.children.get(index).unwrap().clone();
        return value

    }

    fn get_left_sibling_pointer(&self, leaf_node: &BPlusTreeNode<K>) -> Pointer {

        let mut parent_node = &self.nodes[leaf_node.parent];
        let leaf_key = leaf_node.keys.get(0).unwrap();
        let mut sibling = NULLPTR;

        let mut path_key = parent_node.keys.get(0).unwrap();
        while path_key >= leaf_key {
            if parent_node.parent.is_null() {
                return NULLPTR
            }
            parent_node = &self.nodes[parent_node.parent];
            path_key = parent_node.keys.get(0).unwrap();
        }

        let mut path_node_pointer = *parent_node.children.get(parent_node.keys.find(path_key).unwrap()).unwrap();
        let mut path_node = &self.nodes[path_node_pointer];

        while !path_node.is_leaf {
            path_node_pointer = path_node.get_child(path_key)
        }

        sibling

    }

    fn get_right_sibling_pointer(&self, leaf_node: &BPlusTreeNode<K>) -> Pointer {

        // leaf_node.next
        NULLPTR
    }

    pub fn remove(&mut self, key: &K) -> Result<(), EzError> {
        let mut current_node_pointer = self.find_leaf(key);
        if current_node_pointer.is_null() {
            return Err(EzError { tag: ErrorTag::Query, text: format!("Key: '{:?}' does not exist in table: '{}'", key, self.name) } )
        }

        let current_node = &mut self.nodes[current_node_pointer];
        let key_index = current_node.keys.find(key).unwrap();
        current_node.keys.remove(key_index);
        current_node.children.remove(key_index);
        
        let mut num_keys = current_node.keys.len();
        while num_keys < cut(ORDER) {
            let current_node = &self.nodes[current_node_pointer];
            let current_parent_pointer = current_node.parent;
            if current_parent_pointer.is_null() {
                break
            }
            let right_sibling_pointer = self.get_right_sibling_pointer(current_node);
            if right_sibling_pointer.is_null() {
                let left_sibling_pointer = self.get_left_sibling_pointer(current_node);
                let left_sibling = &mut self.nodes[left_sibling_pointer];

                let mut temp_keys = FixedList::new();
                let mut temp_children = FixedList::new();
                if left_sibling.keys.len() == cut(ORDER) {
                    let current_node = &mut self.nodes[current_node_pointer];
                    
                    temp_keys.drain(&mut current_node.keys);
                    temp_children.drain(&mut current_node.children);

                    let left_sibling = &mut self.nodes[left_sibling_pointer];

                    left_sibling.keys.drain(&mut temp_keys);
                    left_sibling.children.drain(&mut temp_children);
                    
                    self.nodes.remove(current_node_pointer);
                    let parent = &mut self.nodes[current_parent_pointer];
    
                    parent.keys.pop();
                    parent.children.pop();
                    num_keys = parent.keys.len();
                    current_node_pointer = current_parent_pointer;
    
                } else {
                    let temp_key = left_sibling.keys.pop().unwrap();
                    let temp_child = left_sibling.children.pop().unwrap();
                    
                    let current_node = &mut self.nodes[current_node_pointer];
                    current_node.keys.push(temp_key);
                    current_node.children.push(temp_child);
                    let min_current_key = current_node.keys.get(0).unwrap().clone();

                    let current_parent = &mut self.nodes[current_parent_pointer];
                    let current_index = current_parent.children.find(&current_node_pointer).unwrap();
                    current_parent.keys.set(current_index, min_current_key);

                    break
                }
            } else {
                let right_sibling = &mut self.nodes[right_sibling_pointer];
                let mut temp_keys = FixedList::new();
                let mut temp_children = FixedList::new();
                if right_sibling.keys.len() == ORDER/2 {
                    temp_keys.drain(&mut right_sibling.keys);
                    temp_children.drain(&mut right_sibling.children);
                    
                    let right_parent_pointer = right_sibling.parent;
                    let current_node = &mut self.nodes[current_node_pointer];
                    current_node.keys.drain(&mut temp_keys);
                    current_node.children.drain(&mut temp_children);
                    
                    self.nodes.remove(right_sibling_pointer);
                    let right_parent = &mut self.nodes[right_parent_pointer];
                    let right_index = right_parent.children.find(&right_sibling_pointer).unwrap();
    
                    right_parent.keys.remove(right_index);
                    right_parent.children.remove(right_index);
                    num_keys = right_parent.keys.len();
                    current_node_pointer = right_parent_pointer;
    
                } else {
                    let temp_key = right_sibling.keys.remove(0);
                    let temp_child = right_sibling.children.remove(0);
                    let min_right_key = right_sibling.keys.get(0).unwrap().clone();

                    let right_parent_pointer = right_sibling.parent;
                    let current_node = &mut self.nodes[current_node_pointer];
                    current_node.keys.push(temp_key);
                    current_node.children.push(temp_child);

                    let right_parent = &mut self.nodes[right_parent_pointer];
                    let right_index = right_parent.children.find(&right_sibling_pointer).unwrap();
                    right_parent.keys.set(right_index, min_right_key);

                    break


                }
            }
        }

        Ok(())
    }


}


#[inline]
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
    pub hash_indexes: HashMap<KeyString, HashMap<DbKey, usize>>,
    pub int_indexes: HashMap<KeyString, BPlusTreeMap<i32>>,
    pub text_indexes: HashMap<KeyString, BPlusTreeMap<KeyString>>,
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
            int_indexes: HashMap::new(),
            text_indexes: HashMap::new(),
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

    pub fn add_hash_index(&mut self, index: KeyString) -> Result<(), EzError> {

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

        let mut new_index_tree: HashMap<DbKey, usize> = HashMap::new();
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

    pub fn add_bplustree_index(&mut self, index: KeyString) -> Result<(), EzError> {

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

        for (_primary_key, pointer) in &self.primary_tree {
            match index_type {
                DbType::Int => {
                    let mut new_index_tree: BPlusTreeMap<i32> = BPlusTreeMap::new(index);
                    let row = self.allocator.get_block(ptr(*pointer));
                    let num = i32_from_le_slice(&row[index_offset..index_offset+4]);
                    new_index_tree.insert(&num, ptr(*pointer));
                    self.int_indexes.insert(index, new_index_tree);
                },
                DbType::Text => {
                    let mut new_index_tree: BPlusTreeMap<KeyString> = BPlusTreeMap::new(index);
                    let row = self.allocator.get_block(ptr(*pointer));
                    let ks = KeyString::try_from(&row[index_offset..index_offset+64]).unwrap();
                    new_index_tree.insert(&ks, ptr(*pointer));
                    self.text_indexes.insert(index, new_index_tree);
                },
                DbType::Float => unreachable!(),
            };
        }

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
        let mut test_tree: BPlusTreeMap<usize> = BPlusTreeMap::new(ksf("test"));
        for i in 0..25usize {
            test_tree.insert(&i, ptr(i*10 as usize));
        }

        for node in test_tree.nodes.into_iter() {
            println!("node:\n{}", node);
        }

        // let test_value = test_tree.get(&10);
        // println!("test_value: {}", test_value);

        // let test_leaf = test_tree.find_leaf(&10);
        // println!("test_leaf: {}", test_leaf);
        // let test_leaf = &test_tree.nodes[test_leaf];
        // let left_sibling = test_tree.get_left_sibling_pointer(test_leaf);
        // println!("sibling: {}", left_sibling);
        // let right_sibling = test_tree.get_right_sibling_pointer(test_leaf);
        // println!("sibling: {}", right_sibling);
    }


}