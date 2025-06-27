use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Debug, Display};
use std::io::Write;
use std::slice::{ChunksExact, ChunksExactMut};


use crate::db_structure::{DbKey, DbType};
use crate::{db_structure::{DbValue, HeaderItem}, utilities::*};


pub const ZEROES: [u8;4096] = [0u8;4096];
pub const CHUNK_SIZE: usize = 4096;

pub const ORDER: usize = 5;
pub const ORDER_PLUS_ONE: usize = ORDER + 1;


#[derive(Clone, PartialEq, Debug)]
pub struct BPlusTreeNode<T: Null + Clone + Copy + Debug + Ord + Eq + Sized + Display> {
    keys: FixedList<T, ORDER>,
    parent: Pointer,
    children: FixedList<Pointer, ORDER_PLUS_ONE>,
    is_leaf: bool,
}

impl<T: Null + Clone + Copy + Debug + Ord + Eq + Sized + Display> Null for BPlusTreeNode<T> {
    fn null() -> Self {
        BPlusTreeNode::new_branch()
    }
}

impl<T: Null + Clone + Copy + Debug + Display + Ord + Eq + Sized> Display for BPlusTreeNode<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_leaf {
            writeln!(f, "LEAF:\nparent: {}\nis_leaf: {}\nkeys: {}\nchildren: {}\nRight_sibling: {}", self.parent, self.is_leaf, self.keys, self.children, self.get_right_sibling_pointer())
        } else {
            writeln!(f, "BRANCH:\nparent: {}\nis_leaf: {}\nkeys: {}\nchildren: {}\n", self.parent, self.is_leaf, self.keys, self.children)

        }
    }
}


impl <T: Null + Clone + Copy + Debug + Ord + Eq + Sized + Display + Display> BPlusTreeNode<T> {
    pub fn new(key: &T, pointer: Pointer) -> BPlusTreeNode<T> {
        let mut keys: FixedList<T, ORDER> = FixedList::new();
        keys.push(key.clone());
        let mut children = FixedList::new();
        children.push(pointer);
        BPlusTreeNode { keys, children, parent: ptr(usize::MAX), is_leaf: true }
    }

    pub fn new_branch() -> BPlusTreeNode<T> {
        BPlusTreeNode { keys: FixedList::new(), parent: NULLPTR, children: FixedList::new(), is_leaf: false }
    }

    pub fn new_leaf() -> BPlusTreeNode<T> {
        BPlusTreeNode { keys: FixedList::new(), parent: NULLPTR, children: FixedList::new(), is_leaf: true }
    }

    pub fn clear(&mut self) {
        self.children = FixedList::new();
        self.keys = FixedList::new();
    }

    fn get_right_sibling_pointer(&self) -> Pointer {
        self.children.get_end_slot()
    }

    fn set_right_sibling_pointer(&mut self, pointer: Pointer) {
        self.children.set_end_slot(pointer);
    }

}




pub struct BPlusTreeMap<K: Null + Clone + Copy + Debug + Ord + Eq + Sized + Display + Display> {
    name: KeyString,
    root_node: Pointer,
    nodes: FreeListVec<BPlusTreeNode<K>>,
}

impl<K: Null + Clone + Copy + Debug + Ord + Eq + Sized + Display> Display for BPlusTreeMap<K> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        
        let mut printer = String::new();

        for (i, node) in self.nodes.into_iter().enumerate() {
            printer.push_str(&format!("{} - {} - \n", i, node));
        }
        
        writeln!(f, "{}", printer)
    }
}

impl<K: Null + Clone + Copy + Debug + Ord + Eq + Sized + Display> BPlusTreeMap<K> {
    pub fn new(name: KeyString) -> BPlusTreeMap<K> {
        let mut root: BPlusTreeNode<K> = BPlusTreeNode::new_branch();
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
        let mut i: usize;
        while !node.is_leaf {
            i = 0;
            while i < node.keys.len() {
                if key >= &node.keys[i] {
                    i += 1;
                }
                else {
                    break;
                }
            }
            node_pointer = node.children[i];
            if node_pointer.is_null() {
                println!("{}", self);
            }
            node = &self.nodes[node_pointer];
        }
        node_pointer
    }

    pub fn insert(&mut self, key: &K, value: Pointer) {
        let node_pointer = self.find_leaf(key);
        
        self.insert_into_leaf(key, value, node_pointer);
    }

    fn insert_into_leaf(&mut self, key: &K, value_pointer: Pointer, target_node_pointer: Pointer) {

        let node = &mut self.nodes[target_node_pointer];
        // println!("node: {}\n{}", node_pointer, node);

        if node.keys.len() > ORDER {
            panic!()
        }

        let index = node.keys.search(key);
        node.keys.insert_at(index, key).unwrap();
        if node.is_leaf {
            node.children.insert_at(index, &value_pointer).unwrap();
            
        } else {
            if index == node.children.len()-1 {
                node.children.push(value_pointer);
            } else if index < node.children.len() -1 {
                node.children.insert_at(index+1, &value_pointer).unwrap();
            } else {
                panic!("Received an index of {} for a BPlusTree of order {}.", index, ORDER)
            }
        }

        if node.keys.len() == ORDER {
            
            let mut left_node: BPlusTreeNode<K>;
            let mut right_node: BPlusTreeNode<K>;
            if node.is_leaf {
                left_node = BPlusTreeNode::new_leaf();
                right_node = BPlusTreeNode::new_leaf();
                
            } else {
                left_node = BPlusTreeNode::new_branch();
                right_node = BPlusTreeNode::new_branch();
            }

            for i in 0 .. node.keys.len() {
                let k = node.keys[i];
                let p = node.children[i];
                if i < cut(ORDER) {
                    left_node.keys.push(k);
                    left_node.children.push(p);
                } else {
                    right_node.keys.push(k);
                    right_node.children.push(p);
                }
            }
            let key = node.keys[cut(ORDER)];

            let mut parent_pointer = node.parent;
            if parent_pointer == NULLPTR {
                assert!(self.root_node == target_node_pointer);
                let new_root_node: BPlusTreeNode<K> = BPlusTreeNode::new_branch();
                
                parent_pointer = self.nodes.add(new_root_node);
                self.root_node = parent_pointer;
                left_node.parent = parent_pointer;
                right_node.parent = parent_pointer;
                self.nodes.remove(target_node_pointer);
                
                let left_pointer = self.nodes.add(left_node);
                let right_pointer = self.nodes.add(right_node);

                let left_node = &mut self.nodes[left_pointer];
                left_node.set_right_sibling_pointer(right_pointer);
                
                let new_root_node = &mut self.nodes[parent_pointer];
                new_root_node.keys.push(key);
                new_root_node.children.push(left_pointer);
                new_root_node.children.push(right_pointer);
            } else {
                left_node.parent = parent_pointer;
                right_node.parent = parent_pointer;
                
                let right_pointer = self.nodes.add(right_node);
                left_node.set_right_sibling_pointer(right_pointer);
                self.nodes[target_node_pointer] = left_node;
                
                // let left_pointer = target_node_pointer;

                // let left_node = &mut self.nodes[left_pointer];
                
                // self.update_keys(parent_pointer, left_pointer, &lower_key, &upper_key);
                self.insert_into_branch(&key, right_pointer, parent_pointer);
            }
            // drop(node);
        }
    }

    fn insert_into_branch(&mut self, key: &K, value_pointer: Pointer, target_node_pointer: Pointer) {
        let node = &mut self.nodes[target_node_pointer];

        if node.keys.len() > ORDER {
            panic!()
        }

        let index = node.keys.search(key);
        node.keys.insert_at(index, key).unwrap();
        
        node.children.insert_at(index+1, &value_pointer).unwrap();

        if node.keys.len() == ORDER {
            
            let mut left_node = BPlusTreeNode::new_branch();
            let mut right_node = BPlusTreeNode::new_branch();

            let mut i = 0;
            while i < node.keys.len() {
                let k = node.keys[i];
                let p = node.children[i];
                if i < cut(ORDER) {
                    left_node.keys.push(k);
                    left_node.children.push(p);
                } else if i == cut(ORDER){
                    left_node.children.push(p);
                } else if i > cut(ORDER) {
                    right_node.keys.push(k);
                    right_node.children.push(p);
                }

                i += 1;
            }
            let p = node.children[i];
            right_node.children.push(p);

            let key = node.keys[cut(ORDER)];

            let mut parent_pointer = node.parent;
            if parent_pointer == NULLPTR {
                assert!(self.root_node == target_node_pointer);
                let new_root_node: BPlusTreeNode<K> = BPlusTreeNode::new_branch();
                
                parent_pointer = self.nodes.add(new_root_node);
                self.root_node = parent_pointer;
                left_node.parent = parent_pointer;
                right_node.parent = parent_pointer;
                self.nodes.remove(target_node_pointer);
                
                let left_pointer = self.nodes.add(left_node);
                let right_pointer = self.nodes.add(right_node);

                let new_root_node = &mut self.nodes[parent_pointer];
                new_root_node.keys.push(key);
                new_root_node.children.push(left_pointer);
                new_root_node.children.push(right_pointer);
            } else {
                left_node.parent = parent_pointer;
                right_node.parent = parent_pointer;
                self.nodes[target_node_pointer] = left_node;
                
                let _left_pointer = target_node_pointer;
                let right_pointer = self.nodes.add(right_node);

                // self.update_keys(parent_pointer, left_pointer, &lower_key, &upper_key);
                self.insert_into_branch(&key, right_pointer, parent_pointer);
            }
        }
    }

    pub fn get(&self, key: &K) -> Pointer {
        let node = self.find_leaf(key);
        if node.is_null() {
            return NULLPTR
        }
        let node = &self.nodes[node];
        
        match node.keys.find(key) {
            Some(index) => {
                return node.children.get(index).unwrap().clone();
            },
            None => return NULLPTR,
        }

    }

    fn get_left_sibling_pointer(&self, leaf_node: &BPlusTreeNode<K>) -> Pointer {
        
        let parent_node = &self.nodes[leaf_node.parent];
        let leaf_key = leaf_node.keys[0];
        let key_index = parent_node.keys.search(&leaf_key);

        if key_index == 0 {
            println!("{}", self);
        }

        return parent_node.children[key_index]
        
    }


    pub fn delete_key(&mut self, key: &K) -> Result<(), EzError> {
        let mut current_node_pointer = self.find_leaf(key);
        if current_node_pointer.is_null() {
            return Err(EzError { tag: ErrorTag::Query, text: format!("Key: '{:?}' does not exist in table: '{}'", key, self.name) } )
        }

        let current_node = &mut self.nodes[current_node_pointer];
        let key_index = match current_node.keys.find(key) {
            Some(index) => index,
            None => {
                println!("{}", self);
                println!("Couldn't find key: '{}' in node: '{}'", key, current_node_pointer);
                panic!()
            }
        };
        current_node.keys.remove(key_index);
        current_node.children.remove(key_index);
        
        if current_node.parent.is_null() {
            return Ok(())
        }

        let mut num_keys = current_node.keys.len();
        while num_keys < cut(ORDER) {
            println!("num_keys: {}", num_keys);
            let current_node = &self.nodes[current_node_pointer];
            if current_node.parent.is_null() {
                return Ok(())
            }
            let mut right_sibling_pointer = current_node.get_right_sibling_pointer();
            if right_sibling_pointer.is_null() {
                /*WHAT IF WE HAVE THE RIGHTMOST NODE */

                let left_sibling_pointer = self.get_left_sibling_pointer(current_node);
                if left_sibling_pointer.is_null() {
                    panic!("If the parent is not null but both the left and right sibling pointers are null then the tree is broken")
                }
                right_sibling_pointer = current_node_pointer;
                current_node_pointer = left_sibling_pointer;

            }

            let right_sibling = &mut self.nodes[right_sibling_pointer];
            let mut temp_keys = FixedList::new();
            let mut temp_children = FixedList::new();
            
            if right_sibling.keys.len() == cut(ORDER) {
                temp_keys.drain(&mut right_sibling.keys);
                temp_children.drain(&mut right_sibling.children);
                
                let right_parent_pointer = right_sibling.parent;
                let right_sibling_right_sibling = right_sibling.get_right_sibling_pointer();
                let current_node = &mut self.nodes[current_node_pointer];
                current_node.keys.drain(&mut temp_keys);
                current_node.children.drain(&mut temp_children);
                
                current_node.set_right_sibling_pointer(right_sibling_right_sibling);

                self.nodes.remove(right_sibling_pointer);
                let right_parent = &mut self.nodes[right_parent_pointer];
                let right_index = right_parent.children.find(&right_sibling_pointer).unwrap();

                right_parent.keys.remove(right_index-1);
                right_parent.children.remove(right_index);
                num_keys = right_parent.keys.len();
                current_node_pointer = right_parent_pointer;
                println!("{}", self);

            } else {

                let right_sibling = &mut self.nodes[right_sibling_pointer];
                let parent_node_pointer = right_sibling.parent;

                let temp_key = right_sibling.keys.remove(0);
                let temp_child = right_sibling.children.remove(0);
                let new_key = right_sibling.keys.get(0).unwrap().clone();
                
                let current_node = &mut self.nodes[current_node_pointer];

                current_node.keys.push(temp_key);
                current_node.children.push(temp_child);


                let parent_node = &mut self.nodes[parent_node_pointer];
                let key_index = match parent_node.children.find(&right_sibling_pointer){
                    Some(idx) => idx - 1,
                    None => {
                        println!("{}", self);
                        panic!()
                    },
                };
                
                *parent_node.keys.get_mut(key_index).unwrap() = new_key;

            }
        }

        Ok(())
    }


}

pub fn check_tree_height<K: Null + Clone + Copy + Debug + Ord + Eq + Sized + Display>(tree: &BPlusTreeMap<K>) -> (bool, String) {

    let mut node = &tree.nodes[tree.root_node];
        
    let mut node_pointer: Pointer;
    let mut i = 0;
    while !node.is_leaf {
        i += 1;
        node_pointer = node.children[0];
        node = &tree.nodes[node_pointer];
    }

    let leftmost_node = node;
    let mut node_pointer = leftmost_node.get_right_sibling_pointer();
    
    while !node_pointer.is_null() {
        node = &tree.nodes[node_pointer];
        node_pointer = node.get_right_sibling_pointer();
        let mut j = 0;
        let mut backtrack_node = node;
        while !backtrack_node.parent.is_null() {
            backtrack_node = &tree.nodes[backtrack_node.parent];
            j += 1;
        }
        if j != i {
            return (false, format!("Node: '{}' is at height '{}' but the leftmost node is of height '{}'", node_pointer, j, i))
        }
    }

    (true, "ALL GOOD".to_owned())
}

pub fn check_tree_ordering<K: Null + Clone + Copy + Debug + Display + Ord + Eq + Sized>(tree: &BPlusTreeMap<K>) -> (bool, String) {

    let mut node = &tree.nodes[tree.root_node];

    let mut node_pointer = NULLPTR;
    while !node.is_leaf {
        node_pointer = node.children[0];
        node = &tree.nodes[node_pointer];
    }

    let mut last_key = node.keys[0];
    while !node_pointer.is_null() {
        node = &tree.nodes[node_pointer];
        for key in node.keys.iter() {
            if &last_key > key {
                return (false, format!("Found out of order key in node: {}. Key '{}' is larger than key: '{}'", node_pointer, last_key, key))
            } else {
                last_key = key.clone();
            }
        }
        node_pointer = node.get_right_sibling_pointer();
    }

    (true, "ALL GOOD".to_owned())
}

pub fn check_tree_accuracy(tree: &BPlusTreeMap<u32>) -> (bool, String) {

    let mut node = &tree.nodes[tree.root_node];

    let mut node_pointer = NULLPTR;
    while !node.is_leaf {
        node_pointer = node.children[0];
        node = &tree.nodes[node_pointer];
    }

    while !node_pointer.is_null() {
        node = &tree.nodes[node_pointer];
        for i in 0..node.keys.len() {
            let key = node.keys[i];
            let value = node.children[i];
            if value != ptr(key as usize) {
                return (false, format!("In node: '{}' - Key '{}' points to Pointer '{}'", node_pointer, key, value))
            }
        }
        node_pointer = node.get_right_sibling_pointer();
    }


    (true, format!("ALL GOOD"))

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

    use fnv::FnvHashSet;

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

        let test_value = test_tree.get(&10);
        println!("test_value: {}", test_value);

        test_tree.delete_key(&10).unwrap();

        let test_value = test_tree.get(&10);
        println!("test_value: {}", test_value);

        // let test_leaf = test_tree.find_leaf(&10);
        // println!("test_leaf: {}", test_leaf);
        // let test_leaf = &test_tree.nodes[test_leaf];
        // let left_sibling = test_tree.get_left_sibling_pointer(test_leaf);
        // println!("sibling: {}", left_sibling);
        // let right_sibling = test_tree.get_right_sibling_pointer(test_leaf);
        // println!("sibling: {}", right_sibling);
    }


    #[test]
    fn test_BPlusTree_proper() {
        let mut tree: BPlusTreeMap<u32> = BPlusTreeMap::new(ksf("test"));

        let mut inserts = FnvHashSet::default();
        for _ in 0..100 {
            let insert: u32 = rand::random_range(0..1000);
            inserts.insert(insert);
        }
        
        // let mut log = Vec::new();
        let mut inserted = Vec::new();
        for count in 0..10_000 {
            let item = pop_from_hashset(&mut inserts);
            if item.is_none() {
                break
            } else {
                let item = item.unwrap();
                tree.insert(&item, ptr(item as usize));
                println!("+{},  {}", item, count);
                inserted.push(item);
            }
            if rand::random_bool(0.1) {
                let delete = inserted.swap_remove(rand::random_range(0..inserted.len()));
                tree.delete_key(&delete).unwrap();
                println!("-{},  {}", delete, count);
            }
        }

        let (height_is_correct, height_error) = check_tree_height(&tree);
        let (order_is_correct, order_error) = check_tree_ordering(&tree);
        let (tree_is_accurate, accuracy_error) = check_tree_accuracy(&tree);

        let mut we_should_panic = false;
        if !height_is_correct {
            println!("tree:\n{}", tree);
            println!("{}", height_error);
            we_should_panic = true;
        }

        if !order_is_correct {
            println!("tree:\n{}", tree);
            println!("{}", order_error);

            we_should_panic = true;
        }

        if !tree_is_accurate {
            println!("tree:\n{}", tree);
            println!("{}", accuracy_error);

            we_should_panic = true;
        }

        if we_should_panic {
            panic!()
        }

    }


}