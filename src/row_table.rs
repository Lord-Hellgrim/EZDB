use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt::{Debug, Display};
use std::io::Write;
use std::slice::{ChunksExact, ChunksExactMut};


use crate::db_structure::{DbKey, DbType};
use crate::ezql::Query;
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
        children[0] = pointer;
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
    head_node: Option<Pointer>,
    nodes: FreeListVec<BPlusTreeNode<K>>,
    allocator: Hallocator,
}

impl<K: Null + Clone + Debug + Ord + Eq + Sized> BPlusTree<K> {
    pub fn new(value_size: usize) -> BPlusTree<K> {
        BPlusTree { 
            head_node: None, 
            nodes: FreeListVec::new(),
            allocator: Hallocator::new(value_size),
        }
    }

    fn find_leaf(&self, node_pointer: Pointer, key: &K) -> Option<&BPlusTreeNode<K>> {
        let mut node = &self.nodes[node_pointer.pointer];
        if node.keys.len() == 0 {
    
            return None;
          }
          while !node.is_leaf {
            let mut i = 0;
            while i < node.keys.len() {
              if key >= &node.keys[i] {
                i += 1;
              }
              else {
                break
            };
            }
            node = &self.nodes[node.children[i].pointer];
          }
          return Some(node);
    }

    fn find_leaf_mut(&mut self, node_pointer: Pointer, key: &K) -> Option<usize> {
        let node = &self.nodes[node_pointer.pointer];
        let mut pointer: usize = usize::MAX;
        if node.keys.len() == 0 {
    
            return None;
          }
          
          while !node.is_leaf {
            let mut i = 0;
            while i < node.keys.len() {
              if key >= &node.keys[i] {
                i += 1;
              }
              else {
                break
            };
            }
            pointer = node.children[i].pointer;
        }
        return Some(pointer);
        
    }

    pub fn find(&self, key: &K) -> Option<(Pointer, &BPlusTreeNode<K>)> {

        let head = self.head_node?;

        let node = self.find_leaf(head, &key)?;

        for index in 0..node.keys.len() {
            if &node.keys[index] == key {
                return Some((node.children[index], node))
            }
            
        }

        None
    }

    pub fn insert(&mut self, key: &K, value: &[u8]) {

        assert!(value.len() == self.allocator.block_size()); 
        
        let record_pointer = self.find(&key);
        if record_pointer.is_some() {
            let (record_pointer, _) = record_pointer.unwrap();
            self.allocator.get_block_mut(record_pointer).copy_from_slice(value);
            return 
        }
        
        let new_pointer = self.allocator.alloc();
        self.allocator.get_block_mut(new_pointer).copy_from_slice(value);
      
        if self.head_node.is_none() {
            self.head_node = Some(new_pointer);
            return
        }
      
        let leaf_pointer = self.find_leaf_mut(new_pointer, &key).unwrap();
        let node_keys_len = self.nodes[leaf_pointer].keys.len();
      
        if node_keys_len < ORDER - 1 {
          self.insert_into_leaf(leaf_pointer, key, new_pointer);
          return
        }
      
        // self.insertIntoLeafAfterSplitting(leaf_pointer, key, new_pointer);

      }

    fn insert_into_leaf(&mut self, leaf: usize, key: &K, value_pointer: Pointer) {
        let mut insertion_point = 0;
        let leaf = &mut self.nodes[leaf];
        while insertion_point < leaf.keys.len() && &leaf.keys[insertion_point] < key {
            insertion_point += 1;
        }
        leaf.keys.insert_before(key, insertion_point);

        leaf.children[insertion_point] = value_pointer;
    }


    pub fn insertIntoLeafAfterSplitting(&mut self, leaf_pointer: usize, key: &K, pointer: Pointer) {
        // node *new_leaf;
        // int *temp_keys;
        // void **temp_pointers;
        // int insertion_index, split, new_key, i, j;
      
        let mut new_leaf = BPlusTreeNode::blank();
        let mut temp_keys: FixedList<K, 20> = FixedList::new();
        
        let mut temp_pointers: FixedList<Pointer, 20> = FixedList::new();
        
        let mut leaf = &mut self.nodes[leaf_pointer];

        let mut insertion_index = 0;
        while insertion_index < ORDER - 1 && &leaf.keys[insertion_index] < key {
            insertion_index += 1;
        }
        
        let mut j = 0;
        for i in 0..leaf.keys.len() {
          if j == insertion_index {
              j += 1;
          }
          temp_keys[j] = leaf.keys[i].clone();
          temp_pointers[j] = leaf.children[i];

          j+= 1;
        }
      
        temp_keys[insertion_index] = key.clone();
        temp_pointers[insertion_index] = pointer;
      
        leaf.clear();
      
        let split = cut(ORDER - 1);
      
        for i in 0..split {
          leaf.children.push(temp_pointers[i]);
          leaf.keys.push(temp_keys[i].clone());
        }
        
        for i in split..ORDER {
          new_leaf.children.push(temp_pointers[i]);
          new_leaf.keys.push(temp_keys[i].clone());
        }

        
        new_leaf.children[ORDER - 1] = leaf.children[ORDER - 1];
        
        for i in leaf.keys.len()..ORDER - 1 {
            leaf.children[i] = NULL;
        }
        for i in new_leaf.keys.len() .. ORDER - 1 {
            new_leaf.children[i] = NULL;
        }
        
        new_leaf.parent = leaf.parent;
        let new_key = new_leaf.keys[0].clone();
        
        drop(leaf);
        let new_leaf_pointer = self.nodes.add(new_leaf);
        let leaf = &mut self.nodes[leaf_pointer];

        leaf.children[ORDER - 1] = ptr(new_leaf_pointer);
      
        self.insertIntoParent(leaf_pointer, &new_key, new_leaf_pointer);
    }


    fn insertIntoParent(&mut self, left: usize, key: &K, right: usize) -> Pointer {
        let left_node = &self.nodes[left];
        
        let parent_pointer = left_node.parent;
      
        if parent_pointer == NULL {
            self.insertIntoNewRoot(left, &key, right);
        }
      
        let left_index = self.getLeftIndex(parent_pointer.pointer, left);

        let parent = &mut self.nodes[parent_pointer.pointer];
      
        if parent.keys.len() < ORDER - 1 {
            self.insertIntoNode(parent_pointer.pointer, left_index, key, right);
        }
      
        insertIntoNodeAfterSplitting(root, parent, left_index, key, right);
    }


    fn insertIntoNewRoot(&mut self, left: usize, key: &K, right: usize) {

        let mut root = BPlusTreeNode::blank();

        root.keys.push(key.clone());
        root.children.push(ptr(left));
        root.children.push(ptr(right));
        root.parent = NULL;

        let root_pointer = self.nodes.add(root);
        let left = &mut self.nodes[left];
        left.parent = ptr(root_pointer);
        drop(left);
        
        let right = &mut self.nodes[right];
        right.parent = ptr(root_pointer);    
    }

    fn getLeftIndex(&self, parent: usize, left: usize) -> usize {
        let mut left_index = 0;
        let parent = &self.nodes[parent];
        while left_index <= parent.keys.len() && parent.children[left_index] != ptr(left) {
            left_index += 1;
        }
        return left_index;
    }

    fn insertIntoNode(&mut self, n: usize, left_index: usize, key: &K, right: usize) {
        
        let n = &mut self.nodes[n];
        
        n.children.insert_before(&ptr(right), left_index + 1);
        n.keys.insert_before(key, left_index);
        
    }

    fn insertIntoNodeAfterSplitting(&mut self, old_node_pointer: usize, left_index: usize, key: &K, right: usize) {
        // int i, j, split, k_prime;
        // node *new_node, *child;
        // int *temp_keys;
        // node **temp_pointers;

        let mut temp_children: FixedList<Pointer, ORDER> = FixedList::new();
        
        let mut temp_keys: FixedList<K, ORDER> = FixedList::new();

        let old_node = &mut self.nodes[old_node_pointer];
        let mut j = 0;
        for i in 0 .. old_node.keys.len() + 1 {
            if j == left_index + 1 {
                j += 1;
            }
            temp_children[j] = old_node.children[i].clone();

            j += 1;
        }

        let mut j = 0;
        for i in 0 .. old_node.keys.len() {
            if j == left_index {
                j += 1;
            }
            temp_keys[j] = old_node.keys[i].clone();
            j += 1;
        }

        temp_children[left_index + 1] = ptr(right);
        temp_keys[left_index] = key.clone();

        let split = cut(ORDER);
        let new_node = BPlusTreeNode::blank();
        
        for i in 0 .. split - 1 {
            old_node.children.push(temp_children[i].clone());
            old_node.keys.push(temp_keys[i].clone());
        }
        old_node.children.push(temp_children[split-1].clone());
        let k_prime = temp_keys[split - 1];

        let mut j = 0;
        for i in split .. ORDER {
            new_node.children.push(temp_children[i].clone());
            new_node.keys.push(temp_keys[i].clone());
            j += 1;
        }
        new_node.children.push(temp_children[ORDER].clone());
        
        new_node.parent = old_node.parent;
        for i in 0 ..= new_node.keys.len() {
            let mut child = selfnew_node.children[i];
            child.parent = new_node;
        }

        return insertIntoParent(root, old_node, k_prime, new_node);
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