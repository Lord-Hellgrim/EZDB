
type NodeIndex = usize;

pub struct EzTreeNode {
    data_pointers: [u64;10],
    child_nodes: [NodeIndex; 11],
    
}


pub struct EzTree {
    memory: Vec<u8>,
    nodes: Vec<EzTreeNode>,
    root: EzTreeNode,

}