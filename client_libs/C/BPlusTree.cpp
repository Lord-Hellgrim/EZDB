// Searching on a B+ tree in C++

#include <iostream>
#include <vector>
#include <cmath>
#include <string>
using namespace std;

// node creation
class Node {
public:
    int order;
    vector<string> values;
    vector<Node*> children; // for internal nodes
    vector<vector<string>> keys; // for leaf nodes
    Node* nextKey;
    Node* parent;
    bool check_leaf;
    Node(int order) {
        this->order = order;
        this->nextKey = nullptr;
        this->parent = nullptr;
        this->check_leaf = false;
    }
    
    // insert at the leaf
    void insert_at_leaf(Node* leaf, string value, string key) {
        if (!values.empty()) {
            for (int i = 0; i < values.size(); i++) {
                if (value == values[i]) {
                    keys[i].push_back(key);
                    break;
                }
                else if (value < values[i]) {
                    values.insert(values.begin() + i, value);
                    keys.insert(keys.begin() + i, vector<string>{key});
                    break;
                }
                else if (i + 1 == values.size()) {
                    values.push_back(value);
                    keys.push_back(vector<string>{key});
                    break;
                }
            }
        }
        else {
            values.push_back(value);
            keys.push_back(vector<string>{key});
        }
    }
};

// B+ tree
class BplusTree {
public:
    Node* root;
    BplusTree(int order) {
        root = new Node(order);
        root->check_leaf = true;
    }
    
    // insert operation
    void insert(string value, string key) {
        Node* old_node = search(value);
        old_node->insert_at_leaf(old_node, value, key);
        if (old_node->values.size() == old_node->order) {
            Node* node1 = new Node(old_node->order);
            node1->check_leaf = true;
            node1->parent = old_node->parent;
            int mid = ceil(old_node->order / 2.0) - 1;
            node1->values.assign(old_node->values.begin() + mid + 1, old_node->values.end());
            node1->keys.assign(old_node->keys.begin() + mid + 1, old_node->keys.end());
            node1->nextKey = old_node->nextKey;
            old_node->values.resize(mid + 1);
            old_node->keys.resize(mid + 1);
            old_node->nextKey = node1;
            insert_in_parent(old_node, node1->values[0], node1);
        }
    }
    
    // search operation for different operations
    Node* search(string value) {
        Node* current_node = root;
        while (!current_node->check_leaf) {
            for (int i = 0; i < current_node->values.size(); i++) {
                if (value == current_node->values[i]) {
                    current_node = current_node->children[i + 1];
                    break;
                }
                else if (value < current_node->values[i]) {
                    current_node = current_node->children[i];
                    break;
                }
                else if (i + 1 == current_node->values.size()) {
                    current_node = current_node->children[i + 1];
                    break;
                }
            }
        }
        return current_node;
    }
    
    // find the node
    bool find(string value, string key) {
        Node* l = search(value);
        for (int i = 0; i < l->values.size(); i++) {
            if (l->values[i] == value) {
                for (int j = 0; j < l->keys[i].size(); j++) {
                    if (l->keys[i][j] == key) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
    
    // inserting at the parent
    void insert_in_parent(Node* n, string value, Node* ndash) {
        if (root == n) {
            Node* rootNode = new Node(n->order);
            rootNode->values.push_back(value);
            rootNode->children.push_back(n);
            rootNode->children.push_back(ndash);
            root = rootNode;
            n->parent = rootNode;
            ndash->parent = rootNode;
            return;
        }
        Node* parentNode = n->parent;
        for (int i = 0; i < parentNode->children.size(); i++) {
            if (parentNode->children[i] == n) {
                parentNode->values.insert(parentNode->values.begin() + i, value);
                parentNode->children.insert(parentNode->children.begin() + i + 1, ndash);
                if (parentNode->children.size() > parentNode->order) {
                    Node* parentdash = new Node(parentNode->order);
                    parentdash->parent = parentNode->parent;
                    int mid = ceil(parentNode->order / 2.0) - 1;
                    parentdash->values.assign(parentNode->values.begin() + mid + 1, parentNode->values.end());
                    parentdash->children.assign(parentNode->children.begin() + mid + 1, parentNode->children.end());
                    string value_ = parentNode->values[mid];
                    parentNode->values.resize(mid);
                    parentNode->children.resize(mid + 1);
                    insert_in_parent(parentNode, value_, parentdash);
                }
                break;
            }
        }
    }
    
    // display the tree
    void printTree(Node* node) {
        if (node == nullptr) return;
        for (int i = 0; i < node->values.size(); i++) {
            cout << node->values[i] << " ";
        }
        cout << endl;
        if (!node->check_leaf) {
            for (int i = 0; i <= node->values.size(); i++) {
                printTree(node->children[i]);
            }
        }
    }
};

int main() {
    int record_len = 3;
    BplusTree bplustree(record_len);
    bplustree.insert("5", "33");
    bplustree.insert("15", "21");
    bplustree.insert("25", "31");
    bplustree.insert("35", "41");
    bplustree.insert("45", "10");
    bplustree.printTree(bplustree.root);
    if (bplustree.find("5", "34")) {
        cout << "Found" << endl;
    } else {
        cout << "Not found" << endl;
    }
    return 0;
}
