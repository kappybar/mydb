#include "db.hpp"

// 4KiB
// checksum is_leaf keys_size pageid(child0) key0 value0 pageid(child1) key1 value1 
// | 8     | | 1  | | 8      || 8          ||400 ||400 ||  8         | ...
// key   = key_size + key
// value = value_size + value
// key_size <= 392
// value_size <= 392

const int checksum_len  = 8;
const int is_leaf_len   = 1;
const int keys_size_len = 8;
const int pageid_len    = 8;
const int key_len       = 400;
const int keysize_len   = 8;
const int value_len     = 400;
const int valuesize_len = 8;

const int order     = 6;  // order means the biggest chidlren size (odd number)
const int halforder = (order - 1) / 2;

bool Node::is_leaf(void) {
    const char *buf = buffer_manager->read_page(pageid,checksum_len,is_leaf_len);
    bool is_leaf = (strcmp(buf,"1") == 0);
    free(const_cast<char*>(buf));
    return is_leaf;
}

int Node::keys_size(void) {
    const char *buf = buffer_manager->read_page(pageid,checksum_len + is_leaf_len,keys_size_len);
    int keys_size = strtol(buf,NULL,16);
    free(const_cast<char*>(buf));
    return keys_size;
}

int Node::child_pageid(int index) {
    const char *buf = buffer_manager->read_page(pageid, checksum_len + is_leaf_len + keys_size_len
                                                       +index * (pageid_len + key_len + value_len),pageid_len);
    int child_pageid = strtol(buf,NULL,16);
    free(const_cast<char*>(buf));
    return child_pageid;
}

std::string Node::keys(int index) {
    const char *buf = buffer_manager->read_page(pageid, checksum_len + is_leaf_len + keys_size_len + pageid_len
                                                       +index * (pageid_len + key_len + value_len),key_len);
    const char *keysize_buf = strndup(buf,keysize_len);
    int keysize = strtol(keysize_buf,NULL,16);
    std::string key(buf,keysize_len,keysize);
    free(const_cast<char*>(buf));
    free(const_cast<char*>(keysize_buf));
    return key;
}

std::string Node::values(int index) {
    const char *buf = buffer_manager->read_page(pageid, checksum_len + is_leaf_len + keys_size_len + pageid_len + key_len
                                                       +index * (pageid_len + key_len + value_len),value_len);
    const char *valuesize_buf = strndup(buf,valuesize_len);
    int valuesize = strtol(valuesize_buf,NULL,16);
    std::string value(buf,valuesize_len,valuesize);
    free(const_cast<char*>(buf));
    free(const_cast<char*>(valuesize_buf));
    return value;
}

void Node::set_is_leaf(bool is_leaf) {
    char buf[1] = {is_leaf ? '1' : '0'};
    buffer_manager->write_page(pageid,buf,checksum_len,is_leaf_len);
}

void Node::set_keys_size(int keys_size) {
    std::string keys_size_str = to_hex(keys_size);
    buffer_manager->write_page(pageid,keys_size_str.c_str(),checksum_len + is_leaf_len, keys_size_len);
}

void Node::set_child_pageid(int index,int child_pageid) {
    std::string child_pageid_str = to_hex(child_pageid);
    buffer_manager->write_page(pageid,child_pageid_str.c_str(), checksum_len + is_leaf_len + keys_size_len
                                                               +index*(pageid_len + key_len + value_len),pageid_len);
}

void Node::set_keys(int index,const std::string &key) {
    unsigned int key_size = key.size();
    std::string key_size_str = to_hex(key_size);
    std::string key_buf = key_size_str + key;
    assert((int)key_buf.size() <= key_len);
    buffer_manager->write_page(pageid,key_buf.c_str(), checksum_len + is_leaf_len + keys_size_len + pageid_len
                                                      +index*(pageid_len + key_len + value_len),key_buf.size());
}

void Node::set_values(int index,const std::string &value) {
    unsigned int value_size = value.size();
    std::string value_size_str = to_hex(value_size);
    std::string value_buf = value_size_str + value;
    assert((int)value_buf.size() <= value_len);
    buffer_manager->write_page(pageid,value_buf.c_str(), checksum_len + is_leaf_len + keys_size_len + pageid_len + key_len
                                                        +index*(pageid_len + key_len + value_len),value_buf.size());
}

std::optional<std::string> Node::search(const std::string &key) {
    for (int i = 0;i < keys_size(); i++) {
        if (key == keys(i)) {
            return values(i);
        } else if (key < keys(i)) {
            if (is_leaf()) {
                break;
            }
            return Node(buffer_manager,child_pageid(i)).search(key);
        }
    }
    if (is_leaf()) {
        return std::nullopt;
    }
    int index = keys_size();
    return Node(buffer_manager,child_pageid(index)).search(key);
}

bool Node::update(const std::string &key,const std::string &value) {
    for (int i = 0;i < keys_size(); i++) {
        if (key == keys(i)) {
            set_values(i,value);
            return true;
        } else if (key < keys(i)) {
            if (is_leaf()) {
                break;
            }
            return Node(buffer_manager,child_pageid(i)).update(key,value);
        }
    }
    if (is_leaf()) {
        return false;
    }
    int index = keys_size();
    return Node(buffer_manager,child_pageid(index)).update(key,value);
}

void Node::insert(const std::string &key,const std::string &value) {
    assert(!isfull());
    int idx = keys_size();
    for(int i = 0;i < keys_size(); i++) {
        if (key <= keys(i)) {
            assert(key != keys(i));
            idx = i;
            break;
        }
    }
    if (is_leaf()) {
        for(int i = keys_size() - 1;i >= idx; i--) {
            set_keys(i+1,keys(i));
            set_values(i+1,values(i));
        }
        set_keys(idx,key);
        set_values(idx,value);
        set_keys_size(keys_size() + 1);
    } else {
        if (Node(buffer_manager,child_pageid(idx)).isfull()) {
            splitchild(idx);
            if (keys(idx) < key) {
                idx++;
            }
        }
        Node(buffer_manager,child_pageid(idx)).insert(key,value);
    }
}

bool Node::del(const std::string &key) {
    if (is_leaf()) {
        int index = -1;
        int node_keys_size = keys_size();
        for(int i = 0;i < node_keys_size; i++) {
            if (key == keys(i)) {
                index = i;
                break;
            }
        }
        if (index == -1) {
            return false;
        } else {
            for(int i = index + 1;i < node_keys_size; i++) {
                set_keys(i-1,keys(i));
                set_values(i-1,values(i));
            }
            set_keys_size(node_keys_size - 1);
            return true;
        }
    } else {
        int index = keys_size();
        for(int i = 0;i < keys_size(); i++) {
            if (key == keys(i)) {
                Node child0(buffer_manager,child_pageid(i));
                Node child1(buffer_manager,child_pageid(i+1));
                if (child0.keys_size() + child1.keys_size() + 1 < order) {
                    merge(i);
                    return child0.del(key);
                } else if (child0.keys_size() > halforder) {
                    auto data = child0.delete_max_data();
                    set_keys(i,data.first);
                    set_values(i,data.second);
                    return true;
                } else {
                    auto data = child1.delete_min_data();
                    set_keys(i,data.first);
                    set_values(i,data.second);
                    return true;
                }
            } else if(key < keys(i)) {
                index = i;
                break;
            }
        }

        Node child(buffer_manager,child_pageid(index));
        if (child.keys_size() <= halforder) {
            if (index - 1 >= 0 && Node(buffer_manager,child_pageid(index-1)).keys_size() > halforder) {
                rightshift(index - 1);
            } else if (index + 1 <= keys_size() && Node(buffer_manager,child_pageid(index+1)).keys_size() > halforder) {
                leftshift(index);
            } else {
                if (index < keys_size()) {
                    merge(index);
                } else {
                    --index;
                    merge(index);
                }
            }
        }
        return Node(buffer_manager,child_pageid(index)).del(key);
    }
}

void Node::splitchild(int idx) {
    assert(!isfull());
    assert(Node(buffer_manager,child_pageid(idx)).isfull());
    Node child = Node(buffer_manager,child_pageid(idx));
    std::string key   = child.keys(halforder);
    std::string value = child.values(halforder); 

    for(int i = keys_size() - 1;i >= idx; i--) {
        set_keys(i+1,keys(i));
        set_values(i+1,values(i));
    }
    set_keys(idx,key);
    set_values(idx,value);

    for(int i = keys_size();i >= idx; i--) {
        set_child_pageid(i+1,child_pageid(i));
    }
    set_keys_size(keys_size() + 1);

    int new_pageid = buffer_manager->create_new_page();
    Node node = Node(buffer_manager,new_pageid);
    node.set_is_leaf(Node(buffer_manager,child_pageid(idx)).is_leaf());
    for(int i = 0;i < halforder; i++) {
        node.set_keys(i,child.keys(i + halforder + 1));
        node.set_values(i,child.values(i + halforder + 1));
    }
    for(int i = 0;i < halforder + 1; i++) {
        node.set_child_pageid(i,child.child_pageid(i + halforder + 1));
    }
    node.set_keys_size(halforder);

    set_child_pageid(idx+1,node.pageid);
    child.set_keys_size(halforder);
}

void Node::leftshift(int index) {
    assert(index < keys_size());
    Node child0(buffer_manager,child_pageid(index));
    Node child1(buffer_manager,child_pageid(index+1));

    int child0_keys_size = child0.keys_size();
    child0.set_keys(child0_keys_size,keys(index));
    child0.set_values(child0_keys_size,values(index));
    child0.set_child_pageid(child0_keys_size+1,child1.child_pageid(0));
    child0.set_keys_size(child0_keys_size + 1);

    set_keys(index,child1.keys(0));
    set_values(index,child1.values(0));

    int child1_keys_size = child1.keys_size();
    for(int i = 0;i < child1_keys_size - 1; i++) {
        child1.set_keys(i,child1.keys(i+1));
        child1.set_values(i,child1.values(i+1));
    }
    for(int i = 0;i < child1_keys_size; i++) {
        child1.set_child_pageid(i,child1.child_pageid(i+1));
    }
    child1.set_keys_size(child1_keys_size - 1);
}

void Node::rightshift(int index) {
    assert(index < keys_size());
    Node child0(buffer_manager,child_pageid(index));
    Node child1(buffer_manager,child_pageid(index+1));
    
    int child0_keys_size = child0.keys_size();
    int child1_keys_size = child1.keys_size();
    for(int i = child1_keys_size - 1;i >= 0; i--) {
        child1.set_keys(i+1,child1.keys(i));
        child1.set_values(i+1,child1.values(i));
    }
    for(int i = child1_keys_size;i >= 0; i--) {
        child1.set_child_pageid(i+1,child1.child_pageid(i));
    }
    child1.set_keys(0,keys(index));
    child1.set_values(0,values(index));
    child1.set_child_pageid(0,child0.child_pageid(child0_keys_size));
    child1.set_keys_size(child1_keys_size + 1);

    set_keys(index,child0.keys(child0_keys_size - 1));
    set_values(index,child0.values(child0_keys_size - 1));

    child0.set_keys_size(child0_keys_size - 1);
}

void Node::merge(int index) {
    assert(index < keys_size());
    Node child0(buffer_manager,child_pageid(index));
    Node child1(buffer_manager,child_pageid(index+1));

    int child0_keys_size = child0.keys_size();
    int child1_keys_size = child1.keys_size();
    assert(child0_keys_size + child1_keys_size + 1 < order);
    child0.set_keys(child0_keys_size,keys(index));
    child0.set_values(child0_keys_size,values(index));

    for(int i = 0;i < child1_keys_size; i++) {
        child0.set_keys(child0_keys_size + i + 1,child1.keys(i));
        child0.set_values(child0_keys_size + i + 1,child1.values(i));
    }
    for(int i = 0;i <= child1_keys_size; i++) {
        child0.set_child_pageid(child0_keys_size + i + 1,child1.child_pageid(i));
    }
    child0.set_keys_size(child0_keys_size + child1_keys_size + 1);

    int node_keys_size = keys_size();
    for(int i = index + 1;i < node_keys_size; i++) {
        set_keys(i-1,keys(i));
        set_values(i-1,values(i));
    }
    for(int i = index + 2;i <= node_keys_size; i++) {
        set_child_pageid(i-1,child_pageid(i));
    }
    set_keys_size(node_keys_size - 1);

    // we don't use child1' page from now on
}

std::pair<std::string,std::string> Node::delete_max_data(void) {
    auto data = max_data();
    del(data.first);
    return data;
}

std::pair<std::string,std::string> Node::max_data(void) {
    int node_keys_size = keys_size();
    if (is_leaf()) {
        return make_pair(keys(node_keys_size-1),values(node_keys_size-1));
    } else {
        return Node(buffer_manager,child_pageid(node_keys_size)).max_data();
    }
}

std::pair<std::string,std::string> Node::delete_min_data(void) {
    auto data = min_data();
    del(data.first);
    return data;
}

std::pair<std::string,std::string> Node::min_data(void) {
    if (is_leaf()) {
        return make_pair(keys(0),values(0));
    } else {
        return Node(buffer_manager,child_pageid(0)).min_data();
    }
}

bool Node::isfull() {
    return keys_size() == order - 1;
}

std::map<std::string,std::string> Node::all_data(void) {
    std::map<std::string,std::string> all_datas;
    if (is_leaf()) {
        for (int i = 0;i < keys_size(); i++) {
            all_datas[keys(i)] = values(i);
        }
        return all_datas;
    } else {
        for (int i = 0;i < keys_size(); i++) {
            all_datas[keys(i)] = values(i);
        }
        for (int i = 0;i <= keys_size(); i++) {
            auto child_datas = Node(buffer_manager,child_pageid(i)).all_data();
            all_datas.merge(child_datas);
        }
        return all_datas;
    }
}


void Node::show() {
    if (is_leaf()) {
        std::cerr << "child" << std::endl;
        for(int i = 0;i < keys_size(); i++) {
            std::cerr << keys(i) << " " << values(i) << "  ";
        }
        std::cerr << std::endl;
    } else {
        std::cerr << "Node" << std::endl;
        for(int i = 0;i < keys_size(); i++) {
            std::cerr << keys(i) << " " << values(i) << "  ";
        }
        std::cerr << std::endl;
        for(int i = 0;i < keys_size() + 1; i++) {
            Node(buffer_manager,child_pageid(i)).show();
        }
        std::cerr << std::endl;
    }
}