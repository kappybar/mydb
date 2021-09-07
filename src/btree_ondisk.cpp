#include "db.hpp"

std::optional<std::string> BTree::search(const std::string &key) {
    return root->search(key);
}    

bool BTree::update(const std::string &key,const std::string &value) {
    return root->update(key,value);
}

void BTree::insert(const std::string &key,const std::string &value) {
    if (root->isfull()) {
        int temp_pageid = buffer_manager.create_new_page();
        Node temp = Node(&buffer_manager,temp_pageid);
        const char *root_page_buf = buffer_manager.read_page(root->pageid,checksum_len,PAGESIZE - checksum_len);
        buffer_manager.write_page(temp.pageid,root_page_buf,checksum_len,PAGESIZE - checksum_len);
        free(const_cast<char*>(root_page_buf));
        root->set_is_leaf(false);
        root->set_keys_size(0);
        root->set_child_pageid(0,temp.pageid);
        root->splitchild(0);
    }
    root->insert(key,value);
}

bool BTree::del(const std::string &key) {
    bool success_del = root->del(key);
    if (root->keys_size() == 0 && !root->is_leaf()) {
        int pageid = root->child_pageid(0);
        const char *page_buf = buffer_manager.read_page(pageid,checksum_len,PAGESIZE - checksum_len);
        buffer_manager.write_page(root->pageid,page_buf,checksum_len,PAGESIZE - checksum_len);
        free(const_cast<char*>(page_buf));
        // pageidはこれから使われなくなる
    }
    return success_del;
}

std::map<std::string,std::string> BTree::all_data(void) {
    return root->all_data();
}

void BTree::show() {
    root->show();
}