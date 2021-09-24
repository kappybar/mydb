#include "db.hpp"

BTree::BTree(const std::string &file_name)
    :buffer_manager(BufferManager(file_name)) 
{
    if (buffer_manager.disk_manager.page_num == 0) {
        buffer_manager.create_new_page();
        root = new Node(&buffer_manager,0);
        root->set_keys_size(0);
        root->set_is_leaf(true);
    } else {
        root = new Node(&buffer_manager,0);
    }
}

BTree::~BTree() {
    if(root != nullptr) delete root;
    buffer_manager.flush();
}

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
        // we don't use pageid's page from now on
    }
    return success_del;
}

void BTree::clear(void) {
    delete root;
    buffer_manager.flush();
    buffer_manager.disk_manager.clear_file();
    buffer_manager.create_new_page();
    root = new Node(&buffer_manager,0);
    root->set_keys_size(0);
    root->set_is_leaf(true);
}

void BTree::flush(void) {
    buffer_manager.flush();
}

std::map<std::string,std::string> BTree::all_data(void) {
    return root->all_data();
}

void BTree::show() {
    root->show();
}