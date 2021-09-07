#pragma once

#include <string>
#include <map>
#include <optional>
#include <vector>
#include <fstream>
#include <memory>
#include <sstream>
#include <iomanip>
#include <vector>
#include <iostream>
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

struct Table;

//
// util.cpp
//

void make_crc32table();
unsigned int crc32(const std::string &s);
unsigned int crc32(const char *s,int len);
std::string to_hex(unsigned int number);
unsigned int from_hex(const std::string &s);
void file_sync(const std::string &file_name);
unsigned int file_size(const std::string &file_name);
void error(const char *s);


//
// page.cpp
//

const int PAGESIZE = 4096;

struct Page {
    int pageid;
    bool dirty;
    int pin_count;
    int access;
    char page[PAGESIZE];

    Page()
        :pageid(-1),
         dirty(false),
         pin_count(0),
         access(0) {}
         
    Page(int pageid,const char page_[])
        :pageid(pageid),
         dirty(false),
         pin_count(0),
         access(0)
        {
            std::copy(page_,page_+PAGESIZE,page);
        }

    const char *read(int offset,int len);
    void write(const char buf[],int offset,int len);
    void update_checksum(void);
    bool confirm_checksum(void);
    void pin(void);
    void unpin(void);
};

//
// diskmanager.cpp
//

struct DiskManager {
    std::string file_name;
    std::fstream file_stream;
    int page_num;

    DiskManager(const std::string &file_name)
        :file_name(file_name)
        {
            std::ofstream output_file_stream;
            output_file_stream.open(file_name,std::ios::app);
            if(!output_file_stream) {
                error("open(disk_manager)");
            }
            output_file_stream.close();
            file_stream.open(file_name);
            if (!file_stream) {
                error("open(disk_manager)");
            }
            page_num = file_size(file_name) / PAGESIZE;
        }
    ~DiskManager() {
        file_stream.close();
    }

    Page fetch_page(int pageid);
    void write_page(int pageid,Page &page);
    void flush(void);
    int allocate_new_page(void);
};

//
// buffermanager.cpp
//

extern const int MAX_BUFFER_SIZE;

struct BufferManager {
    DiskManager disk_manager;
    std::vector<Page> pages;
    std::map<int,int> pagetable;
    int victim_index_base;

    BufferManager(const std::string &file_name)
        :disk_manager(DiskManager(file_name)),
         pages(),
         pagetable(),
         victim_index_base(0) {
            pages.resize(MAX_BUFFER_SIZE);
    }
    ~BufferManager() {
        flush();
    }

    void fetch_page(int pageid);
    int  create_new_page(void);
    const char *read_page(int pageid,int offset,int len);
    void write_page(int pageid,const char buf[],int offset,int len);
    void evict_page(int pageid);
    void flush(void);
    int evict(void);  
};

//
// node.cpp
//

extern const int checksum_len;

struct Node {
    BufferManager *buffer_manager;
    int pageid;
    // key.size() + 1 == children.size()
    //   keys[0]  keys[1]  keys[2]   keys[3]
    // c[0]    c[1]     c[2]     c[3]      c[4]

    Node(BufferManager *buffer_manager,int pageid)
        :buffer_manager(buffer_manager),
         pageid(pageid) {}

    bool is_leaf(void);
    int keys_size(void);
    int child_pageid(int index);
    std::string keys(int index);
    std::string values(int index);

    void set_is_leaf(bool is_leaf);
    void set_keys_size(int keys_size);
    void set_child_pageid(int index,int child_pageid);
    void set_keys(int index,const std::string &key);
    void set_values(int index,const std::string &value);

    std::optional<std::string> search(const std::string &key);
    bool update(const std::string &key,const std::string &value);
    void insert(const std::string &key,const std::string &value);
    bool del(const std::string &key);

    void splitchild(int idx);
    void leftshift(int index);
    void rightshift(int index);
    void merge(int index);
    std::pair<std::string,std::string> delete_min_data(void);
    std::pair<std::string,std::string> min_data(void);
    std::pair<std::string,std::string> delete_max_data(void);
    std::pair<std::string,std::string> max_data(void);
    bool isfull();

    std::map<std::string,std::string> all_data(void);
    void show();
};

//
// btree_ondisk.cpp
//

struct BTree {
    BufferManager buffer_manager;
    Node *root;  
    BTree(const std::string &file_name)
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
    ~BTree() {
        if(root != nullptr) delete root;
        buffer_manager.flush();
    }
    std::optional<std::string> search(const std::string &key);
    bool update(const std::string &key,const std::string &value);
    void insert(const std::string &key,const std::string &value);
    bool del(const std::string &key);

    std::map<std::string,std::string> all_data(void);
    void show();
};

//
// log.cpp
//

enum struct LogKind {
    insert,
    update,
    del,
    commit,
};

struct LogManager {
    std::string log_file_name;
    std::ofstream log_file_output;

    LogManager(std::string log_file_name) : log_file_name(log_file_name) {
        log_file_output.open(log_file_name,std::ios::app);
        if (!log_file_output) {
            error("open(log_file)");
        }
    }
    ~LogManager() {
        log_file_output.close();
    }
    void log(LogKind log_kind,const std::string &key,const std::string &value);
    void log_flush();
    void erase_log();
};

std::string LogKind2str(LogKind log_kind);

//
// transaction.cpp
//

enum struct DataOpe {
    insert,
    update,
    del,
};

enum struct DataState {
    in_keys,
    not_in_keys,
};

struct DataWrite {
    DataState first_data_state;
    DataOpe last_data_ope;
    std::optional<std::string> value;
};

struct Transaction {
    Table *table; 
    std::map<std::string,DataWrite> write_set;
    bool conditional_write_error;

    Transaction(Table *table)
        :table(table),
         write_set({}),
         conditional_write_error(false) {}

    void begin();
    bool commit();
    void rollback();
    std::optional<std::string> select(const std::string &key);
    void insert(const std::string &key,const std::string &value);
    void update(const std::string &key,const std::string &value);
    void del(const std::string &key);
};

// 
// table.cpp
//

struct Table {
    std::map<std::string,std::string> index;
    std::string data_file_name;
    LogManager log_manager;

    Table(std::string data_file_name,std::string log_file_name)
        :index({}),
         data_file_name(data_file_name),
         log_manager(LogManager(log_file_name)) 
    {
        std::ofstream data_file;
        data_file.open(data_file_name,std::ios::app);
        if (!data_file) {
            error("open(data_file)");
        }
        data_file.close();
    }

    void checkpointing(); 
    void recovery();  
     
};