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

struct Table;

//
// util.cpp
//

void make_crc32table();
unsigned int crc32(const std::string &s);
std::string to_hex(unsigned int number);
unsigned int from_hex(const std::string &s);
void file_sync(const std::string &file_name);
void error(const char *s);

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
         log_manager(LogManager(log_file_name)) {}

    void checkpointing(); 
    void recovery();  
     
};