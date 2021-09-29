#pragma once

#include <string>
#include <map>
#include <optional>
#include <vector>
#include <fstream>
#include <deque>
#include <set>
#include <memory>
#include <sstream>
#include <iomanip>
#include <iostream>
#include <utility>
#include <coroutine>
#include <stdio.h>
#include <errno.h>
#include <assert.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <string.h>

struct my_task;
struct Transaction;
enum struct TryLockResult;
struct DataOperation;
using result = std::tuple<Transaction*,TryLockResult,DataOperation>;

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
// lock_manager.cpp
//

enum struct LockKind {
    Share,
    Exclusive,
};

struct Lock {
    LockKind lock_kind;

    // used when lock_kind = Exclusive
    int txnid;

    // used when lock_kind = Share
    int txnnum;
    std::set<int> readers;

    Lock();
    Lock(LockKind lock_kind,int txnid);
    Lock(LockKind lock_kind,int txnnum,const std::set<int>& readers);

    bool has_shared_lock(void);
    bool has_exclusive_lock(void);
    void add_reader(int txnid);
    void delete_reader(int txnid);
    bool has_priority(int txnid_); // if Lock.txnid has higher priority than txnid  return true 
};

Lock Lock_shared(int txnid);
Lock Lock_exclusive(int txnid);

enum struct TryLockResult {
    GetLock,
    Wait,
    Abort,
};

struct LockManager {
    std::map<std::string,Lock> lock_table;

    TryLockResult try_shared_lock(const std::string& s,int txnid);
    TryLockResult try_exclusive_lock(const std::string& s,int txnid);
    TryLockResult try_upgrade_lock(const std::string& s,int txnid);
    void unlock(const std::string& s,int txnid);
};

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

    Page();
    Page(int pageid,const char page_[]);

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

    DiskManager(const std::string &file_name);
    ~DiskManager();

    Page fetch_page(int pageid);
    void write_page(int pageid,Page &page);
    void flush(void);
    int allocate_new_page(void);
    void clear_file(void);
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

    BufferManager(const std::string &file_name);
    ~BufferManager();

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

    Node(BufferManager *buffer_manager,int pageid);

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

    BTree(const std::string &file_name);
    ~BTree();

    std::optional<std::string> search(const std::string &key);
    bool update(const std::string &key,const std::string &value);
    void insert(const std::string &key,const std::string &value);
    bool del(const std::string &key);
    void clear(void);
    void flush(void);

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

    LogManager(std::string log_file_name);
    ~LogManager();

    void log(LogKind log_kind,const std::string &key,const std::string &value);
    void log_flush();
    void erase_log();
};

std::string LogKind2str(LogKind log_kind);

//
// scheduler.cpp
//

enum struct OpeKind {
    select,
    insert,
    update,
    del,
};

struct DataOperation {
    OpeKind ope_kind;
    std::string key;
    std::string value;

    DataOperation(OpeKind ope_kind,const std::string &key,const std::string &value)
        :ope_kind(ope_kind),
         key(key),
         value(value) {}
};

enum State {
    Execute,
    Wait,
    Done,
};

struct Scheduler {
    std::vector<my_task> tasks;
    std::vector<Transaction*> transactions;
    std::vector<State> states;

    void add_task(my_task &&task);
    std::vector<bool> start(void); // return true if txn commit 
};

// 
// table.cpp
//

struct Table {
    BTree btree;
    std::string data_file_name;
    LogManager log_manager;
    LockManager lock_manager;
    Scheduler scheduler;

    Table(const std::string &btree_file_name,const std::string &data_file_name,const std::string &log_file_name);

    void checkpointing(); 
    void recovery();  
    void add_transaction(my_task&& task);
    std::vector<bool> exec_transaction(void);
};


//
// transaction.cpp
//

enum struct DataState {
    in_keys,
    not_in_keys,
};

struct DataWrite {
    DataState first_data_state;
    OpeKind last_ope_kind;
    std::optional<std::string> value;

    DataWrite()
        :first_data_state(DataState::in_keys),
         last_ope_kind(OpeKind::select),
         value(std::nullopt) {}
    DataWrite(DataState first_data_state,OpeKind last_ope_kind,const std::optional<std::string> &value)
        :first_data_state(first_data_state),
         last_ope_kind(last_ope_kind),
         value(value) {}
};

struct Transaction {
    Table *table; 
    std::map<std::string,DataWrite> write_set;
    std::map<std::string,std::optional<std::string>> read_set;
    bool conditional_write_error;
    int txnid;

    Transaction(Table *table);

    int begin();
    bool commit();
    bool rollback();
    result select(const std::string &key);
    result insert(const std::string &key,const std::string &value);
    result update(const std::string &key,const std::string &value);
    result del(const std::string &key);

    std::optional<std::string> get_value(const std::string &key);
    TryLockResult select_internal(const std::string &key);
    TryLockResult insert_internal(const std::string &key,const std::string &value);
    TryLockResult update_internal(const std::string &key,const std::string &value);
    TryLockResult del_internal(const std::string &key);
    void unlock(void);
};

// concurrent

struct my_task {
    struct promise_type {
        int txnid_;
        bool waiting_;
        bool abort_;
        bool commit_;
        DataOperation data_operation_;

        promise_type()
            :txnid_(-1),
             waiting_(false),
             abort_(false),
             commit_(false),
             data_operation_(DataOperation(OpeKind::select,"","")) {}

        static auto get_return_object_on_allocation_failure() { 
            return my_task{nullptr}; 
        }

        auto get_return_object() { 
            return my_task{handle::from_promise(*this)}; 
        }

        auto initial_suspend() { 
            return std::suspend_always{}; 
        }

        auto final_suspend() noexcept { 
            return std::suspend_always{}; 
        }

        void unhandled_exception() { 
            std::terminate(); 
        }

        void return_void() {}

        auto yield_value(int txnid) {
            txnid_ = txnid;
            return std::suspend_always{};
        }

        auto yield_value(bool commit) {
            commit_ = commit;
            abort_ = !commit;
            return std::suspend_always{};
        }
        // co_yield e = co_await p.yield_value(e);

        struct awaiter {
            Transaction *txn;
            DataOperation data_operation;

            awaiter(Transaction *txn,const DataOperation &data_operation)
                :txn(txn), 
                 data_operation(data_operation) {}

            bool await_ready() const { 
                // true  -> continue execution
                // false -> suspend  execution
                return false;
            }

            std::optional<std::string> await_resume() {
                // resumeした直後にする処理
                // return select result;
                if (data_operation.ope_kind == OpeKind::select) {
                    // select
                    return txn->get_value(data_operation.key);
                } else {
                    // updata insert delete
                    return std::nullopt;
                }
            }

            void await_suspend(std::coroutine_handle<> h) {
                // suspendした直後にする処理。次のawait_resumeの準備をする.
            }
            // * image *
            // co_await e = {
            //     awaiter aw = await_transform(e);
            //     if (aw.await_ready()) {
            //         I continue co_routine
            //     } else {
            //         I suspend co_routine
            //         aw.await_suspend(coro);
            //         ...
            //         when I return co_routine
            //         aw.await_resume();
            //     }
            // }
        };

        awaiter await_transform(const result &result) {
            waiting_ = (get<1>(result)) == TryLockResult::Wait;
            abort_ = (get<1>(result)) == TryLockResult::Abort;
            data_operation_ = get<2>(result);
            return awaiter(get<0>(result),get<2>(result));
        }
    };
    using handle = std::coroutine_handle<promise_type>;
    bool move_next() { 
        return can_move() ? (coro.resume(), !coro.done()) : false; 
    }

    bool can_move() { 
        return coro && !coro.done(); 
    }

    int txnid(void) { 
        return coro.promise().txnid_; 
    }

    bool waiting(void) {
        return coro.promise().waiting_;
    }

    bool abort(void) {
        return coro.promise().abort_;
    }

    bool commit(void) {
        return coro.promise().commit_;
    }

    DataOperation data_operation(void) {
        return coro.promise().data_operation_;
    }

    void destroy_handle(void) { 
        if (coro) coro.destroy(); 
    }

    my_task(my_task const&) = delete;
    my_task(my_task && rhs) : coro(rhs.coro) { rhs.coro = nullptr; }
    ~my_task() { destroy_handle(); }
private:
    my_task(handle h) : coro(h) {}
    handle coro;
};