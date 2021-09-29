#include "db.hpp"

Transaction::Transaction(Table *table)
    :table(table),
     write_set({}),
     conditional_write_error(false),
     txnid(-1)
{
    table->scheduler.transactions.push_back(this);
    // table->tasksに同じtxnidのmy_taskがいる。
}

int fresh_txnid(void) {
    static int fresh = 0;
    return fresh++;
}

int Transaction::begin() {
    txnid = fresh_txnid();
    return txnid;
}

bool Transaction::commit() {

    // confirm conditional write
    for(auto [key,data_write] : write_set) {
        auto [first_data_state, _, value] = data_write;
        if ( (table->btree.search(key) == std::nullopt && first_data_state == DataState::in_keys) ||
             (table->btree.search(key) != std::nullopt && first_data_state == DataState::not_in_keys) ) {
            conditional_write_error = true;
        }
        (void)_;
    }

    if (conditional_write_error) {
        rollback();
        return false;
    }

    // write ahead log
    for (auto [key,data_write]:write_set) {
        auto [_, last_ope_kind, value] = data_write;
        if (last_ope_kind == OpeKind::insert) {
            assert(value);
            table->log_manager.log(LogKind::insert,key,value.value());
        } else if (last_ope_kind == OpeKind::update) {
            assert(value);
            table->log_manager.log(LogKind::update,key,value.value());
        } else {
            assert(last_ope_kind == OpeKind::del);
            table->log_manager.log(LogKind::del,key,"");
        }
        (void)_;
    }
    table->log_manager.log(LogKind::commit,"","");
    
    // flush
    table->log_manager.log_flush();

    // update btree index
    // conditional write
    for(auto [key,data_write] : write_set) {
        auto [ _, last_ope_kind, value] = data_write;
        if (last_ope_kind == OpeKind::insert) {
            assert(value);
            if (table->btree.search(key) == std::nullopt) {
                table->btree.insert(key,value.value());
            } else {
                table->btree.update(key,value.value());
            }
        } else if (last_ope_kind == OpeKind::update) {
            assert(value);
            if (table->btree.search(key) == std::nullopt) {
                table->btree.insert(key,value.value());
            } else {
                table->btree.update(key,value.value());
            }
        } else {
            assert(last_ope_kind == OpeKind::del);
            table->btree.del(key);
        }
        (void)_;
    }

    // SS2PL
    unlock();
    return true;
}

bool Transaction::rollback() {
    unlock();
    return false;
}

void Transaction::unlock() {
    for (auto [key, _] : write_set) {
        table->lock_manager.unlock(key,txnid);
        (void)_;
    }
    for (auto [key, _] : read_set) {
        table->lock_manager.unlock(key,txnid);
        (void)_;
    }
    write_set.clear();
    read_set.clear();
}

result Transaction::select(const std::string &key) {
    auto try_lock_result = select_internal(key);
    DataOperation data_operation = DataOperation(OpeKind::select,key,"");
    return std::make_tuple(this,try_lock_result,data_operation);
}

result Transaction::insert(const std::string &key,const std::string &value) {
    auto try_lock_result = insert_internal(key,value);
    DataOperation data_operation = DataOperation(OpeKind::insert,key,value);
    return std::make_tuple(this,try_lock_result,data_operation);
}

result Transaction::update(const std::string &key,const std::string &value) {
    auto try_lock_result = update_internal(key,value);
    DataOperation data_operation = DataOperation(OpeKind::update,key,value);
    return std::make_tuple(this,try_lock_result,data_operation);
}
    
result Transaction::del(const std::string &key) {
    auto try_lock_result = del_internal(key);
    DataOperation data_operation = DataOperation(OpeKind::del,key,"");
    return std::make_tuple(this,try_lock_result,data_operation);
}

std::optional<std::string> Transaction::get_value(const std::string &key) {
    if (read_set.count(key) > 0) {
        return read_set[key];
    } else if (write_set.count(key) > 0) {
        if (write_set[key].last_ope_kind != OpeKind::del) {
            return write_set[key].value;
        } else {
            return std::nullopt;
        }
    }
    assert(false);
}

TryLockResult Transaction::select_internal(const std::string &key) {
    if (read_set.count(key) > 0 || write_set.count(key) > 0) {
        return TryLockResult::GetLock;
    } else {
        TryLockResult res = table->lock_manager.try_shared_lock(key,txnid);
        switch (res) {
            case TryLockResult::GetLock:
                read_set[key] = table->btree.search(key);
                break;
            case TryLockResult::Abort:
                rollback();   
                break; 
            case TryLockResult::Wait:
            default:
                break;
        }
        return res;
    }
}

TryLockResult Transaction::insert_internal(const std::string &key,const std::string &value) {
    if (write_set.count(key) > 0 && write_set[key].last_ope_kind != OpeKind::del) {
        conditional_write_error = true;
        return TryLockResult::Abort;
    } else if (write_set.count(key) > 0) {
        write_set[key] = DataWrite(write_set[key].first_data_state,OpeKind::insert,value);
        return TryLockResult::GetLock;
    } else {
        // read_set.count(key) > 0 ||
        // read_set.count(key) == 0 && write_set.count(key) == 0
        TryLockResult res = table->lock_manager.try_exclusive_lock(key,txnid);
        switch (res) {
            case TryLockResult::GetLock:
                read_set.erase(key);
                write_set[key] = DataWrite(DataState::not_in_keys,OpeKind::insert,value);
                break;
            case TryLockResult::Abort:
                rollback();
                break;
            case TryLockResult::Wait:
            default:
                break;
        }
        return res;
    }
}

TryLockResult Transaction::update_internal(const std::string &key,const std::string &value) {
    if (write_set.count(key) > 0 && write_set[key].last_ope_kind == OpeKind::del) {
        conditional_write_error = true;
        return TryLockResult::Abort;
    } else if (write_set.count(key) > 0) {
        write_set[key] = DataWrite(write_set[key].first_data_state,OpeKind::update,value);
        return TryLockResult::GetLock;
    } else {
        // read_set.count(key) > 0 ||
        // read_set.count(key) == 0 && write_set.count(key) == 0
        TryLockResult res = table->lock_manager.try_exclusive_lock(key,txnid);
        switch (res) {
            case TryLockResult::GetLock:
                read_set.erase(key);
                write_set[key] = DataWrite(DataState::in_keys,OpeKind::update,value);
                break;
            case TryLockResult::Abort:
                rollback();
                break;
            case TryLockResult::Wait:
            default:
                break;
        }
        return res;
    }
}

TryLockResult Transaction::del_internal(const std::string &key) {
    if (write_set.count(key) > 0 && write_set[key].last_ope_kind == OpeKind::del) {
        conditional_write_error = true;
        return TryLockResult::Abort;
    } else if (write_set.count(key) > 0) {
        write_set[key] = DataWrite(write_set[key].first_data_state,OpeKind::del,std::nullopt);
        return TryLockResult::GetLock;
    } else {
        // read_set.count(key) > 0 ||
        // read_set.count(key) == 0 && write_set.count(key) == 0
        TryLockResult res = table->lock_manager.try_exclusive_lock(key,txnid);
        switch (res) {
            case TryLockResult::GetLock:
                read_set.erase(key);
                write_set[key] = DataWrite(DataState::in_keys,OpeKind::del,std::nullopt);
                break;
            case TryLockResult::Abort:
                rollback();
                break;
            case TryLockResult::Wait:
            default:
                break;
        }
        return res;
    }
}


