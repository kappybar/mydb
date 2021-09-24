#include "db.hpp"

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
        auto [_, last_data_ope, value] = data_write;
        if (last_data_ope == DataOpe::insert) {
            assert(value);
            table->log_manager.log(LogKind::insert,key,value.value());
        } else if (last_data_ope == DataOpe::update) {
            assert(value);
            table->log_manager.log(LogKind::update,key,value.value());
        } else {
            assert(last_data_ope == DataOpe::del);
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
        auto [ _, last_data_ope, value] = data_write;
        if (last_data_ope == DataOpe::insert) {
            assert(value);
            if (table->btree.search(key) == std::nullopt) {
                table->btree.insert(key,value.value());
            } else {
                table->btree.update(key,value.value());
            }
        } else if (last_data_ope == DataOpe::update) {
            assert(value);
            if (table->btree.search(key) == std::nullopt) {
                table->btree.insert(key,value.value());
            } else {
                table->btree.update(key,value.value());
            }
        } else {
            assert(last_data_ope == DataOpe::del);
            table->btree.del(key);
        }
        (void)_;
    }

    // SS2PL
    unlock();
    return true;
}

void Transaction::rollback() {
    unlock();
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
    select_internal(key);
    return make_pair(this,key);
}

result Transaction::insert(const std::string &key,const std::string &value) {
    insert_internal(key,value);
    return make_pair(this,std::nullopt);
}

result Transaction::update(const std::string &key,const std::string &value) {
    update_internal(key,value);
    return make_pair(this,std::nullopt);
}
    
result Transaction::del(const std::string &key) {
    del_internal(key);
    return make_pair(this,std::nullopt);
}

std::optional<std::string> Transaction::get_value(const std::string &key) {
    if (read_set.count(key) > 0) {
        return read_set[key];
    } else if (write_set.count(key) > 0) {
        if (write_set[key].last_data_ope != DataOpe::del) {
            return write_set[key].value;
        } else {
            return std::nullopt;
        }
    }
    assert(false);
}



void Transaction::select_internal(const std::string &key) {
    if (read_set.count(key) > 0 || write_set.count(key) > 0) {
        return;
    } else {
        TryLockResult res = table->lock_manager.try_shared_lock(key,txnid);
        switch (res) {
            case TryLockResult::GetLock:
                txn_state = TransactionState::Execute;
                read_set[key] = table->btree.search(key); 
                return;
            case TryLockResult::Wait:
                txn_state = TransactionState::Wait;
                waiting_dataope = DataOpe::select;
                waiting_key = key;
                return;
            case TryLockResult::Abort:
                txn_state = TransactionState::Abort;
                return;
            default:
                assert(false);
        }
    }
}

void Transaction::insert_internal(const std::string &key,const std::string &value) {
    if (write_set.count(key) > 0 && write_set[key].last_data_ope != DataOpe::del) {
        conditional_write_error = true;
    } else if (write_set.count(key) > 0) {
        write_set[key] = DataWrite(write_set[key].first_data_state,DataOpe::insert,value);
    } else {
        // read_set.count(key) > 0 ||
        // read_set.count(key) == 0 && write_set.count(key) == 0
        TryLockResult res = table->lock_manager.try_exclusive_lock(key,txnid);
        switch (res) {
            case TryLockResult::GetLock:
                txn_state = TransactionState::Execute;
                read_set.erase(key);
                write_set[key] = DataWrite(DataState::not_in_keys,DataOpe::insert,value);
                break;
            case TryLockResult::Wait:
                txn_state = TransactionState::Wait;
                waiting_dataope = DataOpe::insert;
                waiting_key = key;
                waiting_value = value;
                break;
            case TryLockResult::Abort:
                txn_state = TransactionState::Abort;
                break;
            default:
                assert(false);
        }
    }
}

void Transaction::update_internal(const std::string &key,const std::string &value) {
    if (write_set.count(key) > 0 && write_set[key].last_data_ope == DataOpe::del) {
        conditional_write_error = true;
    } else if (write_set.count(key) > 0) {
        write_set[key] = DataWrite(write_set[key].first_data_state,DataOpe::update,value);
    } else {
        // read_set.count(key) > 0 ||
        // read_set.count(key) == 0 && write_set.count(key) == 0
        TryLockResult res = table->lock_manager.try_exclusive_lock(key,txnid);
        switch (res) {
            case TryLockResult::GetLock:
                txn_state = TransactionState::Execute;
                read_set.erase(key);
                write_set[key] = DataWrite(DataState::in_keys,DataOpe::update,value);
                break;
            case TryLockResult::Wait:
                txn_state = TransactionState::Wait;
                waiting_dataope = DataOpe::update;
                waiting_key = key;
                waiting_value = value;
                break;
            case TryLockResult::Abort:
                txn_state = TransactionState::Abort;
                break;
            default:
                assert(false);
        }
    }
}

void Transaction::del_internal(const std::string &key) {
    if (write_set.count(key) > 0 && write_set[key].last_data_ope == DataOpe::del) {
        conditional_write_error = true;
    } else if (write_set.count(key) > 0) {
        write_set[key] = DataWrite(write_set[key].first_data_state,DataOpe::del,std::nullopt);
    } else {
        // read_set.count(key) > 0 ||
        // read_set.count(key) == 0 && write_set.count(key) == 0
        TryLockResult res = table->lock_manager.try_exclusive_lock(key,txnid);
        switch (res) {
            case TryLockResult::GetLock:
                txn_state = TransactionState::Execute;
                read_set.erase(key);
                write_set[key] = DataWrite(DataState::in_keys,DataOpe::del,std::nullopt);
                break;
            case TryLockResult::Wait:
                txn_state = TransactionState::Wait;
                waiting_dataope = DataOpe::del;
                waiting_key = key;
                break;
            case TryLockResult::Abort:
                txn_state = TransactionState::Abort;
                break;
            default:
                assert(false);
        }
    }
}

void Transaction::exec_waiting_dataope(void) {
    assert(txn_state == TransactionState::Wait);

    switch (waiting_dataope) {
        case DataOpe::select:
            select_internal(waiting_key);
            break;
        case DataOpe::insert:
            insert_internal(waiting_key,waiting_value);
            break;
        case DataOpe::update:
            update_internal(waiting_key,waiting_value);
            break;
        case DataOpe::del:
            del_internal(waiting_key);
            break;
        default:
            assert(false);
    }

}
