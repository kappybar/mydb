#include "db.hpp"

void Transaction::begin() {
    ;
}

bool Transaction::commit() {

    // conditional writeの確認
    for(auto [key,data_write] : write_set) {
        auto [first_data_state, _, value] = data_write;
        if ( (table->index.count(key) == 0 && first_data_state == DataState::in_keys) ||
             (table->index.count(key) >  0 && first_data_state == DataState::not_in_keys) ) {
            conditional_write_error = true;
        }
        (void)_;
    }

    if (conditional_write_error) {
        write_set.clear();
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

    // in memory indexを更新
    // conditional write
    // b-treeにしたときはここを変える？
    for(auto [key,data_write] : write_set) {
        auto [ _, last_data_ope, value] = data_write;
        if (last_data_ope == DataOpe::insert) {
            assert(value);
            table->index[key] = value.value();
        } else if (last_data_ope == DataOpe::update) {
            assert(value);
            table->index[key] = value.value();
        } else {
            assert(last_data_ope == DataOpe::del);
            table->index.erase(key);
        }
        (void)_;
    }

    write_set.clear();
    return true;
}

void Transaction::rollback() {
    write_set.clear();
}

// transactionのselectは返したものをどこかに保管しておく?(read set)
// 複数transactionがあるときに考慮するべき
std::optional<std::string> Transaction::select(const std::string &key) {
    if (write_set.count(key) > 0) { 
        if (write_set[key].last_data_ope != DataOpe::del) {
            return write_set[key].value;
        } else {
            return std::nullopt;
        }
    } else if (table->index.count(key) > 0) {
        return table->index[key];
    }
    return std::nullopt;
}

void Transaction::insert(const std::string &key,const std::string &value) {
    if (write_set.count(key) > 0 && write_set[key].last_data_ope != DataOpe::del) {
        conditional_write_error = true;
    } else {
        DataWrite data_write;
        data_write.first_data_state = write_set.count(key) > 0 ? write_set[key].first_data_state : DataState::not_in_keys;
        data_write.last_data_ope = DataOpe::insert;
        data_write.value = value;
        write_set[key] = data_write;
    }
}

void Transaction::update(const std::string &key,const std::string &value) {
    if (write_set.count(key) > 0 && write_set[key].last_data_ope == DataOpe::del) {
        conditional_write_error = true;
    } else {
        DataWrite data_write;
        data_write.first_data_state = write_set.count(key) > 0 ? write_set[key].first_data_state : DataState::in_keys;
        data_write.last_data_ope = DataOpe::update;
        data_write.value = value;
        write_set[key] = data_write;
    }
}

void Transaction::del(const std::string &key) {
    if (write_set.count(key) > 0 && write_set[key].last_data_ope == DataOpe::del) {
        conditional_write_error = true;
    } else {
        DataWrite data_write;
        data_write.first_data_state = write_set.count(key) > 0 ? write_set[key].first_data_state : DataState::in_keys;
        data_write.last_data_ope = DataOpe::del;
        data_write.value = std::nullopt;
        write_set[key] = data_write; 
    }
}

