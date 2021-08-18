#include "db.hpp"

void Transaction::begin() {
    ;
}

void Transaction::commit() {

    // write ahead log
    // ここatomicに追記したい?
    for (auto [key,value]:write_set) {
        if (value.first == DataOpe::insert) {
            assert(value.second);
            table->log_manager.log(LogKind::insert,key,value.second.value());
        } else if (value.first == DataOpe::update) {
            assert(value.second);
            table->log_manager.log(LogKind::update,key,value.second.value());
        } else {
            assert(value.first == DataOpe::del);
            table->log_manager.log(LogKind::del,key,"");
        }
    }
    table->log_manager.log(LogKind::commit,"","");
    
    // fsync
    file_sync(table->log_manager.log_file_name);

    // in memory indexを更新
    // b-treeにしたときはここを変える？
    for(auto [key,value] : write_set) {
        if (value.first == DataOpe::insert || value.first == DataOpe::update) {
            assert(value.second);
            table->index[key] = value.second.value();
        } else {
            assert(value.first == DataOpe::del);
            table->index.erase(key);
        }
    }

    write_set.clear();
}

void Transaction::rollback() {
    write_set.clear();
}

// transactionのselectは返したものをどこかに保管しておく?(read set)
std::optional<std::string> Transaction::select(const std::string &key) {
    if (write_set.count(key) > 0) { 
        if (write_set[key].first != DataOpe::del) {
            return write_set[key].second;
        } else {
            return std::nullopt;
        }
    } else if (table->index.count(key) > 0) {
        return table->index[key];
    }
    return std::nullopt;
}

void Transaction::insert(const std::string &key,const std::string &value) {
    write_set[key] = make_pair(DataOpe::insert,value);
}

void Transaction::update(const std::string &key,const std::string &value) {
    write_set[key] = make_pair(DataOpe::update,value);
}

void Transaction::del(const std::string &key) {
    write_set[key] = make_pair(DataOpe::del,std::nullopt); 
}

