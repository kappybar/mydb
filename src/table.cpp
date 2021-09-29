#include "db.hpp"

// <mode,key,value>
std::optional<std::tuple<char,std::string,std::string>> log2data(size_t &idx,const std::string &buf) {
    if (!(idx+24 < buf.size())) {
        return std::nullopt;
    }

    char mode = buf[idx];
    unsigned int checksum   = from_hex(buf.substr(idx+1,8));
    std::string keysize_str = buf.substr(idx+9,8);
    std::string valuesize_str = buf.substr(idx+17,8);
    unsigned int keysize   = from_hex(keysize_str);
    unsigned int valuesize = from_hex(valuesize_str);

    if (!(idx+24+keysize+valuesize < buf.size())) {
        return std::nullopt;
    }

    std::string key = buf.substr(idx+25,keysize);
    std::string value = buf.substr(idx+25+keysize,valuesize);

    if (crc32(keysize_str + valuesize_str + key + value) != checksum) {
        return std::nullopt;
    }

    idx += 25 + keysize + valuesize;
    return make_tuple(mode,key,value);
}

Table::Table(const std::string &btree_file_name,const std::string &data_file_name,const std::string &log_file_name)
    :btree(btree_file_name),
     data_file_name(data_file_name),
     log_manager(LogManager(log_file_name)),
     lock_manager(LockManager()) 
{
    std::ofstream data_file;
    data_file.open(data_file_name,std::ios::app);
    if (!data_file) {
        error("open(data_file)");
    }
    data_file.close();
}

// flush  btree
// update database dump file
// erase wal
void Table::checkpointing() {

    // btree flush
    btree.flush();

    // write tmp_data_file
    std::string data_file_tmp_name = "tmp_" + data_file_name;
    std::ofstream data_file_tmp;
    data_file_tmp.open(data_file_tmp_name,std::ios::trunc);
    if (!data_file_tmp) {
        error("open(data_file)");
    }
    for(auto [key,value] : btree.all_data()) {
        data_file_tmp << key << " " << value << std::endl;
    }
    data_file_tmp.close();

    file_sync(data_file_tmp_name);

    // rename
    if (rename(data_file_tmp_name.c_str(),data_file_name.c_str()) == -1) {
        error("rename(data_file)");
    }

    // fsync
    file_sync(data_file_name);

    // erase log
    log_manager.erase_log();
}

// 電源をonしたときにwalにlogが残っていればそのlogとdatabase dump fileから
// database本体のファイルとbtreeを復元
// walのlogを消してcheckpointingする。
// そうでないときそのままbtreeをを使う。
void Table::recovery() {

    if (file_size(log_manager.log_file_name) == 0) {
        return;
    }

    btree.clear();

    std::ifstream data_file(data_file_name);
    if (!data_file) {
        error("ifstream(data_file)");
    }

    while (!data_file.eof()) {
        std::string key,value;
        data_file >> key; 
        if(key.size() == 0) continue;
        data_file >> value; 
        btree.insert(key,value);
    }

    data_file.close();

    std::ifstream log_file_input(log_manager.log_file_name);
    if (!log_file_input) {
        error("ifstream(log_file_input)");
    }

    std::map<std::string,std::pair<OpeKind,std::string>> write_set;
    std::string buf;
    getline(log_file_input,buf);
    assert(log_file_input.eof());
    size_t idx = 0;
    while (idx < buf.size()) {
        auto data = log2data(idx,buf);
        if (data == std::nullopt) {
            break;
        } 
        auto [mode,key,value] = data.value();

        if (mode == 'i') {
            write_set[key] = make_pair(OpeKind::insert,value);
        } else if (mode == 'u') {
            write_set[key] = make_pair(OpeKind::update,value);
        } else if(mode == 'd') {
            write_set[key] = make_pair(OpeKind::del,value);
        } else {
            assert(mode == 'c');
            for (auto [key,mode_value] : write_set) {
                OpeKind mode = mode_value.first;
                std::string value = mode_value.second;
                if (mode == OpeKind::update || mode == OpeKind::insert) {
                    if (btree.search(key) == std::nullopt) {
                        btree.insert(key,value);
                    } else {
                        btree.update(key,value);
                    }
                } else {
                    assert(mode == OpeKind::del);
                    if (btree.search(key) != std::nullopt) {
                        btree.del(key);
                    }
                }
            }
            write_set.clear();
        }
    }
    log_file_input.close();

    checkpointing();
}

void Table::add_transaction(my_task&& task) {
    scheduler.add_task(std::move(task));
}

std::vector<bool> Table::exec_transaction(void) {
    return scheduler.start();
}