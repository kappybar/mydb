#include "db.hpp"

// database本体のファイルを更新する(atomicに,かなり雑?)。,walのlogを消す
void Table::checkpointing() {

    // tmp_data_fileに書き込み
    std::string data_file_tmp_name = "tmp_" + data_file_name;
    std::ofstream data_file_tmp;
    data_file_tmp.open(data_file_tmp_name,std::ios::trunc);
    if (!data_file_tmp) {
        error("open(data_file)");
    }
    for(auto [key,value] : index) {
        data_file_tmp << key << " " << value << std::endl; //とりあえず適当に
    }
    data_file_tmp.close();

    // rename
    if (rename(data_file_tmp_name.c_str(),data_file_name.c_str()) == -1) {
        error("rename(data_file)");
    }

    // fsync
    file_sync(data_file_name);

    // logを消す
    log_manager.erase_log();
}

// 電源をonしたときにwalにlogが残っていればそのlogから
// database本体のファイルを復元
// そうでないときdatabase本体からdataを読んでin memory のindexに入れる。
// walのlogを消す。
void Table::recovery() {
    assert(index.size() == 0);

    std::ifstream data_file(data_file_name);
    if (!data_file) {
        error("ifstream(data_file)");
    }

    while (!data_file.eof()) {
        std::string key,value;
        data_file >> key; //　とりあえず
        if(key.size() == 0) continue;
        data_file >> value; // とりあえず
        index[key] = value;
    }

    data_file.close();

    std::ifstream log_file(log_manager.log_file_name);
    if (!log_file) {
        error("ifstream(log_file)");
    }

    std::map<std::string,std::pair<DataOpe,std::string>> write_set;
    while (!log_file.eof()) {
        std::string buf;
        log_file >> buf;
        if (buf.size() == 0) continue;
        char mode = buf[0];
        unsigned int checksum = from_hex(buf.substr(1,8));
        // unsigned int datasize = from_hex(buf.substr(9,8)); 必要?

        if (mode == 'i') {
            std::string key,value;
            log_file >> key >> value;

            if (checksum == crc32(key+value)) {
                write_set[key] = make_pair(DataOpe::insert,value);
            } else {
                assert(false); //?
            }
        } else if (mode == 'u') {
            std::string key,value;
            log_file >> key >> value;

            if (checksum == crc32(key+value)) {
                write_set[key] = make_pair(DataOpe::update,value);
            } else {
                assert(false); //?
            }
        } else if(mode == 'd') {
            std::string key;
            log_file >> key;

            if (checksum == crc32(key+"")) {
                write_set[key] = make_pair(DataOpe::del,std::string("")); 
            } else {
                assert(false); //?
            }
        } else {
            assert(mode == 'c');
            for (auto [key,mode_value] : write_set) {
                DataOpe mode = mode_value.first;
                std::string value = mode_value.second;
                if (mode == DataOpe::update || mode == DataOpe::insert) {
                    index[key] = value;
                } else {
                    assert(mode == DataOpe::del);
                    index.erase(key);
                }
            }
            write_set.clear();
        }
    }

    //logを消す。
    log_manager.erase_log();
}