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
        data_file >> key; 
        if(key.size() == 0) continue;
        data_file >> value; 
        index[key] = value;
    }

    data_file.close();

    std::ifstream log_file_input(log_manager.log_file_name);
    if (!log_file_input) {
        error("ifstream(log_file_input)");
    }

    std::map<std::string,std::pair<DataOpe,std::string>> write_set;
    std::string buf;
    getline(log_file_input,buf);
    assert(log_file_input.eof());
    size_t idx = 0;
    while (idx < buf.size()) {
        assert(idx+24 < buf.size());
        char mode = buf[idx];
        unsigned int checksum   = from_hex(buf.substr(idx+1,8));
        std::string keysize_str = buf.substr(idx+9,8);
        std::string valuesize_str = buf.substr(idx+17,8);
        unsigned int keysize   = from_hex(keysize_str);
        unsigned int valuesize = from_hex(valuesize_str);

        assert(idx+24+keysize+valuesize < buf.size());
        std::string key = buf.substr(idx+25,keysize);
        std::string value = buf.substr(idx+25+keysize,valuesize);

        if (crc32(keysize_str + valuesize_str + key + value) != checksum) {
            break;
        }
        idx += 25 + keysize + valuesize;

        if (mode == 'i') {
            write_set[key] = make_pair(DataOpe::insert,value);
        } else if (mode == 'u') {
            write_set[key] = make_pair(DataOpe::update,value);
        } else if(mode == 'd') {
            write_set[key] = make_pair(DataOpe::del,value);
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
    log_file_input.close();

    //logを消す。
    log_manager.erase_log();
}