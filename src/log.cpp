#include "db.hpp"

// write (insert,update) log
// w crc32(key+value) sizeof(key)+sizeof(value) key value
// example) w (0x)12345678 (0x)18 aaa bbb \n
// とりあえず改行で区切る
void LogManager::log(LogKind log_kind,const std::string &key,const std::string &value) {
    unsigned int data_size = key.size() + value.size();
    unsigned int check_sum = crc32(key+value);
    std::string buf = "";

    buf += LogKind2str(log_kind); // insert "i"
                                  // update "u" 
                                  // del    "d"
                                  // commit "c"
    buf += to_hex(check_sum);     // 8
    buf += to_hex(data_size);     // 8
    buf += " ";
    buf += key;
    buf += " ";
    buf += value;
    buf += "\n";

    std::ofstream log_file;
    log_file.open(log_file_name,std::ios::app);

    if (!log_file) {
        error("open(log_file)");
    }

    log_file << buf;
    log_file.close();
}


//logを消す
void LogManager::erase_log() {
    std::ofstream log_file;
    log_file.open(log_file_name,std::ios::trunc);
    if (!log_file) {
        error("open(log_file");
    }
    log_file.close();
}

std::string LogKind2str(LogKind log_kind) {
    if (log_kind == LogKind::insert) {
        return "i";
    } else if (log_kind == LogKind::update) {
        return "u";
    } else if (log_kind == LogKind::del) {
        return "d";
    } else if (log_kind == LogKind::commit) {
        return "c";
    }
    error("LogKind2str");
    return "";
}