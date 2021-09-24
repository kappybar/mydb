#include "db.hpp"

LogManager::LogManager(std::string log_file_name) 
    :log_file_name(log_file_name) 
{
    log_file_output.open(log_file_name,std::ios::app);
    if (!log_file_output) {
        error("open(log_file)");
    }
}

LogManager::~LogManager() {
    log_file_output.close();
}

// write (insert,update) log
// w crc32(key+value) sizeof(key)+sizeof(value) key value
// example) w (0x)12345678 (0x)18 aaa bbb 
void LogManager::log(LogKind log_kind,const std::string &key,const std::string &value) {
    unsigned int key_size = key.size();
    unsigned int value_size = value.size();
    std::string key_value_size = to_hex(key_size) + to_hex(value_size);
    unsigned int check_sum = crc32(key_value_size+key+value);
    std::string buf = "";

    buf += LogKind2str(log_kind); // insert "i"
                                  // update "u" 
                                  // del    "d"
                                  // commit "c"
    buf += to_hex(check_sum);     // 8
    buf += key_value_size;        // 16
    buf += key;
    buf += value;

    log_file_output << buf;
}

void LogManager::log_flush() {
    log_file_output << std::flush;
}

//logを消す
void LogManager::erase_log() {
    log_file_output.close();
    log_file_output.open(log_file_name,std::ios::trunc);
    if (!log_file_output) {
        error("open(log_file");
    }
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