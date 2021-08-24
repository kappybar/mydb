#include "db.hpp"
#include <iostream>

void util_test() {
    {
        //crc32
        std::string s1 = "abcdefghijklmnopqrstuvwxyz";
        unsigned int crc1 = crc32(s1);
        assert(crc1 == 0x4c2750bd);

        std::string s2 = "keyvalue";
        unsigned int crc2 = crc32(s2);
        assert(crc2 == 0xc655f3e6);
    }
    {
        //from_hex to_hex
        unsigned int number1 = 0x9fa83c09;
        std::string s1 = "9fa83c09";
        assert(from_hex(s1) == number1);
        assert(to_hex(number1) == s1);

        unsigned int number2 = 0x00001234;
        std::string s2 = "00001234";
        assert(from_hex(s2) == number2);
        assert(to_hex(number2) == s2);
    }
    std::cerr << "util_test success!" << std::endl;
}

void log_test() {
    {
        std::string log_file_name = "log1.txt";
        std::string key = "key1";
        std::string value = "value1";
        LogManager log_manager(log_file_name);
        log_manager.log(LogKind::insert,key,value);
        log_manager.log(LogKind::update,key,value);
        log_manager.log(LogKind::del,key,"");
        log_manager.log(LogKind::commit,"","");
        log_manager.log_flush();

        unsigned int key_size = key.size();
        unsigned int value_size = value.size();
        unsigned int check_sum1 = crc32(to_hex(key_size)+to_hex(value_size)+key+value);
        unsigned int check_sum2 = crc32(to_hex(key_size)+to_hex(0)+key);
        unsigned int check_sum3 = crc32(to_hex(0)+to_hex(0));
        std::string buf = "";
        buf += LogKind2str(LogKind::insert) + to_hex(check_sum1) + to_hex(key_size) + to_hex(value_size) + key + value; 
        buf += LogKind2str(LogKind::update) + to_hex(check_sum1) + to_hex(key_size) + to_hex(value_size) + key + value;
        buf += LogKind2str(LogKind::del)    + to_hex(check_sum2) + to_hex(key_size) + to_hex(0)          + key;
        buf += LogKind2str(LogKind::commit) + to_hex(check_sum3) + to_hex(0)        + to_hex(0);
        std::ifstream log_file(log_file_name);
        std::string log;
        getline(log_file,log);
        assert(log == buf);
        log_file.close();
        remove(log_file_name.c_str());
    }
    std::cerr << "log_test success!" << std::endl;
}

void table_test() {
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        table.index["key1"] = "value1";
        table.index["key2"] = "value2";
        table.checkpointing();
        std::vector<std::string> str = {"key1","value1","key2","value2",""};
        int i = 0;
        std::ifstream data_file(data_file_name);
        while (!data_file.eof()) {
            assert(i < (int)str.size());
            std::string tmp;
            data_file >> tmp;
            assert(tmp == str[i++]);
        }
        data_file.close();

        table.log_manager.log(LogKind::insert,"key3","value3");
        table.log_manager.log(LogKind::insert,"key4","value4");
        table.log_manager.log(LogKind::commit,"","");
        table.log_manager.log_flush();

        //電源on
        table.index.clear();
        table.recovery();
        assert(table.index.size() == 4);
        assert(table.index["key1"] == "value1");
        assert(table.index["key2"] == "value2");
        assert(table.index["key3"] == "value3");
        assert(table.index["key4"] == "value4");
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        table.index["key1"] = "value1";
        table.index["key2"] = "value2";
        table.checkpointing();

        table.log_manager.log(LogKind::insert,"key3","value3");
        table.log_manager.log(LogKind::insert,"key4","value4");
        table.log_manager.log(LogKind::update,"key1","value1_new");
        table.log_manager.log(LogKind::del,"key2","");
        table.log_manager.log(LogKind::commit,"","");
        table.log_manager.log_flush();

        //電源on
        table.index.clear();
        table.recovery();
        assert(table.index.size() == 3);
        assert(table.index["key1"] == "value1_new");
        assert(table.index["key3"] == "value3");
        assert(table.index["key4"] == "value4");
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        table.index["key1"] = "value1";
        table.index["key2"] = "value2";
        table.checkpointing();

        table.log_manager.log(LogKind::insert,"key3","value3");
        table.log_manager.log(LogKind::insert,"key4","value4");
        table.log_manager.log(LogKind::update,"key1","value1_new");
        table.log_manager.log(LogKind::del,"key2","");
        table.log_manager.log_flush();
        // commitされていないからこのlogは無視する。

        //電源on
        table.index.clear();
        table.recovery();
        assert(table.index.size() == 2);
        assert(table.index["key1"] == "value1");
        assert(table.index["key2"] == "value2");
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    std::cerr << "table_test success!" << std::endl;
}

// 同時に一つのtransaction
void transaction_test() {
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        Transaction txn(&table);
        txn.begin();
        txn.insert("key1","value1");
        txn.insert("key2","value2");
        assert(txn.commit());
        assert(txn.write_set.size() == 0);
        assert(txn.table->index.size() == 2);
        assert(txn.table->index["key1"] == "value1");
        assert(txn.table->index["key2"] == "value2");
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        table.index["key1"] = "value1";
        table.index["key2"] = "value2";
        Transaction txn(&table);
        txn.begin();
        txn.update("key1","value1_new");
        txn.insert("key3","value3");
        txn.del("key2");

        // 未commit
        assert(table.index.size() == 2);
        assert(table.index["key1"] == "value1");
        assert(table.index["key2"] == "value2");

        // commit
        assert(txn.commit());
        assert(table.index.size() == 2);
        assert(table.index["key1"] == "value1_new");
        assert(table.index["key3"] == "value3");
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        table.index["key1"] = "value1";
        table.index["key2"] = "value2";
        Transaction txn(&table);
        txn.begin();
        assert(txn.select("key1") == "value1");
        assert(txn.select("key2") == "value2");
        assert(txn.select("key3") == std::nullopt);
        txn.update("key1","value1_new");
        txn.del("key2");
        txn.insert("key3","value3");
        assert(txn.select("key1") == "value1_new");
        assert(txn.select("key2") == std::nullopt);
        assert(txn.select("key3") == "value3");

        assert(txn.commit());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        table.index["key1"] = "value1";
        table.index["key2"] = "value2";
        Transaction txn(&table);
        txn.begin();
        txn.update("key1","value1_new");
        txn.insert("key3","value3");
        txn.rollback();
        assert(table.index.size() == 2);
        assert(table.index["key1"] == "value1");
        assert(table.index["key2"] == "value2");
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        table.index["key1"] = "value1";
        table.index["key2"] = "value2";
        Transaction txn(&table);
        txn.begin();
        txn.update("key3","value3");
        assert(!txn.commit());
        txn.begin();
        txn.insert("key1","value1_new");
        assert(!txn.commit());
        txn.begin();
        txn.del("key4");
        assert(!txn.commit());
        assert(table.index.size() == 2);
        assert(table.index["key1"] == "value1");
        assert(table.index["key2"] == "value2");
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        std::string data_file_name = "data1.txt";
        std::string log_file_name = "log1.txt";
        Table table(data_file_name,log_file_name);
        table.index["key1"] = "value1";
        table.index["key2"] = "value2";
        Transaction txn(&table);
        txn.begin();
        txn.del("key1");
        txn.insert("key1","value1_new");
        txn.update("key2","value2");
        txn.del("key2");
        txn.insert("key2","value2_new");
        assert(txn.commit());
        assert(table.index.size() == 2);
        assert(table.index["key1"] == "value1_new");
        assert(table.index["key2"] == "value2_new");
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    std::cerr << "transaction_test success!" << std::endl;
}

// void recovery_test() {
//     ?
// }

int main() {
    util_test();
    log_test();
    table_test();
    transaction_test();
    std::cerr << "all test success!" << std::endl;
    return 0;
}