#include "db.hpp"

int main() {
    std::string data_file_name = "data1.txt";
    std::string log_file_name = "log1.txt";
    Table table(data_file_name,log_file_name);
    table.recovery();

    Transaction txn1(&table);
    txn1.begin();
    for(int i = 0;i < 10; i++) {
        txn1.insert(std::to_string(i),std::to_string(i));
    }
    std::cerr << (txn1.commit() ? "succes" : "fail(conditional write)") << std::endl;
    std::cerr << "commit1" << std::endl;

    Transaction txn2(&table);
    txn2.begin();
    for(int i = 11;i < 1000000; i++) {
        txn2.insert(std::to_string(i),std::to_string(i));
    }
    std::cerr << "commit2 start" << std::endl;
    std::cerr << (txn2.commit() ? "success" : "fail(conditional write)") << std::endl;
    std::cerr << "commit2 end" << std::endl;

    table.checkpointing();
    return 0;
}