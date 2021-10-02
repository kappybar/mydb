#include "../src/db.hpp"

my_task transaction1(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();

    co_await txn.insert("key1","value1");
    co_await txn.insert("key3","value3");
    co_await txn.insert("key5","value5");

    co_yield txn.commit();
    co_return;
}

my_task transaction2(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();

    co_await txn.insert("key2","value2");
    co_await txn.insert("key4","value4");
    co_await txn.insert("key6","value6");

    co_yield txn.commit();
    co_return;
}

int main() {
    std::string btree_file_name = "btree.txt";
    std::string log_file_name = "log.txt";
    std::string data_file_name = "data.txt";

    Table table(btree_file_name,data_file_name,log_file_name);
    table.recovery();

    table.add_transaction(transaction1(&table));
    table.add_transaction(transaction2(&table));

    // transaction1 transaction2 interleave execution
    // commit == true -> commit success
    // commit == false -> abort
    std::vector<bool> commit = table.exec_transaction();

    table.checkpointing();
    return 0;
}