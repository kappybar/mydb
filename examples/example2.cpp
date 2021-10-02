#include "../src/db.hpp"

my_task transaction1(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();

    std::optional<std::string> v1 = co_await txn.select("key1");
    if (v1.has_value()) {
        co_await txn.update("key1","value1_new");
    } else {
        co_await txn.insert("key1","value1_new");
    }


    std::optional<std::string> v3 = co_await txn.select("key3");
    if (v3.has_value()) {
        co_await txn.del("key3");
    } else {
        co_await txn.insert("key3","value3_new");
    }

    co_yield txn.commit();
    co_return;
}

my_task transaction2(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();

    std::optional<std::string> v3 = co_await txn.select("key3");
    if (v3.has_value()) {
        co_await txn.del("key3");
    } else {
        co_await txn.insert("key3","value3_new");
    }

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