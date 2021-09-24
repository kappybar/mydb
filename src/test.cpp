#include "db.hpp"
#include <random>

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


void file_size_check(const std::string &file_name,unsigned int file_size_) {
    assert(file_size(file_name) == file_size_);
}

void page_test(void) {
    {
        char buf[PAGESIZE] = {};
        Page page(0,buf);
        page.write("hello,world!1",checksum_len,13);
        page.write("hello,world!2",100,13);
        const char *res1 = page.read(checksum_len,13);
        const char *res2 = page.read(100,13);
        const char *res3 = page.read(105,8);
        char page_right[PAGESIZE] = {};
        strncpy(page_right+checksum_len,"hello,world!1",14);
        strncpy(page_right+100,"hello,world!2",14);
        assert(strcmp(res1,"hello,world!1") == 0);
        assert(strcmp(res2,"hello,world!2") == 0);
        assert(strcmp(res3,",world!2") == 0);
        assert(strcmp(page_right,page.page) == 0);
        assert(page.access == 1);
        assert(page.dirty);
        assert(page.pin_count == 0);
        free(const_cast<char*>(res1));
        free(const_cast<char*>(res2));
        free(const_cast<char*>(res3));
    }
    {
        char buf[PAGESIZE] = {};
        Page page(0,buf);
        page.write("hello,world!1",checksum_len,13);
        page.write("hello,world!2",100,13);
        page.update_checksum();
        char page_right[PAGESIZE] = {};
        strncpy(page_right+checksum_len,"hello,world!1",14);
        strncpy(page_right+100,"hello,world!2",14);
        int checksum = crc32(page_right+checksum_len,PAGESIZE-checksum_len);
        strncpy(page_right,to_hex(checksum).c_str(),checksum_len);
        assert(strcmp(page_right,page.page) == 0);
    }
    std::cerr << "page_test success!" << std::endl;
}

void disk_manager_test(void) {
    std::string file_name = "disk_test.txt";
    {
        DiskManager disk_manager(file_name);
        disk_manager.allocate_new_page();
        disk_manager.allocate_new_page();
        file_size_check(file_name,2 * PAGESIZE);
        Page page = disk_manager.fetch_page(0);
        page.write("hello,world!1",checksum_len,13);
        page.write("hello,world!2",100,13);
        disk_manager.write_page(0,page);
        disk_manager.flush();
        Page page_disk = disk_manager.fetch_page(0);
        assert(strcmp(page.page,page_disk.page) == 0);
        assert(disk_manager.page_num == 2);
    }
    {
        DiskManager disk_manager(file_name);   
        assert(disk_manager.page_num == 2);
        Page page = disk_manager.fetch_page(1);
        page.write("end of file!",4084,12);
        disk_manager.write_page(1,page);
        disk_manager.flush();
        Page page_disk = disk_manager.fetch_page(1);
        assert(strcmp(page.page,page_disk.page) == 0);
    }
    remove(file_name.c_str());
    std::cerr << "disk_manager_test success!" << std::endl;
}

void buffer_manager_test(void) {
    std::string file_name = "buffer_test.txt";
    {
        BufferManager buffer_manager(file_name);
        int pageid0 = buffer_manager.create_new_page();
        int pageid1 = buffer_manager.create_new_page();
        assert(pageid0 == 0);
        assert(pageid1 == 1);
        buffer_manager.fetch_page(pageid0);
        buffer_manager.fetch_page(pageid1);
        assert(buffer_manager.pagetable.size() == 2);
        assert(buffer_manager.pages[0].pageid == pageid0);
        assert(buffer_manager.pages[1].pageid == pageid1);
        assert(buffer_manager.pagetable.size() == 2);
        assert(buffer_manager.pagetable[pageid0] == 0);
        assert(buffer_manager.pagetable[pageid1] == 1);

        buffer_manager.write_page(pageid0,"hello,world!0",checksum_len,13);
        const char *buf = buffer_manager.read_page(pageid0,checksum_len,13);
        assert(strcmp(buf,"hello,world!0") == 0);
        free(const_cast<char*>(buf));
        buffer_manager.write_page(pageid1,"hello,world!1",100,13);
        buffer_manager.flush();
        file_size_check(file_name,2 * PAGESIZE);
    }
    {
        BufferManager buffer_manager(file_name);
        int pageid2 = buffer_manager.create_new_page();
        assert(pageid2 == 2);
        const char *buf0 = buffer_manager.read_page(0,checksum_len,13);
        const char *buf1 = buffer_manager.read_page(1,100,13);
        assert(strcmp(buf0,"hello,world!0") == 0);
        assert(strcmp(buf1,"hello,world!1") == 0);
        free(const_cast<char*>(buf0));
        free(const_cast<char*>(buf1));
    }
    {
        BufferManager buffer_manager(file_name);
        for(int i = 0;i < MAX_BUFFER_SIZE - 3; i++) {
            buffer_manager.create_new_page();
        }
        for(int i = 0;i < MAX_BUFFER_SIZE; i++) {
            buffer_manager.write_page(i,to_hex(i).c_str(),checksum_len,8);
        }
        assert((int)buffer_manager.pages.size() == MAX_BUFFER_SIZE);
        assert((int)buffer_manager.pagetable.size() == MAX_BUFFER_SIZE);
        for(int i = 0;i < MAX_BUFFER_SIZE; i++) {
            assert(buffer_manager.pages[i].pageid == i);
            assert(buffer_manager.pagetable[i] == i);
        }
        int pageid = buffer_manager.create_new_page();
        buffer_manager.write_page(pageid,to_hex(pageid).c_str(),checksum_len,8);
        assert((int)buffer_manager.pages.size() == MAX_BUFFER_SIZE);
        assert((int)buffer_manager.pagetable.size() == MAX_BUFFER_SIZE);
        assert(buffer_manager.pagetable[pageid] == 0);
        assert(buffer_manager.pages[0].pageid == pageid);
        buffer_manager.flush();
        file_size_check(file_name,(MAX_BUFFER_SIZE + 1) * PAGESIZE);
    }
    {
        BufferManager buffer_manager(file_name);
        for(int i = 0;i < MAX_BUFFER_SIZE + 1; i++) {
            const char *buf = buffer_manager.read_page(i,checksum_len,8);
            assert(strcmp(buf,to_hex(i).c_str()) == 0);
            free(const_cast<char*>(buf));
        }
    }
    remove(file_name.c_str());
    std::cerr << "buffer_manager_test success!" << std::endl;
}

void set_node( Node &node,
               bool is_leaf,
               int keys_size,
               const std::vector<int> &child_pageid,
               const std::vector<std::string> &keys,
               const std::vector<std::string> &values) {
    node.set_is_leaf(is_leaf);
    node.set_keys_size(keys_size);
    for(int i = 0;i < keys_size; i++) {
        node.set_keys(i,keys[i]);
        node.set_values(i,values[i]);
    }
    if(!is_leaf){
        for(int i = 0;i < keys_size + 1; i++) {
            node.set_child_pageid(i,child_pageid[i]);
        }
    }
}

void check_node( Node &node,
                 bool is_leaf,
                 int keys_size,
                 const std::vector<int> &child_pageid,
                 const std::vector<std::string> &keys,
                 const std::vector<std::string> &values) {
    assert(node.is_leaf() == is_leaf);
    assert(node.keys_size() == keys_size);
    for(int i = 0;i < keys_size; i++) {
        assert(node.keys(i) == keys[i]);
        assert(node.values(i) == values[i]);
    }
    if(!is_leaf){
        for(int i = 0;i < keys_size + 1; i++) {
            assert(node.child_pageid(i) == child_pageid[i]);
        }
    }
}

void node_test(void) {
    std::string file_name = "node_test.txt";
    {
        BufferManager buffer_manager(file_name);
        buffer_manager.create_new_page();
        Node node(&buffer_manager,0);
        node.set_is_leaf(false);
        node.set_keys_size(1);
        node.set_child_pageid(0,1);
        node.set_child_pageid(1,2);
        node.set_keys(0,"key");
        node.set_values(0,"value");

        check_node(node,false,1,{1,2},{"key"},{"value"});
        assert(!node.isfull());
        remove(file_name.c_str());
    }
    {
        BufferManager buffer_manager(file_name);
        int pageid0 = buffer_manager.create_new_page();   
        int pageid1 = buffer_manager.create_new_page();
        Node node0(&buffer_manager,pageid0);   
        Node node1(&buffer_manager,pageid1);   
        node0.set_is_leaf(true);
        node0.set_keys_size(5);
        for(int i = 0;i < 5;i++){
            node0.set_keys(i,"key" + std::to_string(i));
            node0.set_values(i,"value" + std::to_string(i));
        }
        node1.set_is_leaf(false);
        node1.set_keys_size(0);
        node1.set_child_pageid(0,pageid0);
        node1.splitchild(0);

        Node node2(&buffer_manager,2);
        check_node(node0,true,2,{},{"key0","key1"},{"value0","value1"});
        check_node(node1,false,1,{pageid0,2},{"key2"},{"value2"});
        check_node(node2,true,2,{},{"key3","key4"},{"value3","value4"});
        remove(file_name.c_str());
    }
    {
        BufferManager buffer_manager(file_name);
        int pageid0 = buffer_manager.create_new_page();
        int pageid1 = buffer_manager.create_new_page();
        int pageid2 = buffer_manager.create_new_page();
        Node node0(&buffer_manager,pageid0);
        Node node1(&buffer_manager,pageid1);
        Node node2(&buffer_manager,pageid2);
        set_node(node0,false,1,{pageid1,pageid2},{"key2"},{"value2"});
        set_node(node1,true,2,{},{"key0","key1"},{"value0","value1"});
        set_node(node2,true,2,{},{"key3","key4"},{"value3","value4"});
        node0.rightshift(0);
        node0.leftshift(0);
        check_node(node0,false,1,{pageid1,pageid2},{"key2"},{"value2"});
        check_node(node1,true,2,{},{"key0","key1"},{"value0","value1"});
        check_node(node2,true,2,{},{"key3","key4"},{"value3","value4"});
        remove(file_name.c_str());
    }
    {
        BufferManager buffer_manager(file_name);
        int pageid0 = buffer_manager.create_new_page();
        int pageid1 = buffer_manager.create_new_page();
        int pageid2 = buffer_manager.create_new_page();
        int pageid3 = buffer_manager.create_new_page();
        Node node0(&buffer_manager,pageid0);
        Node node1(&buffer_manager,pageid1);
        Node node2(&buffer_manager,pageid2);
        Node node3(&buffer_manager,pageid3);
        set_node(node0,false,2,{pageid1,pageid2,pageid3},{"key2","key5"},{"value2","value5"});
        set_node(node1,true,2,{},{"key0","key1"},{"value0","value1"});
        set_node(node2,true,2,{},{"key3","key4"},{"value3","value4"});
        set_node(node3,true,2,{},{"key6","key7"},{"value6","value7"});
        node0.merge(0);
        check_node(node0,false,1,{pageid1,pageid3},{"key5"},{"value5"});
        check_node(node1,true,5,{},{"key0","key1","key2","key3","key4"},{"value0","value1","value2","value3","value4"});
        check_node(node3,true,2,{},{"key6","key7"},{"value6","value7"});
        remove(file_name.c_str());
    }
    std::cerr << "node_test success!" << std::endl;
}

void btree_ondisk_test(void) {
    std::string file_name = "btree_ondisk_test.txt";
    std::map<std::string,std::string> mp;
    {
        BTree btree(file_name);
        std::random_device seed_gen;
        std::mt19937_64 rnd(seed_gen());
        std::uniform_int_distribution<int> dist(0,1);
        for(int i = 0;i < 10000; i++) {
            btree.insert("key" + std::to_string(i),"value" + std::to_string(i));
            mp["key" + std::to_string(i)] = "value" + std::to_string(i);
        }
        for(int i = 0;i < 10000; i+= 2) {
            btree.update("key" + std::to_string(i),std::to_string(i));
            mp["key" + std::to_string(i)] = std::to_string(i);
        }
        for(int i = 0;i < 10000; i++) {
            int k = dist(rnd);
            if (k == 0) {
                assert(btree.del("key" + std::to_string(i)));
                mp.erase("key" + std::to_string(i));
            }
        }
        for(int i = 0;i < 10000; i++) {
            std::optional<std::string> value = btree.search("key" + std::to_string(i));
            if (value == std::nullopt) {
                assert(mp.count("key" + std::to_string(i)) == 0);
            } else {
                std::string right_value = mp["key" + std::to_string(i)];
                assert(value.value() == right_value);
            }
        }
        assert(btree.all_data() == mp);
    }
    {
        BTree btree2(file_name);
        assert(btree2.all_data() == mp);
        btree2.clear();
        auto index = btree2.all_data();
        assert(index.size() == 0);
    }
    remove(file_name.c_str());
    std::cerr << "btree_ondisk_test success!" << std::endl;
} 

void lock_manager_test(void) {
    {
        Lock lock = Lock_exclusive(1);
        assert(lock.has_exclusive_lock());
        assert(lock.has_priority(2));
        assert(!lock.has_priority(0));
    }
    {
        Lock lock = Lock_shared(1);
        lock.add_reader(2);
        lock.add_reader(4);
        assert(lock.has_shared_lock());
        assert(lock.has_priority(3));
        assert(!lock.has_priority(0));
        lock.delete_reader(1);
        assert(!lock.has_priority(1));
    }
    {
        LockManager lock_manager = LockManager();
        assert(lock_manager.try_exclusive_lock("key",3) == TryLockResult::GetLock);
        assert(lock_manager.try_exclusive_lock("key",3) == TryLockResult::GetLock);
        assert(lock_manager.try_exclusive_lock("key",1) == TryLockResult::Wait);
        assert(lock_manager.try_exclusive_lock("key",5) == TryLockResult::Abort);
        assert(lock_manager.try_shared_lock("key",3) == TryLockResult::GetLock);
        assert(lock_manager.try_shared_lock("key",1) == TryLockResult::Wait);
        assert(lock_manager.try_shared_lock("key",5) == TryLockResult::Abort);
        lock_manager.unlock("key",3);
        assert(lock_manager.try_shared_lock("key",5) == TryLockResult::GetLock);
        assert(lock_manager.try_shared_lock("key",3) == TryLockResult::GetLock);
        assert(lock_manager.try_shared_lock("key",1) == TryLockResult::GetLock);
        assert(lock_manager.try_exclusive_lock("key",2) == TryLockResult::Abort);
        assert(lock_manager.try_exclusive_lock("key",0) == TryLockResult::Wait);;
    }
    {
        LockManager lock_manager = LockManager();
        assert(lock_manager.try_shared_lock("key",2) == TryLockResult::GetLock);
        assert(lock_manager.try_exclusive_lock("key",2) == TryLockResult::GetLock);
    }
    {
        LockManager lock_manager = LockManager();
        assert(lock_manager.try_shared_lock("key",2) == TryLockResult::GetLock);
        assert(lock_manager.try_shared_lock("key",1) == TryLockResult::GetLock);
        assert(lock_manager.try_exclusive_lock("key",2) == TryLockResult::Abort);
    }
    std::cerr << "lock_manager_test success!" << std::endl;
}



void table_test() {
    std::string btree_file_name = "btree1.txt";
    std::string data_file_name = "data1.txt";
    std::string log_file_name = "log1.txt";
    {
        Table table(btree_file_name,data_file_name,log_file_name);
        table.btree.insert("key1","value1");
        table.btree.insert("key2","value2");
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
        table.btree.clear();
        table.recovery();
        auto index = table.btree.all_data();
        assert(index.size() == 4);
        assert(index["key1"] == "value1");
        assert(index["key2"] == "value2");
        assert(index["key3"] == "value3");
        assert(index["key4"] == "value4");
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        Table table(btree_file_name,data_file_name,log_file_name);
        table.btree.insert("key1","value1");
        table.btree.insert("key2","value2");
        table.checkpointing();

        table.log_manager.log(LogKind::insert,"key3","value3");
        table.log_manager.log(LogKind::insert,"key4","value4");
        table.log_manager.log(LogKind::update,"key1","value1_new");
        table.log_manager.log(LogKind::del,"key2","");
        table.log_manager.log(LogKind::commit,"","");
        table.log_manager.log_flush();

        //電源on
        table.btree.clear();
        table.recovery();
        auto index = table.btree.all_data();
        assert(index.size() == 3);
        assert(index["key1"] == "value1_new");
        assert(index["key3"] == "value3");
        assert(index["key4"] == "value4");
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        Table table(btree_file_name,data_file_name,log_file_name);
        table.btree.insert("key1","value1");
        table.btree.insert("key2","value2");
        table.checkpointing();

        table.log_manager.log(LogKind::insert,"key3","value3");
        table.log_manager.log(LogKind::insert,"key4","value4");
        table.log_manager.log(LogKind::update,"key1","value1_new");
        table.log_manager.log(LogKind::del,"key2","");
        table.log_manager.log_flush();
        // commitされていないからこのlogは無視する。

        //電源on
        table.btree.clear();
        table.recovery();
        auto index = table.btree.all_data();
        assert(index.size() == 2);
        assert(index["key1"] == "value1");
        assert(index["key2"] == "value2");
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    std::cerr << "table_test success!" << std::endl;
}

// 同時に一つのtransaction
void transaction_test() {
    std::string btree_file_name = "btree1.txt";
    std::string data_file_name = "data1.txt";
    std::string log_file_name = "log1.txt";
    {   
        Table table(btree_file_name,data_file_name,log_file_name);
        Transaction txn(&table);
        txn.begin();
        txn.insert("key1","value1");
        txn.insert("key2","value2");
        assert(txn.commit());
        assert(txn.write_set.size() == 0);
        auto index = txn.table->btree.all_data(); 
        assert(index.size() == 2);
        assert(index["key1"] == "value1");
        assert(index["key2"] == "value2");
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        Table table(btree_file_name,data_file_name,log_file_name);
        table.btree.insert("key1","value1");
        table.btree.insert("key2","value2");
        Transaction txn(&table);
        txn.begin();
        txn.update("key1","value1_new");
        txn.insert("key3","value3");
        txn.del("key2");

        // 未commit
        auto index = table.btree.all_data();
        assert(index.size() == 2);
        assert(index["key1"] == "value1");
        assert(index["key2"] == "value2");

        // commit
        assert(txn.commit());
        index = table.btree.all_data();
        assert(index.size() == 2);
        assert(index["key1"] == "value1_new");
        assert(index["key3"] == "value3");
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        Table table(btree_file_name,data_file_name,log_file_name);
        table.btree.insert("key1","value1");
        table.btree.insert("key2","value2");
        Transaction txn(&table);
        txn.begin();
        txn.select("key1");assert(txn.get_value("key1") == "value1");
        txn.select("key2");assert(txn.get_value("key2") == "value2");
        txn.select("key3");assert(txn.get_value("key3") == std::nullopt);
        txn.update("key1","value1_new");
        txn.del("key2");
        txn.insert("key3","value3");
        txn.select("key1");assert(txn.get_value("key1") == "value1_new");
        txn.select("key2");assert(txn.get_value("key2") == std::nullopt);
        txn.select("key3");assert(txn.get_value("key3") == "value3");

        assert(txn.commit());
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        Table table(btree_file_name,data_file_name,log_file_name);
        table.btree.insert("key1","value1");
        table.btree.insert("key2","value2");
        Transaction txn(&table);
        txn.begin();
        txn.update("key1","value1_new");
        txn.insert("key3","value3");
        txn.rollback();
        auto index = table.btree.all_data();
        assert(index.size() == 2);
        assert(index["key1"] == "value1");
        assert(index["key2"] == "value2");
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        Table table(btree_file_name,data_file_name,log_file_name);
        table.btree.insert("key1","value1");
        table.btree.insert("key2","value2");
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
        auto index = table.btree.all_data();
        assert(index.size() == 2);
        assert(index["key1"] == "value1");
        assert(index["key2"] == "value2");
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    {
        Table table(btree_file_name,data_file_name,log_file_name);
        table.btree.insert("key1","value1");
        table.btree.insert("key2","value2");
        Transaction txn(&table);
        txn.begin();
        txn.del("key1");
        txn.insert("key1","value1_new");
        txn.update("key2","value2");
        txn.del("key2");
        txn.insert("key2","value2_new");
        assert(txn.commit());
        auto index = table.btree.all_data();
        assert(index.size() == 2);
        assert(index["key1"] == "value1_new");
        assert(index["key2"] == "value2_new");
        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    std::cerr << "transaction_test success!" << std::endl;
}

my_task transaction1(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();
    co_await txn.insert("key1","value1"); // getlock
    co_await txn.update("key2","value1"); // wait
    auto v = co_await txn.select("key1");
    assert(v.value() == "value1");
    co_yield txn.commit();
    co_return;
}

my_task transaction2(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();
    co_await txn.insert("key2","value2"); //getlock
    auto v = co_await txn.select("key2");
    assert(v.value() == "value2");
    co_yield txn.commit();
    co_return;
}

my_task transaction3(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();
    co_await txn.insert("key3","value3"); 
    co_await txn.insert("key4","value3"); 
    auto v3 = co_await txn.select("key3");
    auto v4 = co_await txn.select("key4");
    assert(v3.value() == "value3");
    assert(v4.value() == "value3");
    co_yield txn.commit();
    co_return;
}

my_task transaction4(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();
    co_await txn.insert("key4","value4"); 
    co_await txn.insert("key3","value4"); 
    auto v3 = co_await txn.select("key3");
    auto v4 = co_await txn.select("key4");
    assert(v3.value() == "value4");
    assert(v4.value() == "value4");
    co_yield txn.commit();
    co_return;
}

my_task transaction5(Table *table) {
    Transaction txn(table);
    co_yield txn.begin();
    auto v1 = co_await txn.select("key1"); 
    assert(v1.value() == "value1");
    auto v2 = co_await txn.select("key2"); 
    assert(v2.value() == "value1");
    auto v3 = co_await txn.select("key3"); 
    assert(v3.value() == "value3");
    auto v4 = co_await txn.select("key4"); 
    assert(v4.value() == "value3");
    co_yield txn.commit();
    co_return;
}

void concurrent_test(void) {
    std::string btree_file_name = "btree1.txt";
    std::string data_file_name = "data1.txt";
    std::string log_file_name = "log1.txt";
    {
        Table table(btree_file_name,data_file_name,log_file_name);

        // transaction2 commit 
        // transaction1 commit
        table.add_transaction(transaction1(&table));
        table.add_transaction(transaction2(&table));
        table.start();
        auto index = table.btree.all_data();
        assert(index.size() == 2);
        assert(index["key1"] == "value1");
        assert(index["key2"] == "value1");

        // transaction4 abort wait-die
        // transaction3 commit 
        table.add_transaction(transaction3(&table));
        table.add_transaction(transaction4(&table));
        table.start();
        index = table.btree.all_data();
        assert(index.size() == 4);
        assert(index["key1"] == "value1");
        assert(index["key2"] == "value1");
        assert(index["key3"] == "value3");
        assert(index["key4"] == "value3");

        // transaction5 commit
        // transaction5 commit
        table.add_transaction(transaction5(&table));
        table.add_transaction(transaction5(&table));
        table.start();

        remove(btree_file_name.c_str());
        remove(data_file_name.c_str());
        remove(log_file_name.c_str());
    }
    std::cerr << "concurrent_test success!" << std::endl;
}

int main() {
    util_test();
    log_test();
    page_test();
    disk_manager_test();
    buffer_manager_test();
    node_test();
    btree_ondisk_test();
    lock_manager_test();
    table_test();
    transaction_test();
    concurrent_test();
    std::cerr << "all test success!" << std::endl;
    return 0;
}