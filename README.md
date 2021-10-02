# mydb

### Database management system
* ACID property
* Key-value store (key: std::string, value: std::string, key.size() <= 392, value.size() <= 392)
* Data operation
  * select
  * insert
  * update
  * delete
* Conditional write 
  * cannot update nonexistent key
  * cannot insert exsistent key
  * cannot del nonexistent key

### Implementation 

* Log manager (Redo log)
* Crash recovery
* 4KiB Page
* Disk manager  
* Buffer manager (clock algorithm)
* B-tree
* Concurrency control (S2PL)
* Deadlock prevention (Wait-die algorithm)
* C++20 co_routine 

### Test
```
make test
./test
```

### Example
```
make example1
./example1
```
