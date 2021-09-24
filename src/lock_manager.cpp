#include "db.hpp"

Lock::Lock() {}

Lock::Lock(LockKind lock_kind,int txnid)
    :lock_kind(lock_kind),
     txnid(txnid),
     txnnum(0) {}

Lock::Lock(LockKind lock_kind,int txnnum,const std::set<int>& readers)
    :lock_kind(lock_kind),
     txnid(-1),
     txnnum(txnnum),
     readers(readers) {}

Lock Lock_exclusive(int txnid) {
    return Lock(LockKind::Exclusive,txnid);
}

Lock Lock_shared(int txnid) {
    return Lock(LockKind::Share,1,{txnid});
}

bool Lock::has_exclusive_lock(void) {
    return lock_kind == LockKind::Exclusive;
}

bool Lock::has_shared_lock(void) {
    return lock_kind == LockKind::Share;
}

void Lock::add_reader(int txnid) {
    assert(lock_kind == LockKind::Share);
    ++txnnum;
    readers.insert(txnid);
    return;
}

void Lock::delete_reader(int txnid) {
    assert(lock_kind == LockKind::Share);
    --txnnum;
    readers.erase(txnid);
    return;
}

bool Lock::has_priority(int txnid_) {
    switch (lock_kind) {
        case LockKind::Exclusive:
            return txnid < txnid_;
        case LockKind::Share:
            for (auto r : readers) {
                if (r < txnid_) {
                    return true;
                }
            }
            return false;
        default:
            assert(false);
    }
}


TryLockResult LockManager::try_shared_lock(const std::string& s,int txnid) {
    if (lock_table.count(s) > 0) {
        if (lock_table[s].has_exclusive_lock()) {
            if (lock_table[s].txnid == txnid) {
                return TryLockResult::GetLock;
            }
            // Wait-Die
            if (lock_table[s].has_priority(txnid)) {
                return TryLockResult::Abort;
            } else {
                return TryLockResult::Wait;
            }
        } else {
            assert(lock_table[s].has_shared_lock());
            if (lock_table[s].readers.count(txnid) == 0) {
                lock_table[s].add_reader(txnid);
            }
            return TryLockResult::GetLock;
        }
    } else {
        lock_table[s] = Lock_shared(txnid);
        return TryLockResult::GetLock;
    }
}

TryLockResult LockManager::try_exclusive_lock(const std::string& s,int txnid) {
    if (lock_table.count(s) > 0) {
        if (lock_table[s].has_exclusive_lock() && lock_table[s].txnid == txnid) {
            return TryLockResult::GetLock;
        } else if (lock_table[s].has_shared_lock() && lock_table[s].readers.count(txnid) > 0) {
            return try_upgrade_lock(s,txnid);
        }
        // Wait-Die
        if (lock_table[s].has_priority(txnid)) {
            return TryLockResult::Abort;
        } else {
            return TryLockResult::Wait;
        }
    } else {
        lock_table[s] = Lock_exclusive(txnid);
        return TryLockResult::GetLock;
    }
}

TryLockResult LockManager::try_upgrade_lock(const std::string& s,int txnid) {
    assert(lock_table.count(s) > 0 && lock_table[s].has_shared_lock() && lock_table[s].readers.count(txnid) > 0);
    if (lock_table[s].readers.size() == 1) {
        lock_table[s] = Lock_exclusive(txnid);
        return TryLockResult::GetLock;
    } else {
        // Wait-Die
        if (lock_table[s].has_priority(txnid)) {
            return TryLockResult::Abort;
        } else {
            return TryLockResult::Wait;
        }
    }
}


void LockManager::unlock(const std::string& s,int txnid) {
    assert(lock_table.count(s) > 0);
    switch (lock_table[s].lock_kind) {
        case LockKind::Share:
            assert(lock_table[s].readers.count(txnid) > 0);
            lock_table[s].delete_reader(txnid);
            if (lock_table[s].txnnum == 0) {
                lock_table.erase(s);
            }
            break;
        case LockKind::Exclusive:
            assert(lock_table[s].txnid == txnid);
            lock_table.erase(s);
            break;
        default:
            assert(false);
    }
}



