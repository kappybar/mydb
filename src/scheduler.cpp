#include "db.hpp"

void Scheduler::add_task(my_task &&task) {
    tasks.emplace_back(std::move(task));
    states.emplace_back(State::Execute);
    // transactionsのemplace_backはtransaction.begin()のタイミングで行われる。
}

std::vector<bool> Scheduler::start(void) {
    int tasks_size = static_cast<int>(tasks.size());
    if (tasks_size == 0) {
        return {};
    }

    int finish_task_count = 0;
    int idx = 0;
    while (finish_task_count < tasks_size) {
        switch (states[idx]) {
            case State::Execute :
                if (tasks[idx].can_move()) {
                    tasks[idx].move_next();
                    if (tasks[idx].waiting()) {
                        states[idx] = State::Wait;
                    }
                    if (tasks[idx].abort()) {
                        tasks[idx].destroy_handle();
                        states[idx] = State::Done;
                        ++finish_task_count;
                    }
                    if (tasks[idx].commit()) {
                        tasks[idx].destroy_handle();
                        states[idx] = State::Done;
                        ++finish_task_count;
                    }
                } else {
                    transactions[idx]->unlock();
                    states[idx] = State::Done;
                    ++finish_task_count;
                }
                break;
            case State::Wait :
                // try waiting lock
                // exec waiting data operation
                {
                    TryLockResult try_lock_result;
                    auto [data_ope,key,value] = tasks[idx].data_operation();
                    switch (data_ope) {
                        case OpeKind::select:
                            try_lock_result = transactions[idx]->select_internal(key);
                            break;
                        case OpeKind::insert:
                            try_lock_result = transactions[idx]->insert_internal(key,value);
                            break;
                        case OpeKind::update:
                            try_lock_result = transactions[idx]->update_internal(key,value);
                            break;
                        case OpeKind::del:
                            try_lock_result = transactions[idx]->del_internal(key);
                            break;
                        default:
                            assert(false);
                    }
                    switch (try_lock_result) {
                        case TryLockResult::GetLock:
                            states[idx] = State::Execute;
                            break;
                        case TryLockResult::Abort:
                            tasks[idx].destroy_handle();
                            states[idx] = State::Done;
                            ++finish_task_count;
                            break;
                        case TryLockResult::Wait:
                            break;
                        default:
                            assert(false);
                    }
                    break;
                }
            case State::Done :
                break;
        }
        ++idx;
        if (idx == tasks_size) idx = 0;
    }

    std::vector<bool> commit(tasks_size,false);
    for (int i = 0;i < tasks_size; i++) {
        commit[i] = tasks[i].commit();
    }
    tasks.clear();
    states.clear();
    transactions.clear();

    return commit;
}

