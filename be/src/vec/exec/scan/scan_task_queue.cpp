// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "scan_task_queue.h"

#include "pipeline/pipeline_task.h"
#include "runtime/task_group/task_group.h"
#include "vec/exec/scan/scanner_context.h"

namespace doris {
namespace taskgroup {
static void empty_function() {}
ScanTask::ScanTask() : ScanTask(empty_function, nullptr, nullptr, 1) {}

ScanTask::ScanTask(WorkFunction scan_func, vectorized::ScannerContext* scanner_context,
                   TGSTEntityPtr scan_entity, int priority)
        : scan_func(std::move(scan_func)),
          scanner_context(scanner_context),
          scan_entity(scan_entity),
          priority(priority) {}

ScanTaskQueue::ScanTaskQueue() : _queue(config::doris_scanner_thread_pool_queue_size) {}

Status ScanTaskQueue::try_push_back(ScanTask scan_task) {
    if (_queue.try_put(std::move(scan_task))) {
        VLOG_DEBUG << "try_push_back scan task " << scan_task.scanner_context->ctx_id << " "
                   << scan_task.priority;
        return Status::OK();
    } else {
        return Status::InternalError("failed to submit scan task to ScanTaskQueue");
    }
}
bool ScanTaskQueue::try_get(ScanTask* scan_task, uint32_t timeout_ms) {
    auto r = _queue.blocking_get(scan_task, timeout_ms);
    if (r) {
        VLOG_DEBUG << "try get scan task " << scan_task->scanner_context->ctx_id << " "
                   << scan_task->priority;
    }
    return r;
}

ScanTaskTaskGroupQueue::ScanTaskTaskGroupQueue(size_t core_size) : _core_size(core_size) {
    _empty_scan_task->scan_entity = _empty_group_entity;
    _empty_scan_task->is_empty_task = true;
    _empty_group_entity->set_empty_group_entity(true);

    Status st = ThreadPoolBuilder("ScanQueuePool")
            .set_min_threads(1)
            .set_max_threads(1)
            .set_max_queue_size(1)
            .build(&_thread_pool);
   st = _thread_pool->submit_func([this]() { this->print_group_info(); });
   if (!st.ok()) {
        std::cout << "init scan log pool failed" << std::endl;
   }
}

void ScanTaskTaskGroupQueue::print_group_info() {
    uint64_t last_user_cpu_time = 0;
    uint64_t last_user_take_count = 0;

    uint64_t last_empty_cpu_time = 0;
    uint64_t last_empty_take_count = 0;
    while (true) {
        {
            std::unique_lock<std::mutex> lock(_rs_mutex);
            uint64_t cur_user_cpu_time = 0;
            uint64_t cur_empty_cpu_time = 0;

            if (_group_entities.size() > 1) {
                for (auto* entity : _group_entities) {
                    if (!entity->is_empty_group_entity()) {
                        cur_user_cpu_time = entity->_real_runtime_ns;
                    } else {
                        cur_empty_cpu_time = entity->_real_runtime_ns;
                    }
                }
            } else {
                cur_user_cpu_time = _tmp_entity == nullptr ? 0 : _tmp_entity->_real_runtime_ns;
                cur_empty_cpu_time = _empty_group_entity->_real_runtime_ns;
            }

            uint64_t last_user_60s_cpu_time = cur_user_cpu_time - last_user_cpu_time;
            uint64_t last_empty_60s_cpu_time = cur_empty_cpu_time - last_empty_cpu_time;
            uint64_t fenmu = 1000000000;

            uint64_t last_user_60s_take_count = cur_user_take_count - last_user_take_count;
            uint64_t last_empty_60s_take_count = cur_empty_take_count - last_empty_take_count;

            LOG(INFO) << "group size= " << _group_entities.size()
                      << ", (scan)task queue last 30s, user_cpu_time=" << last_user_60s_cpu_time / fenmu
                      << ", empty_cpu_time=" << last_empty_60s_cpu_time / fenmu
                      << ", user_take_count=" << last_user_60s_take_count
                      << ", empty_take_count=" << last_empty_60s_take_count
                      << ", cur_empty_cpu_time=" << cur_empty_cpu_time / fenmu
                      << ", cur_user_cpu_time=" << cur_user_cpu_time / fenmu;

            last_user_cpu_time = cur_user_cpu_time;
            last_empty_cpu_time = cur_empty_cpu_time;

            last_user_take_count = cur_user_take_count;
            last_empty_take_count = cur_empty_take_count;
        }
        sleep(30);
    }
}

ScanTaskTaskGroupQueue::~ScanTaskTaskGroupQueue() {
    delete _empty_group_entity;
    delete _empty_scan_task;
}

void ScanTaskTaskGroupQueue::close() {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    _closed = true;
    _wait_task.notify_all();
}

bool ScanTaskTaskGroupQueue::take(ScanTask* scan_task) {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    taskgroup::TGSTEntityPtr entity = nullptr;
    while (entity == nullptr) {
        if (_closed) {
            return false;
        }
        if (_group_entities.empty()) {
            _wait_task.wait_for(lock, std::chrono::milliseconds(WAIT_CORE_TASK_TIMEOUT_MS * 5));
        } else {
            entity = _next_tg_entity();
            if (!entity) {
                _wait_task.wait_for(lock, std::chrono::milliseconds(WAIT_CORE_TASK_TIMEOUT_MS));
            }
        }
    }
    if (entity->is_empty_group_entity()) {
        *scan_task = *_empty_scan_task;
        cur_empty_take_count++;
        return true;
    }
    cur_user_take_count++;
    DCHECK(entity->task_size() > 0);
    if (entity->task_size() == 1) {
        _dequeue_task_group(entity);
        if (_enable_cpu_hard_limit) {
            reset_empty_group_entity();
        }
    }
    return entity->task_queue()->try_get(scan_task, WAIT_CORE_TASK_TIMEOUT_MS /* timeout_ms */);
}

bool ScanTaskTaskGroupQueue::push_back(ScanTask scan_task) {
    auto* entity = scan_task.scanner_context->get_task_group()->local_scan_task_entity();
    std::unique_lock<std::mutex> lock(_rs_mutex);
    auto status = entity->task_queue()->try_push_back(scan_task);
    if (!status.ok()) {
        LOG(WARNING) << "try_push_back scan task fail: " << status;
        return false;
    }
    if (_group_entities.find(entity) == _group_entities.end()) {
        _enqueue_task_group(entity);
        _tmp_entity = entity;
        if (_enable_cpu_hard_limit) {
            reset_empty_group_entity();
        }
    }
    _wait_task.notify_one();
    return true;
}

void ScanTaskTaskGroupQueue::update_statistics(ScanTask scan_task, int64_t time_spent) {
    auto* entity = scan_task.scan_entity;
    std::unique_lock<std::mutex> lock(_rs_mutex);
    auto find_entity = _group_entities.find(entity);
    bool is_in_queue = find_entity != _group_entities.end();
    VLOG_DEBUG << "scan task task group queue update_statistics " << entity->debug_string()
               << ", in queue:" << is_in_queue << ", time_spent: " << time_spent;
    if (is_in_queue) {
        _group_entities.erase(entity);
    }
    entity->incr_runtime_ns(time_spent);
    if (is_in_queue) {
        _group_entities.emplace(entity);
        _update_min_tg();
    }
}

void ScanTaskTaskGroupQueue::update_tg_cpu_share(const taskgroup::TaskGroupInfo& task_group_info,
                                                 taskgroup::TGSTEntityPtr entity) {
    std::unique_lock<std::mutex> lock(_rs_mutex);
    bool is_in_queue = _group_entities.find(entity) != _group_entities.end();
    if (is_in_queue) {
        _group_entities.erase(entity);
        _total_cpu_share -= entity->cpu_share();
    }
    entity->check_and_update_cpu_share(task_group_info);
    if (is_in_queue) {
        _group_entities.emplace(entity);
        _total_cpu_share += entity->cpu_share();
    }
}

void ScanTaskTaskGroupQueue::reset_empty_group_entity() {
    int user_g_cpu_hard_limit = 0;
    bool contains_empty_group = false;
    for (auto* entity : _group_entities) {
        if (!entity->is_empty_group_entity()) {
            user_g_cpu_hard_limit += entity->cpu_share();
        } else {
            contains_empty_group = true;
        }
    }

    // 0 <= user_g_cpu_hard_limit <= 100, bound by FE
    // user_g_cpu_hard_limit = 0 means no group exists
    int empty_group_cpu_share = 100 - user_g_cpu_hard_limit;
    if (empty_group_cpu_share > 0 && empty_group_cpu_share < 100 && !contains_empty_group) {
        _empty_group_entity->update_empty_cpu_share(empty_group_cpu_share);
        _enqueue_task_group(_empty_group_entity);
    } else if ((empty_group_cpu_share == 0 || empty_group_cpu_share == 100) &&
               contains_empty_group) {
        // no need to update empty group here
        // only update empty group's cpu share when exec enqueue
        _dequeue_task_group(_empty_group_entity);
    }
}

void ScanTaskTaskGroupQueue::_enqueue_task_group(TGSTEntityPtr tg_entity) {
    _total_cpu_share += tg_entity->cpu_share();
    // TODO llj tg If submitted back to this queue from the scanner thread, `adjust_vruntime_ns`
    // should be avoided.
    /**
     * If a task group entity leaves task queue for a long time, its v runtime will be very
     * small. This can cause it to preempt too many execution time. So, in order to avoid this
     * situation, it is necessary to adjust the task group's v runtime.
     * */
    auto old_v_ns = tg_entity->vruntime_ns();
    auto* min_entity = _min_tg_entity.load();
    if (min_entity) {
        auto min_tg_v = min_entity->vruntime_ns();
        auto ideal_r = _ideal_runtime_ns(tg_entity) / 2;
        uint64_t new_vruntime_ns = min_tg_v > ideal_r ? min_tg_v - ideal_r : min_tg_v;
        if (new_vruntime_ns > old_v_ns) {
            VLOG_DEBUG << tg_entity->debug_string() << ", adjust to new " << new_vruntime_ns;
            tg_entity->adjust_vruntime_ns(new_vruntime_ns);
        }
    } else if (old_v_ns < _min_tg_v_runtime_ns) {
        VLOG_DEBUG << tg_entity->debug_string() << ", adjust to " << _min_tg_v_runtime_ns;
        tg_entity->adjust_vruntime_ns(_min_tg_v_runtime_ns);
    }
    _group_entities.emplace(tg_entity);
    VLOG_DEBUG << "scan enqueue tg " << tg_entity->debug_string()
               << ", group entity size: " << _group_entities.size();
    _update_min_tg();
}

void ScanTaskTaskGroupQueue::_dequeue_task_group(TGSTEntityPtr tg_entity) {
    _total_cpu_share -= tg_entity->cpu_share();
    _group_entities.erase(tg_entity);
    VLOG_DEBUG << "scan task group queue dequeue tg " << tg_entity->debug_string()
               << ", group entity size: " << _group_entities.size();
    _update_min_tg();
}

TGSTEntityPtr ScanTaskTaskGroupQueue::_next_tg_entity() {
    taskgroup::TGSTEntityPtr res = nullptr;
    for (auto* entity : _group_entities) {
        res = entity;
        break;
    }
    return res;
}

uint64_t ScanTaskTaskGroupQueue::_ideal_runtime_ns(TGSTEntityPtr tg_entity) const {
    // Scan task does not have time slice, so we use pipeline task's instead.
    return pipeline::PipelineTask::THREAD_TIME_SLICE * _core_size * tg_entity->cpu_share() /
           _total_cpu_share;
}

void ScanTaskTaskGroupQueue::_update_min_tg() {
    auto* min_entity = _next_tg_entity();
    _min_tg_entity = min_entity;
    if (min_entity) {
        auto min_v_runtime = min_entity->vruntime_ns();
        if (min_v_runtime > _min_tg_v_runtime_ns) {
            _min_tg_v_runtime_ns = min_v_runtime;
        }
    }
}

bool ScanTaskTaskGroupQueue::TaskGroupSchedEntityComparator::operator()(
        const taskgroup::TGSTEntityPtr& lhs_ptr, const taskgroup::TGSTEntityPtr& rhs_ptr) const {
    auto lhs_val = lhs_ptr->vruntime_ns();
    auto rhs_val = rhs_ptr->vruntime_ns();
    if (lhs_val != rhs_val) {
        return lhs_val < rhs_val;
    } else {
        auto l_share = lhs_ptr->cpu_share();
        auto r_share = rhs_ptr->cpu_share();
        if (l_share != r_share) {
            return l_share < r_share;
        } else {
            return lhs_ptr->task_group_id() < rhs_ptr->task_group_id();
        }
    }
}

} // namespace taskgroup
} // namespace doris