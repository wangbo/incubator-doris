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

#include "task_queue.h"

namespace doris {
namespace pipeline {

PipelineTask* SubWorkTaskQueue::try_take(bool is_steal) {
    if (_queue.empty()) {
        return nullptr;
    }
    auto task = _queue.front();
    if (!task->can_steal() && is_steal) {
        return nullptr;
    }
    ++_schedule_time;
    _queue.pop();
    return task;
}

////////////////////  WorkTaskQueue ////////////////////

WorkTaskQueue::WorkTaskQueue() : _closed(false) {
    std::cout << "begin init" << std::endl;
    double factor = 1;
    for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
        _sub_queues[SUB_QUEUE_LEVEL - i - 1].set_factor_for_normal(factor);
        factor *= LEVEL_QUEUE_TIME_FACTOR;
        std::cout << "level=" << (SUB_QUEUE_LEVEL - i - 1) << ",factor=" << factor << std::endl;
    }

    std::cout << std::endl;

    _task_vruntime_level_limit[0] = BASE_TIME_SLICE_NS;
    std::cout << "level=" << 0 << ", vruntime=" << _task_vruntime_level_limit[0] << std::endl;
    for (int i = 1; i < SUB_QUEUE_LEVEL; i++) {
        _task_vruntime_level_limit[i] = _task_vruntime_level_limit[i - 1] + BASE_TIME_SLICE_NS * i;
        std::cout << "level=" << i << ", vruntime=" << _task_vruntime_level_limit[i] << std::endl;
    }
    std::cout << "end init" << std::endl;
    std::cout << std::endl;
}

void WorkTaskQueue::close() {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    _closed = true;
    _wait_task.notify_all();
}

PipelineTask* WorkTaskQueue::try_take_unprotected(bool is_steal) {
    if (_total_task_size == 0 || _closed) {
        return nullptr;
    }
    double normal_vruntime_array[SUB_QUEUE_LEVEL];
    double min_vruntime = -1;
    int min_vruntime_queue_idx = -1;
    for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
        normal_vruntime_array[i] = _sub_queues[i].get_total_vruntime_after_normal();
        if (!_sub_queues[i].empty() && (min_vruntime == -1 || normal_vruntime_array[i] < min_vruntime)) {
            min_vruntime_queue_idx = i;
            min_vruntime = normal_vruntime_array[i];
        }
    }
    DCHECK(min_vruntime_queue_idx != -1);
    // todo(wb) rethinking is it appropriate to update empty queue's schedule time here
    // or we can update it when queue is not empty
    for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
        if (_sub_queues[i].empty() && normal_vruntime_array[i] < min_vruntime) {
            _sub_queues[i].adjust_queue_vruntime_to_min(min_vruntime);
        }
    }

    auto task = _sub_queues[min_vruntime_queue_idx].try_take(is_steal);
    if (task) {
        _total_task_size--;
    }
    return task;
}

int WorkTaskQueue::_compute_level(PipelineTask* task) {
    uint64_t task_vruntime = task->get_task_vruntime();
    for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
        if (task_vruntime <= _task_vruntime_level_limit[i]) {
            return i;
        }
    }
    return SUB_QUEUE_LEVEL - 1;
}

PipelineTask* WorkTaskQueue::try_take(bool is_steal) {
    // TODO other efficient lock? e.g. if get lock fail, return null_ptr
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    return try_take_unprotected(is_steal);
}

PipelineTask* WorkTaskQueue::take(uint32_t timeout_ms) {
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    auto task = try_take_unprotected(false);
    if (task) {
        return task;
    } else {
        if (timeout_ms > 0) {
            _wait_task.wait_for(lock, std::chrono::milliseconds(timeout_ms));
        } else {
            _wait_task.wait(lock);
        }
        return try_take_unprotected(false);
    }
}

Status WorkTaskQueue::push(PipelineTask* task) {
    if (_closed) {
        return Status::InternalError("WorkTaskQueue closed");
    }
    auto level = _compute_level(task);
    std::unique_lock<std::mutex> lock(_work_size_mutex);
    _sub_queues[level].push_back(task);
    _sub_queues[level].inc_total_vruntime(task->get_last_sched_vruntime());
    _total_task_size++;
    _wait_task.notify_one();
    return Status::OK();
}

////////////////// TaskQueue ////////////

void TaskQueue::close() {
    _closed = true;
    for (int i = 0; i < _core_size; ++i) {
        _async_queue[i].close();
    }
}

PipelineTask* TaskQueue::try_take(size_t core_id) {
    PipelineTask* task;
    while (!_closed) {
        task = _async_queue[core_id].try_take(false);
        if (task) {
            break;
        }
        task = steal_take(core_id);
        if (task) {
            break;
        }
        task = _async_queue[core_id].take(WAIT_CORE_TASK_TIMEOUT_MS /* timeout_ms */);
        if (task) {
            break;
        }
    }
    if (task) {
        task->pop_out_runnable_queue();
    }
    return task;
}

PipelineTask* TaskQueue::steal_take(size_t core_id) {
    DCHECK(core_id < _core_size);
    size_t next_id = core_id;
    for (size_t i = 1; i < _core_size; ++i) {
        ++next_id;
        if (next_id == _core_size) {
            next_id = 0;
        }
        DCHECK(next_id < _core_size);
        auto task = _async_queue[next_id].try_take(true);
        if (task) {
            return task;
        }
    }
    return nullptr;
}

Status TaskQueue::push_back(PipelineTask* task) {
    int core_id = task->get_previous_core_id();
    if (core_id < 0) {
        core_id = _next_core.fetch_add(1) % _core_size;
    }
    return push_back(task, core_id);
}

Status TaskQueue::push_back(PipelineTask* task, size_t core_id) {
    DCHECK(core_id < _core_size);
    task->put_in_runnable_queue();
    return _async_queue[core_id].push(task);
}

} // namespace pipeline
} // namespace doris