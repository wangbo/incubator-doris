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

#pragma once

#include <mutex>
#include <thread>

#include "util/blocking_priority_queue.hpp"
#include "util/lock.h"
#include "util/thread_group.h"

namespace doris {

class SubQueue {
    friend class PriorityQueue;

public:

    void push_back(PriorityThreadPool::PrioTask task) { _queue.emplace(task); }

    bool try_take(PriorityThreadPool::PrioTask* task) {
        if (_queue.empty()) {
            return false;
        }
        *task = _queue.front();
        _queue.pop();
        return true;
    }

    void set_factor_for_normal(double factor_for_normal) { _factor_for_normal = factor_for_normal; }

    double get_vruntime() { return _real_runtime / _factor_for_normal; }

    void inc_real_runtime(uint64_t delta_time) {
        // _real_runtime.fetch_add(delta_time);
        _real_runtime += delta_time;
    }

    bool empty() { return _queue.empty(); }

public:
    std::queue<PriorityThreadPool::PrioTask> _queue;
    // factor for normalization
    double _factor_for_normal = 1;
    // the value cal the queue task time consume, the WorkTaskQueue
    // use it to find the min queue to take task work
    // std::atomic<uint64_t> _schedule_time = 0;
    std::atomic<uint64_t> _real_runtime = 0;
};

class PriorityQueue {
public:
    PriorityQueue() {
        std::cout << "begin init queue" << std::endl;
        _closed = false;
        double factor = 1;
        for (int i = SUB_QUEUE_LEVEL - 1; i >= 0; i--) {
            std::cout << "level=" << i << ", factor=" << factor << std::endl;
            _sub_queues[i].set_factor_for_normal(factor);
            factor *= LEVEL_QUEUE_TIME_FACTOR;
        }

        std::cout << std::endl;

        for (int i = 0; i < SUB_QUEUE_LEVEL - 1; i++) {
            std::cout << "level=" << i << ", limit=" << _task_schedule_limit[i] << std::endl;
        }

        std::cout << "end init queue" << std::endl;
        std::cout << std::endl;
    }

    // PriorityQueue(const PriorityQueue&) = delete;
    // PriorityQueue(PriorityQueue&&) = default;

    void close() {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        _closed = true;
        _wait_task.notify_all();
    }

    // actually not blocking,just poc
    bool blocking_put(PriorityThreadPool::PrioTask task) {
        int level = _compute_level(task.real_runtime);
        task.level = level;
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        _sub_queues[level].push_back(task);
        _total_task_size++;
        _wait_task.notify_one();
        return true;
    }

    bool non_blocking_get_with_lock(PriorityThreadPool::PrioTask* task) {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        return non_blocking_get(task);
    }

    bool non_blocking_get(PriorityThreadPool::PrioTask* task) {
        if (_total_task_size == 0 || _closed) {
            return false;
        }
        double normal_vruntime[SUB_QUEUE_LEVEL];
        double min_vruntime = 0;
        int idx = -1;
        for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
            normal_vruntime[i] = _sub_queues[i].get_vruntime();
            if (!_sub_queues[i].empty() &&
                (idx == -1 || normal_vruntime[i] < min_vruntime)) {
                idx = i;
                min_vruntime = normal_vruntime[i];
            }
        }
        DCHECK(idx != -1);
        // update empty queue's schedule time, to avoid too high priority
        for (int i = 0; i < SUB_QUEUE_LEVEL; ++i) {
            if (_sub_queues[i].empty() && normal_vruntime[i] < min_vruntime) {
                _sub_queues[i]._real_runtime = min_vruntime * _sub_queues[i]._factor_for_normal;
            }
        }

        bool ret = _sub_queues[idx].try_take(task);
        if (ret) {
            _total_task_size--;
            return true;
        } else {
            return false;
        }
    }

    bool blocking_get(PriorityThreadPool::PrioTask* task, uint32_t timeout_ms) {
        std::unique_lock<std::mutex> lock(_work_size_mutex);
        bool ret = non_blocking_get(task);
        if (!ret) {
            if (timeout_ms > 0) {
                _wait_task.wait_for(lock, std::chrono::milliseconds(timeout_ms));
            } else {
                _wait_task.wait(lock);
            }
            return non_blocking_get(task);
        } else {
            return true;
        }
    }

    // Get the each thread task size to do
    size_t size() const { return _total_task_size; }

public:
    static constexpr auto LEVEL_QUEUE_TIME_FACTOR = 2;
    static constexpr size_t SUB_QUEUE_LEVEL = 6;
    // 3, 6, 9, 12
    static constexpr uint32_t BASE_LIMIT = 3;
    SubQueue _sub_queues[SUB_QUEUE_LEVEL];
    uint64_t _task_schedule_limit[SUB_QUEUE_LEVEL - 1] = {1000000000, 3000000000, 10000000000,
                                                          60000000000, 300000000000};
    std::mutex _work_size_mutex;
    std::condition_variable _wait_task;
    std::atomic<size_t> _total_task_size = 0;
    bool _closed;

    int _compute_level(uint64_t real_runtime) {
        for (int i = 0; i < SUB_QUEUE_LEVEL - 1; ++i) {
            if (real_runtime <= _task_schedule_limit[i]) {
                return i;
            }
        }
        return SUB_QUEUE_LEVEL - 1;
    }
};

// Work-Stealing threadpool which processes items (of type T) in parallel which were placed on multi
// blocking queues by Offer(). Each item is processed by a single user-supplied method.
class PriorityWorkStealingThreadPool : public PriorityThreadPool {
public:
    // Creates a new thread pool and start num_threads threads.
    //  -- num_threads: how many threads are part of this pool
    //  -- num_queues: how many queues are part of this pool
    //  -- queue_size: the maximum size of the queue on which work items are offered. If the
    //     queue exceeds this size, subsequent calls to Offer will block until there is
    //     capacity available.
    PriorityWorkStealingThreadPool(uint32_t num_threads, uint32_t num_queues, uint32_t queue_size,
                                   const std::string& name)
            : PriorityThreadPool(0, 0, name) {
        DCHECK_GT(num_queues, 0);
        DCHECK_GE(num_threads, num_queues);
        // init _work_queues first because the work thread needs it
        // for (int i = 0; i < num_queues; ++i) {
        //     _work_queues.emplace_back(std::make_shared<BlockingPriorityQueue<Task>>(queue_size));
        // }
        for (int i = 0; i < num_queues; ++i) {
            _prio_work_queues.emplace_back(new PriorityQueue());
        }
        for (int i = 0; i < num_threads; ++i) {
            _threads.create_thread(std::bind<void>(
                    std::mem_fn(&PriorityWorkStealingThreadPool::work_thread), this, i));
        }
    }

    virtual ~PriorityWorkStealingThreadPool() {
        shutdown();
        join();
    }

    // Blocking operation that puts a work item on the queue. If the queue is full, blocks
    // until there is capacity available.
    //
    // 'work' is copied into the work queue, but may be referenced at any time in the
    // future. Therefore the caller needs to ensure that any data referenced by work (if T
    // is, e.g., a pointer type) remains valid until work has been processed, and it's up to
    // the caller to provide their own signalling mechanism to detect this (or to wait until
    // after DrainAndshutdown returns).
    //
    // Returns true if the work item was successfully added to the queue, false otherwise
    // (which typically means that the thread pool has already been shut down).
    bool offer(PriorityThreadPool::PrioTask task) override { return _prio_work_queues[task.queue_id]->blocking_put(task); }

    bool offer(WorkFunction func) override {
        PriorityThreadPool::Task task = {0, func, 0};
        return _work_queues[task.queue_id]->blocking_put(task);
    }

    // Shuts the thread pool down, causing the work queue to cease accepting offered work
    // and the worker threads to terminate once they have processed their current work item.
    // Returns once the shutdown flag has been set, does not wait for the threads to
    // terminate.
    void shutdown() override {
        PriorityThreadPool::shutdown();
        for (int i = 0; i < _prio_work_queues.size(); i++) {
            _prio_work_queues[i]->close();
        }
    }

    uint32_t get_queue_size() const override {
        uint32_t size = 0;
        for (int i = 0; i < _prio_work_queues.size(); i++) {
            size += _prio_work_queues[i]->size();
        }
        return size;
    }

    // Blocks until the work queue is empty, and then calls shutdown to stop the worker
    // threads and Join to wait until they are finished.
    // Any work Offer()'ed during DrainAndshutdown may or may not be processed.
    void drain_and_shutdown() override {
        {
            std::unique_lock l(_lock);
            while (get_queue_size() != 0) {
                _empty_cv.wait(l);
            }
        }
        shutdown();
        join();
    }

private:
    // Driver method for each thread in the pool. Continues to read work from the queue
    // until the pool is shutdown.
    void work_thread(int thread_id) {
        auto queue_id = thread_id % _prio_work_queues.size();
        auto steal_queue_id = (queue_id + 1) % _prio_work_queues.size();
        while (!is_shutdown()) {
            PriorityThreadPool::PrioTask task;
            // avoid blocking get
            bool is_other_queues_empty = true;
            // steal work in round-robin if nothing to do
            while (_prio_work_queues[queue_id]->size() == 0 && queue_id != steal_queue_id &&
                   !is_shutdown()) {
                if (_prio_work_queues[steal_queue_id]->non_blocking_get_with_lock(&task)) {
                    is_other_queues_empty = false;
                    MonotonicStopWatch task_real_time_watch;
                    task_real_time_watch.start();
                    task.work_function();
                    int64_t time_cost = task_real_time_watch.elapsed_time();
                    _prio_work_queues[steal_queue_id]->_sub_queues[task.level].inc_real_runtime(
                            time_cost);
                }
                steal_queue_id = (steal_queue_id + 1) % _prio_work_queues.size();
            }
            if (queue_id == steal_queue_id) {
                steal_queue_id = (steal_queue_id + 1) % _prio_work_queues.size();
            }
            if (is_other_queues_empty &&
                _prio_work_queues[queue_id]->blocking_get(
                        &task, config::doris_blocking_priority_queue_wait_timeout_ms)) {
                MonotonicStopWatch task_real_time_watch;
                task_real_time_watch.start();
                task.work_function();
                int64_t time_cost = task_real_time_watch.elapsed_time();
                _prio_work_queues[steal_queue_id]->_sub_queues[task.level].inc_real_runtime(
                        time_cost);
            }
            if (_prio_work_queues[queue_id]->size() == 0) {
                _empty_cv.notify_all();
            }
        }
    }

    // Queue on which work items are held until a thread is available to process them in
    // FIFO order.
    std::vector<std::shared_ptr<BlockingPriorityQueue<Task>>> _work_queues;
    // std::vector<std::shared_ptr<PriorityQueue>> _prio_work_queues;
    // std::vector<PriorityQueue> _prio_work_queues;
    std::vector<PriorityQueue*> _prio_work_queues;
};

} // namespace doris
