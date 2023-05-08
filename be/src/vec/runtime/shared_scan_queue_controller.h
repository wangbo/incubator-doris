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

#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <vector>

#include "vec/core/block.h"

namespace doris::vectorized {

class SharedQueueContext{

friend class SharedScanQueueController;

public:
    bool is_finished() {
        return _is_finish == 1;
    }

    void inc_finish_parallel_num() {
        std::unique_lock<std::mutex> l(_mutex);
        _finish_parallel++;
        DCHECK(_finish_parallel <= _total_parallel);
        // std::cout << "inc_finish_parallel_num, _finish_parallel=" << _finish_parallel << ", total=" << _total_parallel << std::endl;
        if (_finish_parallel == _total_parallel) {
            _is_finish = 1;
        }
    }

    void set_total_parallel(int total_paralled) {
        _total_parallel = total_paralled;
    }

    // evenly append to each queue
    void push_blocks(std::vector<vectorized::BlockUPtr>* blocks_ptr) {
        auto& blocks = *blocks_ptr;
        const int queue_size = _queue_mutexs.size();
        const int block_size = blocks.size();
        int64_t local_bytes = 0;

        int queue = _next_queue_to_feed.fetch_add(1);
        queue = queue % queue_size;
        for (int i = 0; i < queue_size && i < block_size; ++i) {
            {
                std::lock_guard<std::mutex> l(*_queue_mutexs[queue]);
                for (int j = i; j < block_size; j += queue_size) {
                    local_bytes += blocks[j]->allocated_bytes();
                    _blocks_queues[queue].emplace_back(std::move(blocks[j]));
                }
            }
            queue = queue + 1 < queue_size ? queue + 1 : 0;
        }
        _current_used_bytes += local_bytes;
    }

    // append to a specified queue
    void push_block_with_queue_id(vectorized::BlockUPtr blocks_ref, int queue_id) {
        std::lock_guard<std::mutex> l(*_queue_mutexs[queue_id]);
        _current_used_bytes += blocks_ref->allocated_bytes();
        _blocks_queues[queue_id].emplace_back(std::move(blocks_ref));
    }

    void get_blocks(int id, vectorized::BlockUPtr* block, bool* get_block) {
        std::unique_lock<std::mutex> l(*_queue_mutexs[id]);
        if (!_blocks_queues[id].empty()) {
            *block = std::move(_blocks_queues[id].front());
            _blocks_queues[id].pop_front();
            _current_used_bytes -= (*block)->allocated_bytes();
            *get_block = true;
        } else {
            *get_block = false;
        }
    }

    bool empty_in_queue(int queue_id) {
        std::unique_lock<std::mutex> l(*_queue_mutexs[queue_id]);
        return _blocks_queues[queue_id].empty();
    }

    bool has_enough_space_in_blocks_queue() { return _current_used_bytes < (_max_bytes_limit / 2); }

    void set_max_bytes_limit(int64_t max_bytes_limit) {
        this->_max_bytes_limit = max_bytes_limit;
    }

private:
    std::atomic_int32_t _next_queue_to_feed = 0;
    std::vector<std::unique_ptr<std::mutex>> _queue_mutexs;
    std::vector<std::list<vectorized::BlockUPtr>> _blocks_queues;
    std::atomic_int64_t _current_used_bytes = 0;
    int64_t _max_bytes_limit = 0;

    std::mutex _mutex;
    // 0 should not finish; 1 should finish
    // when all scanner finished,then _is_finish == 1
    std::atomic_int32_t _is_finish = 0; 
    int _total_parallel = 0;
    int _finish_parallel = 0;
};

class SharedScanQueueController {

public:
    SharedQueueContext* get_scan_queue_ctx(int node_id, int& parallel, int64_t query_mem_limit) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _node_queue_map.find(node_id);
        auto it_2 = _node_id_real_paral_map.find(node_id);
        if (it_2 == _node_id_real_paral_map.cend()) {
            std::cout << "meets unknown error" << std::endl;
            return nullptr;
        }
        int real_parallel = it_2->second;
        parallel = real_parallel;

        if (it == _node_queue_map.cend()) {
            std::shared_ptr<SharedQueueContext> shared_queue_ctx =
                    std::make_shared<SharedQueueContext>();
            for (int i = 0; i < parallel; i++) {
                shared_queue_ctx->_queue_mutexs.emplace_back(new std::mutex);
                shared_queue_ctx->_blocks_queues.emplace_back(std::list<vectorized::BlockUPtr>());
            }

            shared_queue_ctx->set_max_bytes_limit(query_mem_limit * parallel);
            shared_queue_ctx->set_total_parallel(parallel);
            SharedQueueContext* ret = shared_queue_ctx.get();
            // std::cout << "parallel=" << parallel << ", max mem limit=" << (query_mem_limit * parallel) << std::endl;
            _node_queue_map.insert({node_id, std::move(shared_queue_ctx)});
            return ret;
        } else {
            auto* ret = it->second.get();
            return ret;
        }
    }

    bool shared_queue_context_is_ready(int node_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        return _node_queue_map.find(node_id) != _node_queue_map.end();
    }

    int get_shared_scan_queue_id(int node_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _node_id_queue_id_map.find(node_id);
        if (it == _node_id_queue_id_map.cend()) {
            _node_id_queue_id_map.insert({node_id, 0});
            return 0;
        } else {
            auto queue_id = it->second + 1;
            _node_id_queue_id_map[node_id] = queue_id;
            return queue_id;
        }
    }

    void insert_real_parallel(int node_id, int parallel) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (_node_id_real_paral_map.find(node_id) == _node_id_real_paral_map.cend()) {
            _node_id_real_paral_map.insert({node_id, parallel});
        }
    }

    std::mutex _mutex;
    std::map<int, std::shared_ptr<SharedQueueContext>> _node_queue_map;
    std::map<int /*node id*/, int /*parallel*/> _node_id_queue_id_map;

    std::map<int /*node id*/, int /*parallel*/> _node_id_real_paral_map;

    std::unique_ptr<SharedQueueContext> _shared_queue_ctx = std::make_unique<SharedQueueContext>();
};

}