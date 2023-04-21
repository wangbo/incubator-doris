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
#include "vec/exec/scan/scanner_context.h"

namespace doris::vectorized {

class ScannerContext;

class SharedScannerQueue {
public:
    class SharedQueueContext {
    public:
        std::atomic_int32_t _next_queue_to_feed = 0;
        std::vector<std::unique_ptr<std::mutex>> _queue_mutexs;
        std::vector<std::list<vectorized::BlockUPtr>> _blocks_queues;
        std::atomic_int64_t _current_used_bytes = 0;
        int max_bytes_limit = 0;
        
        std::mutex _mutex;
        // control end condition
        std::atomic_int32_t is_finish = 0; // 0 should finish; 1 should finish
        int regist_client_num = 0;
        int total_client_num = 0;
        int finish_client_num = 0;

        bool is_all_scan_finish() {
            return is_finish == 1;
        }

        void update_finish_client() {
            std::unique_lock<std::mutex> l(_mutex);
            finish_client_num++;
            if (finish_client_num > total_client_num) {
                std::cout << "unexpected error" << std::endl;
            }
            if (regist_client_num == total_client_num && finish_client_num == total_client_num) {
                is_finish = 1;
            }
        }

        void regist_client(int total_client) {
            std::unique_lock<std::mutex> l(_mutex);
            total_client_num = total_client;
            regist_client_num++;
        }

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

        bool empty_in_queue(int id) {
            std::unique_lock<std::mutex> l(*_queue_mutexs[id]);
            return _blocks_queues[id].empty();
        }

        bool has_enough_space_in_blocks_queue() {
            return _current_used_bytes <
                   max_bytes_limit; // need max_bytes_limit = single * instance num
        }
    };

    // need max_bytes_limit = single * instance num
    SharedQueueContext* get_queue_context(int node_id, int parallel, int max_bytes_limit) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _node_queue_map.find(node_id);

        if (it == _node_queue_map.cend()) {
            std::shared_ptr<SharedQueueContext> shared_queue_ctx =
                    std::make_shared<SharedQueueContext>();
            for (int i = 0; i < parallel; i++) {
                shared_queue_ctx->_queue_mutexs.emplace_back(new std::mutex);
                shared_queue_ctx->_blocks_queues.emplace_back(std::list<vectorized::BlockUPtr>());
            }
            shared_queue_ctx->max_bytes_limit = max_bytes_limit;

            shared_queue_ctx->regist_client(parallel);

            SharedQueueContext* ret = shared_queue_ctx.get();
            _node_queue_map.insert({node_id, std::move(shared_queue_ctx)});
            return ret;
        } else {
            auto* ret = it->second.get();
            ret->regist_client(parallel);
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
            auto queue_id = it->second;
            _node_id_queue_id_map[node_id] = queue_id + 1;
            return queue_id + 1;
        }
    }

    std::mutex _mutex;
    std::map<int, std::shared_ptr<SharedQueueContext>> _node_queue_map;
    std::map<int /*node id*/, int /*parallel*/> _node_id_queue_id_map;
};

class SharedScannerController {
public:
    std::pair<bool, int> should_build_scanner_and_queue_id(int my_node_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        auto it = _scanner_parallel.find(my_node_id);

        if (it == _scanner_parallel.cend()) {
            _scanner_parallel.insert({my_node_id, 0});
            return {true, 0};
        } else {
            auto queue_id = it->second;
            _scanner_parallel[my_node_id] = queue_id + 1;
            return {false, queue_id + 1};
        }
    }

    void set_scanner_context(int my_node_id,
                             const std::shared_ptr<ScannerContext> scanner_context) {
        std::lock_guard<std::mutex> lock(_mutex);
        _scanner_context.insert({my_node_id, scanner_context});
    }

    bool scanner_context_is_ready(int my_node_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        return _scanner_context.find(my_node_id) != _scanner_context.end();
    }

    std::shared_ptr<ScannerContext> get_scanner_context(int my_node_id) {
        std::lock_guard<std::mutex> lock(_mutex);
        return _scanner_context[my_node_id];
    }

    std::unique_ptr<SharedScannerQueue> shared_queue = std::make_unique<SharedScannerQueue>();

private:
    std::mutex _mutex;
    std::map<int /*node id*/, int /*parallel*/> _scanner_parallel;
    std::map<int /*node id*/, std::shared_ptr<ScannerContext>> _scanner_context;
};

} // namespace doris::vectorized