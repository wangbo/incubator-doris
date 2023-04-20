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

class SharedQueue {
public:
    void init_share_queue(int parallel) {
        for (int i = 0; i < parallel; ++i) {
            _shared_queue_mutexs.emplace_back(new std::mutex);
            _shared_blocks_queues.emplace_back(std::list<vectorized::BlockUPtr>());
        }
        _shared_max_queue_size = parallel;
    }

    // consider destroy
    std::vector<std::unique_ptr<std::mutex>> _shared_queue_mutexs;
    std::vector<std::list<vectorized::BlockUPtr>> _shared_blocks_queues;
    std::atomic_int64_t _shared_current_used_bytes = 0;
    std::atomic_int32_t _shared_next_queue_to_feed = 0;
    int _shared_max_queue_size = 1;
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

    SharedQueue* get_shared_queue(int par) {
        std::lock_guard<std::mutex> lock(_mutex);
        if (!is_queue_inited) {
            shared_queue.init_share_queue(par);
            parallel = par;
            is_queue_inited = true;
        }
        return &shared_queue;
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

    void get_scanners(std::list<VScanner*>* scanner_list, int id) {
        if (_node_scanners.find(id) == _node_scanners.end()) {
            std::cout << "unexpected error" << std::endl;
        }
        if (_node_scanners[id].singe_node_scanner_num == -1) {
            int local = _node_scanners[id].scanners.size() / parallel;
            // is there any case an instance can not get a scanners?
            // if so, it needn't submit scanctx, just get block
            local = local == 0 ? 1 : local;
            _node_scanners[id].singe_node_scanner_num = local;
            // std::cout << "singe_node_scanner_num=" << singe_node_scanner_num << std::endl;
        }
        if (_node_scanners[id].scanners.size() > 0) {
            for (int i = 0; i < _node_scanners[id].singe_node_scanner_num; i++) {
                scanner_list->push_back(_node_scanners[id].scanners.front());
                _node_scanners[id].scanners.pop_front();
            }
        }
    }

    void insert_sc_ctx(int id, std::list<VScanner*>* scanners) {
        sc_ctx sc;
        sc.id = id;
        sc.scanners = std::move(*scanners);
        _node_scanners.insert({id, sc});
    }

    struct sc_ctx {
        std::list<VScanner*> scanners;
        int id;
        int singe_node_scanner_num = -1;
    };

    SharedQueue shared_queue;
    // std::list<VScanner*> scanners;
    ObjectPool _shared_scanner_pool;
    std::map<int /*node id*/, sc_ctx> _node_scanners;

public:
    int singe_node_scanner_num = -1;
    int parallel = -1;
    bool is_queue_inited = false;
    bool has_create_scaners = false;
    std::mutex _mutex;
    std::map<int /*node id*/, int /*parallel*/> _scanner_parallel;
    std::map<int /*node id*/, std::shared_ptr<ScannerContext>> _scanner_context;
};

} // namespace doris::vectorized