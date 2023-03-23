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

#include "scanner_context.h"

namespace doris {

namespace pipeline {

class PipScannerContext : public vectorized::ScannerContext {
public:
    PipScannerContext(RuntimeState* state, vectorized::VScanNode* parent,
                      const TupleDescriptor* input_tuple_desc,
                      const TupleDescriptor* output_tuple_desc,
                      const std::list<vectorized::VScanner*>& scanners, int64_t limit,
                      int64_t max_bytes_in_blocks_queue)
            : vectorized::ScannerContext(state, parent, input_tuple_desc, output_tuple_desc,
                                         scanners, limit, max_bytes_in_blocks_queue) {}

    Status get_block_from_queue(RuntimeState* state, vectorized::BlockUPtr* block, bool* eos,
                                int id, bool wait = false) override {
        {
            std::unique_lock<std::mutex> l(_transfer_lock);
            if (state->is_cancelled()) {
                _process_status = Status::Cancelled("cancelled");
            }

            if (!_process_status.ok()) {
                return _process_status;
            }
        }

        {
            std::unique_lock<std::mutex> l(*_queue_mutexs[id]);
            if (!_blocks_queues[id].empty()) {
                *block = std::move(_blocks_queues[id].front());
                _current_used_bytes -= (*block)->allocated_bytes();
                _blocks_queues[id].pop_front();
                return Status::OK();
            } else {
                *eos = _is_finished || _should_stop;
            }
        }
        return Status::OK();
    }

    // We should make those method lock free.
    bool done() override { return _is_finished || _should_stop || _status_error; }

    void append_blocks_to_queue(std::vector<vectorized::BlockUPtr>& blocks) override {
        const int queue_size = _queue_mutexs.size();
        const int block_size = blocks.size();
        int64_t local_bytes = 0;
        for (int i = 0; i < queue_size && i < block_size; ++i) {
            int queue = _next_queue_to_feed;
            {
                std::unique_lock<std::mutex> l(*_queue_mutexs[queue]);
                for (int j = i; j < block_size; j += queue_size) {
                    local_bytes += blocks[j]->allocated_bytes();
                    _blocks_queues[queue].emplace_back(std::move(blocks[j]));
                }
            }
            _next_queue_to_feed = queue + 1 < queue_size ? queue + 1 : 0;
        }
        _current_used_bytes += local_bytes;
    }

    bool empty_in_queue(int id) override {
        std::unique_lock<std::mutex> l(*_queue_mutexs[id]);
        return _blocks_queues[id].empty();
    }

    void set_max_queue_size(int max_queue_size) override {
        for (int i = 0; i < max_queue_size; ++i) {
            _blocks_queue_empty.emplace_back(true);
            _queue_mutexs.emplace_back(new std::mutex);
            _blocks_queues.emplace_back(std::list<vectorized::BlockUPtr>());
        }
    }

    bool has_enough_space_in_blocks_queue() const override {
        return _current_used_bytes < _max_bytes_in_queue / 2;
    }

    // note(wb) only used by ScannerScheduler::_schedule_scanners
    void reschedule_if_necessary() override {
        std::unique_lock l(_transfer_lock);
        // 1 update _num_scheduling_ctx
        if (_num_running_scanners == 0 && _num_scheduling_ctx == 1) {
            auto submit_st = _scanner_scheduler->submit(this);
            if (!submit_st.ok()) {
                _num_scheduling_ctx--;
                set_status_on_error(
                        Status::InternalError("failed to submit scanner_ctx to scanner_ctx pool"));

            }
        } else {
            _num_scheduling_ctx--;
        }

        _blocks_queue_added_cv.notify_one();
        _ctx_finish_cv.notify_one();
    }

private:
    int _next_queue_to_feed = 0;
    std::vector<bool> _blocks_queue_empty;
    std::vector<std::unique_ptr<std::mutex>> _queue_mutexs;
    std::vector<std::list<vectorized::BlockUPtr>> _blocks_queues;
    std::atomic_int64_t _current_used_bytes = 0;
};
} // namespace pipeline
} // namespace doris
