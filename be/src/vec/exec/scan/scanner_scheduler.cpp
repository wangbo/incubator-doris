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

#include "scanner_scheduler.h"

#include "common/config.h"
#include "util/async_io.h"
#include "util/priority_thread_pool.hpp"
#include "util/priority_work_stealing_thread_pool.hpp"
#include "util/telemetry/telemetry.h"
#include "util/thread.h"
#include "util/threadpool.h"
#include "vec/core/block.h"
#include "vec/exec/scan/new_olap_scanner.h"
#include "vec/exec/scan/vscanner.h"
#include "vec/exprs/vexpr.h"
#include "vfile_scanner.h"

namespace doris::vectorized {

ScannerScheduler::ScannerScheduler() {}

ScannerScheduler::~ScannerScheduler() {
    if (!_is_init) {
        return;
    }

    for (int i = 0; i < QUEUE_NUM; i++) {
        _pending_queues[i]->shutdown();
    }

    _is_closed = true;

    _scheduler_pool->shutdown();
    _local_scan_thread_pool->shutdown();
    _remote_scan_thread_pool->shutdown();
    _limited_scan_thread_pool->shutdown();

    _scheduler_pool->wait();
    _local_scan_thread_pool->join();

    for (int i = 0; i < QUEUE_NUM; i++) {
        delete _pending_queues[i];
    }
    delete[] _pending_queues;
}

Status ScannerScheduler::init(ExecEnv* env) {
    // 1. scheduling thread pool and scheduling queues
    ThreadPoolBuilder("SchedulingThreadPool")
            .set_min_threads(QUEUE_NUM)
            .set_max_threads(QUEUE_NUM)
            .build(&_scheduler_pool);

    _pending_queues = new BlockingQueue<ScannerContext*>*[QUEUE_NUM];
    for (int i = 0; i < QUEUE_NUM; i++) {
        _pending_queues[i] = new BlockingQueue<ScannerContext*>(INT32_MAX);
        _scheduler_pool->submit_func([this, i] { this->_schedule_thread(i); });
    }

    // 2. local scan thread pool
    _local_scan_thread_pool.reset(new PriorityWorkStealingThreadPool(
            config::doris_scanner_thread_pool_thread_num, env->store_paths().size(),
            config::doris_scanner_thread_pool_queue_size, "local_scan"));

    // 3. remote scan thread pool
    ThreadPoolBuilder("RemoteScanThreadPool")
            .set_min_threads(config::doris_scanner_thread_pool_thread_num)            // 48 default
            .set_max_threads(config::doris_max_remote_scanner_thread_pool_thread_num) // 512 default
            .set_max_queue_size(config::doris_scanner_thread_pool_queue_size)
            .build(&_remote_scan_thread_pool);

    // 4. limited scan thread pool
    ThreadPoolBuilder("LimitedScanThreadPool")
            .set_min_threads(config::doris_scanner_thread_pool_thread_num)
            .set_max_threads(config::doris_scanner_thread_pool_thread_num)
            .set_max_queue_size(config::doris_scanner_thread_pool_queue_size)
            .build(&_limited_scan_thread_pool);

    _is_init = true;
    return Status::OK();
}

Status ScannerScheduler::submit(ScannerContext* ctx) {
    if (ctx->queue_idx == -1) {
        ctx->queue_idx = (_queue_idx++ % QUEUE_NUM);
    }
    if (!_pending_queues[ctx->queue_idx]->blocking_put(ctx)) {
        return Status::InternalError("failed to submit scanner context to scheduler");
    }
    return Status::OK();
}

std::unique_ptr<ThreadPoolToken> ScannerScheduler::new_limited_scan_pool_token(
        ThreadPool::ExecutionMode mode, int max_concurrency) {
    return _limited_scan_thread_pool->new_token(mode, max_concurrency);
}

void ScannerScheduler::_schedule_thread(int queue_id) {
    BlockingQueue<ScannerContext*>* queue = _pending_queues[queue_id];
    while (!_is_closed) {
        ScannerContext* ctx;
        bool ok = queue->blocking_get(&ctx);
        if (!ok) {
            // maybe closed
            continue;
        }

        _schedule_scanners(ctx);
        // If ctx is done, no need to schedule it again.
        // But should notice that there may still scanners running in scanner pool.
    }
    return;
}

[[maybe_unused]] static void* run_scanner_bthread(void* arg) {
    auto f = reinterpret_cast<std::function<void()>*>(arg);
    (*f)();
    delete f;
    return nullptr;
}

int ScannerScheduler::_calculate_priority(int sche_times) {
    if (sche_times < 62) {
        return 10;
    } else if (sche_times < 310) {
        return 9;
    } else if (sche_times < 620) {
        return 8;
    } else if (sche_times < 1240) {
        return 7;
    } else if (sche_times < 1860) {
        return 6;
    } else if (sche_times < 3100) {
        return 5;
    } else if (sche_times < 4650) {
        return 4;
    } else if (sche_times < 6200) {
        return 3;
    } else if (sche_times < 31000) {
        return 2;
    } else if (sche_times < 62000) {
        return 1;
    } else {
        return 0;
    }
}

void ScannerScheduler::_schedule_scanners(ScannerContext* ctx) {
    ctx->incr_num_ctx_scheduling(1);
    if (ctx->done()) {
        ctx->update_num_running(0, -1);
        return;
    }

    std::list<VScanner*> this_run;
    ctx->get_next_batch_of_scanners(&this_run);
    if (this_run.empty()) {
        // There will be 2 cases when this_run is empty:
        // 1. The blocks queue reaches limit.
        //      The consumer will continue scheduling the ctx.
        // 2. All scanners are running.
        //      There running scanner will schedule the ctx after they are finished.
        // So here we just return to stop scheduling ctx.
        ctx->update_num_running(0, -1);
        return;
    }

    ctx->update_num_running(this_run.size(), -1);
    // Submit scanners to thread pool
    auto iter = this_run.begin();
    auto submit_to_thread_pool = [&] {
        ctx->incr_num_scanner_scheduling(this_run.size());
        if (ctx->thread_token != nullptr) {
            while (iter != this_run.end()) {
                (*iter)->start_wait_worker_timer();
                auto s = ctx->thread_token->submit_func(
                        [this, scanner = *iter, ctx] { this->_scanner_scan(this, ctx, scanner); });
                if (s.ok()) {
                    this_run.erase(iter++);
                } else {
                    ctx->set_status_on_error(s);
                    break;
                }
            }
        } else {
            while (iter != this_run.end()) {
                (*iter)->start_wait_worker_timer();
                TabletStorageType type = (*iter)->get_storage_type();
                bool ret = false;
                if (type == TabletStorageType::STORAGE_TYPE_LOCAL) {
                    PriorityThreadPool::Task task;
                    task.work_function = [this, scanner = *iter, ctx] {
                        this->_scanner_scan(this, ctx, scanner);
                    };
                    int sche_times = ctx->fetch_add_sche_times();
                    task.priority = _calculate_priority(sche_times);
                    task.queue_id = (*iter)->queue_id();
                    ret = _local_scan_thread_pool->offer(task);
                } else {
                    ret = _remote_scan_thread_pool->submit_func([this, scanner = *iter, ctx] {
                        this->_scanner_scan(this, ctx, scanner);
                    });
                }
                if (ret) {
                    this_run.erase(iter++);
                } else {
                    ctx->set_status_on_error(
                            Status::InternalError("failed to submit scanner to scanner pool"));
                    break;
                }
            }
        }
    };
#if !defined(USE_BTHREAD_SCANNER)
    submit_to_thread_pool();
#else
    // Only OlapScanner uses bthread scanner
    // Todo: Make other scanners support bthread scanner
    if (dynamic_cast<NewOlapScanner*>(*iter) == nullptr) {
        return submit_to_thread_pool();
    }
    ctx->incr_num_scanner_scheduling(this_run.size());
    while (iter != this_run.end()) {
        (*iter)->start_wait_worker_timer();
        AsyncIOCtx io_ctx {.nice = nice};

        auto f = new std::function<void()>([this, scanner = *iter, ctx, io_ctx] {
            AsyncIOCtx* set_io_ctx =
                    static_cast<AsyncIOCtx*>(bthread_getspecific(AsyncIO::btls_io_ctx_key));
            if (set_io_ctx == nullptr) {
                set_io_ctx = new AsyncIOCtx(io_ctx);
                CHECK_EQ(0, bthread_setspecific(AsyncIO::btls_io_ctx_key, set_io_ctx));
            } else {
                LOG(WARNING) << "New bthread should not have io_nice_key";
            }
            this->_scanner_scan(this, ctx, scanner);
        });
        bthread_t btid;
        int ret = bthread_start_background(&btid, nullptr, run_scanner_bthread, (void*)f);

        if (ret == 0) {
            this_run.erase(iter++);
            ctx->_btids.push_back(btid);
        } else {
            delete f;
            LOG(FATAL) << "failed to submit scanner to bthread";
            ctx->set_status_on_error(Status::InternalError("failed to submit scanner to bthread"));
            break;
        }
    }
#endif
}

void ScannerScheduler::_scanner_scan(ScannerScheduler* scheduler, ScannerContext* ctx,
                                     VScanner* scanner) {
    auto tracker_config = [&] {
        SCOPED_ATTACH_TASK(scanner->runtime_state());
        Thread::set_self_name("_scanner_scan");
    };
#if !defined(USE_BTHREAD_SCANNER)
    tracker_config();
#else
    if (dynamic_cast<NewOlapScanner*>(scanner) == nullptr) {
        tracker_config();
    }
#endif
    scanner->update_wait_worker_timer();
    scanner->start_scan_cpu_timer();
    Status status = Status::OK();
    bool eos = false;
    RuntimeState* state = ctx->state();
    DCHECK(nullptr != state);
    if (!scanner->is_open()) {
        status = scanner->open(state);
        if (!status.ok()) {
            ctx->set_status_on_error(status);
            eos = true;
        }
        scanner->set_opened();
    }

    scanner->try_append_late_arrival_runtime_filter();

    // Because we use thread pool to scan data from storage. One scanner can't
    // use this thread too long, this can starve other query's scanner. So, we
    // need yield this thread when we do enough work. However, OlapStorage read
    // data in pre-aggregate mode, then we can't use storage returned data to
    // judge if we need to yield. So we record all raw data read in this round
    // scan, if this exceeds row number or bytes threshold, we yield this thread.
    std::vector<vectorized::BlockUPtr> blocks;
    int64_t raw_rows_read = scanner->get_rows_read();
    int64_t raw_rows_threshold = raw_rows_read + config::doris_scanner_row_num;
    int64_t raw_bytes_read = 0;
    int64_t raw_bytes_threshold = config::doris_scanner_row_bytes;
    bool has_free_block = true;
    int num_rows_in_block = 0;

    // Only set to true when ctx->done() return true.
    // Use this flag because we need distinguish eos from `should_stop`.
    // If eos is true, we still need to return blocks,
    // but is should_stop is true, no need to return blocks
    bool should_stop = false;
    // Has to wait at least one full block, or it will cause a lot of schedule task in priority
    // queue, it will affect query latency and query concurrency for example ssb 3.3.
    while (!eos && raw_bytes_read < raw_bytes_threshold &&
           ((raw_rows_read < raw_rows_threshold && has_free_block) ||
            num_rows_in_block < state->batch_size())) {
        if (UNLIKELY(ctx->done())) {
            // No need to set status on error here.
            // Because done() maybe caused by "should_stop"
            should_stop = true;
            break;
        }

        BlockUPtr block = ctx->get_free_block(&has_free_block);
        status = scanner->get_block(state, block.get(), &eos);
        VLOG_ROW << "VScanNode input rows: " << block->rows() << ", eos: " << eos;
        // The VFileScanner for external table may try to open not exist files,
        // Because FE file cache for external table may out of date.
        // So, NOT_FOUND for VFileScanner is not a fail case.
        // Will remove this after file reader refactor.
        if (!status.ok() && (typeid(*scanner) != typeid(doris::vectorized::VFileScanner) ||
                             (typeid(*scanner) == typeid(doris::vectorized::VFileScanner) &&
                              !status.is<ErrorCode::NOT_FOUND>()))) {
            LOG(WARNING) << "Scan thread read VScanner failed: " << status.to_string();
            break;
        }
        if (status.is<ErrorCode::NOT_FOUND>()) {
            // The only case in this if branch is external table file delete and fe cache has not been updated yet.
            // Set status to OK.
            status = Status::OK();
            eos = true;
        }

        raw_bytes_read += block->bytes();
        num_rows_in_block += block->rows();
        if (UNLIKELY(block->rows() == 0)) {
            ctx->return_free_block(std::move(block));
        } else {
            if (!blocks.empty() && blocks.back()->rows() + block->rows() <= state->batch_size()) {
                vectorized::MutableBlock(blocks.back().get()).merge(*block);
                ctx->return_free_block(std::move(block));
            } else {
                blocks.push_back(std::move(block));
            }
        }
        raw_rows_read = scanner->get_rows_read();
    } // end for while

    // if we failed, check status.
    if (UNLIKELY(!status.ok())) {
        // _transfer_done = true;
        ctx->set_status_on_error(status);
        eos = true;
        blocks.clear();
    } else if (should_stop) {
        // No need to return blocks because of should_stop, just delete them
        blocks.clear();
    } else if (!blocks.empty()) {
        ctx->append_blocks_to_queue(blocks);
    }

    scanner->update_scan_cpu_timer();
    if (eos || should_stop) {
        scanner->mark_to_need_to_close();
    }

    ctx->push_back_scanner_and_reschedule(scanner);
}

} // namespace doris::vectorized
