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

#include <brpc/controller.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/types.pb.h>

#include <atomic>
#include <memory>
#include <string>

#include "common/config.h"
#include "common/factory_creator.h"
#include "common/object_pool.h"
#include "runtime/datetime_value.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/runtime_filter_mgr.h"
#include "runtime/runtime_predicate.h"
#include "service/backend_options.h"
#include "task_group/task_group.h"
#include "util/pretty_printer.h"
#include "util/threadpool.h"
#include "vec/exec/scan/scanner_scheduler.h"
#include "vec/runtime/shared_hash_table_controller.h"
#include "vec/runtime/shared_scanner_controller.h"

namespace doris {

class ShuffleKeepAliveHandler;

class KeepAliveClosure : public google::protobuf::Closure {
public:
    KeepAliveClosure() = default;

    ~KeepAliveClosure() override = default;
    KeepAliveClosure(const KeepAliveClosure& other) = delete;
    KeepAliveClosure& operator=(const KeepAliveClosure& other) = delete;

    void Run() noexcept override {
        try {
            if (cntl.Failed()) {
                std::string err = fmt::format(
                        "failed to send brpc when pull exchange source keep alive, error={}, "
                        "error_text={}, client: {}, "
                        "latency = {}",
                        berror(cntl.ErrorCode()), cntl.ErrorText(), BackendOptions::get_localhost(),
                        cntl.latency_us());
                callback_fn(exchange_node_id, false);
            } else {
                // const bool ret = keep_alive_response.is_alive;
                // callback_fn(exchange_node_id, ret);
            }
        } catch (const std::exception& exp) {
            LOG(FATAL) << "brpc callback error: " << exp.what();
        } catch (...) {
            LOG(FATAL) << "brpc callback error.";
        }
    }

    brpc::Controller cntl;
    PPipelineKeepAliveResponse keep_alive_response;
    std::function<void(const int&, const bool&)> callback_fn;
    int exchange_node_id;
};

class ShuffleKeepAliveHandler {
public:
    class KeepAliveContext {
    public:
        std::vector<std::shared_ptr<PBackendService_Stub>> source_client_list;
        int last_keep_alive_time = 0;
        bool is_source_alive = true;
        bool has_send_request = false;
        std::unique_ptr<KeepAliveClosure> keep_alive_closure = nullptr;
        PPipelineKeepAliveRequest keep_alive_request;
    };

    Status init_keep_alive_ctx(
            const std::map<int, std::vector<TPlanFragmentSourceHost>> src_hosts, ExecEnv* exec_env) {
        // rethinking whether should lazy init here?
        for (auto iter = src_hosts.begin(); iter != src_hosts.end(); iter++) {
            int exchange_node_id = iter->first;
            // 1 init keep alive lock
            keep_alive_lock[exchange_node_id] = std::make_unique<std::mutex>();

            // 2 init keep alive context
            // keep_alive_ctx_map[exchange_node_id] = {};

            // 3 init rpc client
            // auto& host_list = iter->second;
            // auto& client_list = keep_alive_ctx_map[exchange_node_id].source_client_list;
            // for (int i = 0; i < host_list.size(); i++) {
                // auto& host = host_list[i];
                // std::shared_ptr<PBackendService_Stub> brpc_stub = nullptr;
                // if (host.hostname == BackendOptions::get_localhost()) {
                //     brpc_stub = exec_env->brpc_internal_client_cache()->get_client(
                //             "127.0.0.1", 8080); // todo fix it
                // } else {
                //     brpc_stub =
                //             exec_env->brpc_internal_client_cache()->get_client(_brpc_dest_addr);
                // }
                // if (!brpc_stub) {
                //     std::string msg =
                //             fmt::format("Get exchange source client failed, dest_addr={}:{}",
                //                         "", 8080); // todo fix it
                //     LOG(WARNING) << msg;
                //     return Status::InternalError(msg);
                // }
                // client_list.insert(std::move(brpc_stub));
            // }
        }
        return Status::OK();
    }

    void keep_alive_callback(const int exchange_node_id, const bool is_source_alive) {
        std::unique_lock<std::mutex> lock(*keep_alive_lock[exchange_node_id]);
        auto& keep_alive_ctx = keep_alive_ctx_map[exchange_node_id];
        if (!keep_alive_ctx.is_source_alive) {
            keep_alive_ctx.is_source_alive = is_source_alive;
        }
        keep_alive_ctx_map[exchange_node_id].last_keep_alive_time = MonotonicMillis();
    }

    void update_keep_alive_time(int exchange_node_id) {
        std::unique_lock<std::mutex> lock(*keep_alive_lock[exchange_node_id]);
        keep_alive_ctx_map[exchange_node_id].last_keep_alive_time = MonotonicMillis();
    }

    void send_keep_alive_rpc(KeepAliveContext* keep_alive_ctx, TUniqueId query_id,
                             int exchange_node_id) {
        auto& client_list = keep_alive_ctx->source_client_list;

        // init closure
        if (!keep_alive_ctx->keep_alive_closure) {
            keep_alive_ctx->keep_alive_closure.reset(new KeepAliveClosure());
        } else {
            keep_alive_ctx->keep_alive_closure->cntl.Reset();
        }

        keep_alive_ctx->keep_alive_closure->exchange_node_id = exchange_node_id;
        keep_alive_ctx->keep_alive_closure->callback_fn = [&](const int exchange_node_id,
                                                              const bool is_source_alive) {
            keep_alive_callback(exchange_node_id, is_source_alive);
        };

        // keep_alive_ctx->keep_alive_request.query_id.set_hi(query_id.hi);
        // keep_alive_ctx->keep_alive_request.query_id.set_lo(query_id.lo);
        for (int i = 0; i < client_list.size(); i++) {
            client_list[i]->pipeline_keep_alive(&keep_alive_ctx->keep_alive_closure->cntl,
                                                &keep_alive_ctx->keep_alive_request,
                                                &keep_alive_ctx->keep_alive_closure->keep_alive_response,
                                                keep_alive_ctx->keep_alive_closure.get());
        }
    }

    // return true, source client is timeout
    bool handle_keep_alive_timeout(int exchange_node_id, TUniqueId query_id) {
        std::unique_lock<std::mutex> lock(*keep_alive_lock[exchange_node_id]);
        auto& keep_alive_ctx = keep_alive_ctx_map[exchange_node_id];

        if (!keep_alive_ctx.is_source_alive) {
            return true;
        }

        if (keep_alive_ctx.last_keep_alive_time == 0) {
            keep_alive_ctx.last_keep_alive_time = MonotonicMillis();
            return false;
        }

        // case 1: timeout and has not send keep alive request
        if (!keep_alive_ctx.has_send_request &&
            keep_alive_ctx.last_keep_alive_time - MonotonicMillis() >
                    config::pipeline_keep_alive_timeout_ms) {
            send_keep_alive_rpc(&keep_alive_ctx, query_id, exchange_node_id);
            keep_alive_ctx.has_send_request = true;
        }
        return false;
    }

    std::map<int, std::unique_ptr<std::mutex>> keep_alive_lock;
    std::map<int, KeepAliveContext> keep_alive_ctx_map;
};

// Save the common components of fragments in a query.
// Some components like DescriptorTbl may be very large
// that will slow down each execution of fragments when DeSer them every time.
class DescriptorTbl;
class QueryContext {
    ENABLE_FACTORY_CREATOR(QueryContext);

public:
    QueryContext(int total_fragment_num, ExecEnv* exec_env, const TQueryOptions& query_options)
            : fragment_num(total_fragment_num),
              timeout_second(-1),
              _exec_env(exec_env),
              _runtime_filter_mgr(new RuntimeFilterMgr(TUniqueId(), this)),
              _query_options(query_options) {
        _start_time = vectorized::VecDateTimeValue::local_time();
        _shared_hash_table_controller.reset(new vectorized::SharedHashTableController());
        _shared_scanner_controller.reset(new vectorized::SharedScannerController());
    }

    ~QueryContext() {
        // query mem tracker consumption is equal to 0, it means that after QueryContext is created,
        // it is found that query already exists in _query_ctx_map, and query mem tracker is not used.
        // query mem tracker consumption is not equal to 0 after use, because there is memory consumed
        // on query mem tracker, released on other trackers.
        if (query_mem_tracker->peak_consumption() != 0) {
            LOG(INFO) << fmt::format(
                    "Deregister query/load memory tracker, queryId={}, Limit={}, CurrUsed={}, "
                    "PeakUsed={}",
                    print_id(query_id), MemTracker::print_bytes(query_mem_tracker->limit()),
                    MemTracker::print_bytes(query_mem_tracker->consumption()),
                    MemTracker::print_bytes(query_mem_tracker->peak_consumption()));
        }
        if (_task_group) {
            _task_group->remove_mem_tracker_limiter(query_mem_tracker);
        }
    }

    // Notice. For load fragments, the fragment_num sent by FE has a small probability of 0.
    // this may be a bug, bug <= 1 in theory it shouldn't cause any problems at this stage.
    bool countdown() { return fragment_num.fetch_sub(1) <= 1; }

    ExecEnv* exec_env() { return _exec_env; }

    bool is_timeout(const vectorized::VecDateTimeValue& now) const {
        if (timeout_second <= 0) {
            return false;
        }
        if (now.second_diff(_start_time) > timeout_second) {
            return true;
        }
        return false;
    }

    void set_thread_token(int concurrency, bool is_serial) {
        _thread_token = _exec_env->scanner_scheduler()->new_limited_scan_pool_token(
                is_serial ? ThreadPool::ExecutionMode::SERIAL
                          : ThreadPool::ExecutionMode::CONCURRENT,
                concurrency);
    }

    ThreadPoolToken* get_token() { return _thread_token.get(); }

    void set_ready_to_execute(bool is_cancelled) {
        {
            std::lock_guard<std::mutex> l(_start_lock);
            _is_cancelled = is_cancelled;
            _ready_to_execute = true;
        }
        if (query_mem_tracker && is_cancelled) {
            query_mem_tracker->set_is_query_cancelled(is_cancelled);
        }
        _start_cond.notify_all();
    }
    void set_ready_to_execute_only() {
        {
            std::lock_guard<std::mutex> l(_start_lock);
            _ready_to_execute = true;
        }
        _start_cond.notify_all();
    }

    bool is_ready_to_execute() {
        std::lock_guard<std::mutex> l(_start_lock);
        return _ready_to_execute;
    }

    bool wait_for_start() {
        int wait_time = config::max_fragment_start_wait_time_seconds;
        std::unique_lock<std::mutex> l(_start_lock);
        while (!_ready_to_execute.load() && !_is_cancelled.load() && --wait_time > 0) {
            _start_cond.wait_for(l, std::chrono::seconds(1));
        }
        return _ready_to_execute.load() && !_is_cancelled.load();
    }

    std::shared_ptr<vectorized::SharedHashTableController> get_shared_hash_table_controller() {
        return _shared_hash_table_controller;
    }

    std::shared_ptr<vectorized::SharedScannerController> get_shared_scanner_controller() {
        return _shared_scanner_controller;
    }

    vectorized::RuntimePredicate& get_runtime_predicate() { return _runtime_predicate; }

    void set_task_group(taskgroup::TaskGroupPtr& tg) { _task_group = tg; }

    taskgroup::TaskGroup* get_task_group() const { return _task_group.get(); }

    int execution_timeout() const {
        return _query_options.__isset.execution_timeout ? _query_options.execution_timeout
                                                        : _query_options.query_timeout;
    }

    int32_t runtime_filter_wait_time_ms() const {
        return _query_options.runtime_filter_wait_time_ms;
    }

    bool enable_pipeline_exec() const {
        return _query_options.__isset.enable_pipeline_engine &&
               _query_options.enable_pipeline_engine;
    }

    int be_exec_version() const {
        if (!_query_options.__isset.be_exec_version) {
            return 0;
        }
        return _query_options.be_exec_version;
    }

    RuntimeFilterMgr* runtime_filter_mgr() { return _runtime_filter_mgr.get(); }

public:
    TUniqueId query_id;
    DescriptorTbl* desc_tbl;
    bool set_rsc_info = false;
    std::string user;
    std::string group;
    TNetworkAddress coord_addr;
    TQueryGlobals query_globals;

    /// In the current implementation, for multiple fragments executed by a query on the same BE node,
    /// we store some common components in QueryContext, and save QueryContext in FragmentMgr.
    /// When all Fragments are executed, QueryContext needs to be deleted from FragmentMgr.
    /// Here we use a counter to store the number of Fragments that have not yet been completed,
    /// and after each Fragment is completed, this value will be reduced by one.
    /// When the last Fragment is completed, the counter is cleared, and the worker thread of the last Fragment
    /// will clean up QueryContext.
    std::atomic<int> fragment_num;
    int timeout_second;
    ObjectPool obj_pool;
    // MemTracker that is shared by all fragment instances running on this host.
    std::shared_ptr<MemTrackerLimiter> query_mem_tracker;

    std::vector<TUniqueId> fragment_ids;

    // plan node id -> TFileScanRangeParams
    // only for file scan node
    std::map<int, TFileScanRangeParams> file_scan_range_params_map;

    ShuffleKeepAliveHandler shuffle_keep_alive_handler;

private:
    ExecEnv* _exec_env;
    vectorized::VecDateTimeValue _start_time;

    // A token used to submit olap scanner to the "_limited_scan_thread_pool",
    // This thread pool token is created from "_limited_scan_thread_pool" from exec env.
    // And will be shared by all instances of this query.
    // So that we can control the max thread that a query can be used to execute.
    // If this token is not set, the scanner will be executed in "_scan_thread_pool" in exec env.
    std::unique_ptr<ThreadPoolToken> _thread_token;

    std::mutex _start_lock;
    std::condition_variable _start_cond;
    // Only valid when _need_wait_execution_trigger is set to true in FragmentExecState.
    // And all fragments of this query will start execution when this is set to true.
    std::atomic<bool> _ready_to_execute {false};
    std::atomic<bool> _is_cancelled {false};

    std::shared_ptr<vectorized::SharedHashTableController> _shared_hash_table_controller;
    std::shared_ptr<vectorized::SharedScannerController> _shared_scanner_controller;
    vectorized::RuntimePredicate _runtime_predicate;

    taskgroup::TaskGroupPtr _task_group;
    std::unique_ptr<RuntimeFilterMgr> _runtime_filter_mgr;
    const TQueryOptions _query_options;
};

} // namespace doris
