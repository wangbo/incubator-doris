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

#include "common/daemon.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <gflags/gflags.h>
#include <gperftools/malloc_extension.h> // IWYU pragma: keep
// IWYU pragma: no_include <bits/std_abs.h>
#include <math.h>
#include <signal.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <algorithm>
// IWYU pragma: no_include <bits/chrono.h>
#include <chrono> // IWYU pragma: keep
#include <map>
#include <ostream>
#include <set>
#include <string>

#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/options.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/block_spill_manager.h"
#include "runtime/exec_env.h"
#include "runtime/load_channel_mgr.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/task_group/task_group_manager.h"
#include "runtime/user_function_cache.h"
#include "service/backend_options.h"
#include "util/cpu_info.h"
#include "util/debug_util.h"
#include "util/disk_info.h"
#include "util/doris_metrics.h"
#include "util/mem_info.h"
#include "util/metrics.h"
#include "util/network_util.h"
#include "util/perf_counters.h"
#include "util/system_metrics.h"
#include "util/thrift_util.h"
#include "util/time.h"

namespace doris {

void Daemon::tcmalloc_gc_thread() {
    size_t val = 0;
    MallocExtension::instance()->GetNumericProperty("tcmalloc.aggressive_memory_decommit", &val);
}

void Daemon::memory_maintenance_thread() {
    int32_t interval_milliseconds = config::memory_maintenance_sleep_time_ms;
    int64_t last_print_proc_mem = PerfCounters::get_vm_rss();
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(interval_milliseconds))) {
        if (!MemInfo::initialized() || !ExecEnv::GetInstance()->initialized()) {
            continue;
        }
        // Refresh process memory metrics.
        doris::PerfCounters::refresh_proc_status();
        doris::MemInfo::refresh_proc_meminfo();
        doris::MemInfo::refresh_proc_mem_no_allocator_cache();

        // Update and print memory stat when the memory changes by 256M.
        if (abs(last_print_proc_mem - PerfCounters::get_vm_rss()) > 268435456) {
            last_print_proc_mem = PerfCounters::get_vm_rss();
            doris::MemTrackerLimiter::enable_print_log_process_usage();

            // Refresh mem tracker each type counter.
            doris::MemTrackerLimiter::refresh_global_counter();

            // Refresh allocator memory metrics.
#if !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && !defined(THREAD_SANITIZER)
            doris::MemInfo::refresh_allocator_mem();
            if (config::enable_system_metrics) {
                DorisMetrics::instance()->system_metrics()->update_allocator_metrics();
            }
#endif
            LOG(INFO) << MemTrackerLimiter::
                            process_mem_log_str(); // print mem log when memory state by 256M
        }
    }
}

void Daemon::memory_gc_thread() {
    int32_t interval_milliseconds = config::memory_maintenance_sleep_time_ms;
    int32_t memory_minor_gc_sleep_time_ms = 0;
    int32_t memory_full_gc_sleep_time_ms = 0;
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(interval_milliseconds))) {
        if (!MemInfo::initialized() || !ExecEnv::GetInstance()->initialized()) {
            continue;
        }
        auto sys_mem_available = doris::MemInfo::sys_mem_available();
        auto proc_mem_no_allocator_cache = doris::MemInfo::proc_mem_no_allocator_cache();

        // GC excess memory for resource groups that not enable overcommit
        auto tg_free_mem = doris::MemInfo::tg_hard_memory_limit_gc();
        sys_mem_available += tg_free_mem;
        proc_mem_no_allocator_cache -= tg_free_mem;

        if (memory_full_gc_sleep_time_ms <= 0 &&
            (sys_mem_available < doris::MemInfo::sys_mem_available_low_water_mark() ||
             proc_mem_no_allocator_cache >= doris::MemInfo::mem_limit())) {
            // No longer full gc and minor gc during sleep.
            memory_full_gc_sleep_time_ms = config::memory_gc_sleep_time_ms;
            memory_minor_gc_sleep_time_ms = config::memory_gc_sleep_time_ms;
            doris::MemTrackerLimiter::print_log_process_usage("process full gc", false);
            if (doris::MemInfo::process_full_gc()) {
                // If there is not enough memory to be gc, the process memory usage will not be printed in the next continuous gc.
                doris::MemTrackerLimiter::enable_print_log_process_usage();
            }
        } else if (memory_minor_gc_sleep_time_ms <= 0 &&
                   (sys_mem_available < doris::MemInfo::sys_mem_available_warning_water_mark() ||
                    proc_mem_no_allocator_cache >= doris::MemInfo::soft_mem_limit())) {
            // No minor gc during sleep, but full gc is possible.
            memory_minor_gc_sleep_time_ms = config::memory_gc_sleep_time_ms;
            doris::MemTrackerLimiter::print_log_process_usage("process minor gc", false);
            if (doris::MemInfo::process_minor_gc()) {
                doris::MemTrackerLimiter::enable_print_log_process_usage();
            }
        } else {
            if (memory_full_gc_sleep_time_ms > 0) {
                memory_full_gc_sleep_time_ms -= interval_milliseconds;
            }
            if (memory_minor_gc_sleep_time_ms > 0) {
                memory_minor_gc_sleep_time_ms -= interval_milliseconds;
            }
        }
    }
}

void Daemon::load_channel_tracker_refresh_thread() {
    // Refresh the memory statistics of the load channel tracker more frequently,
    // which helps to accurately control the memory of LoadChannelMgr.
    while (!_stop_background_threads_latch.wait_for(
            std::chrono::milliseconds(config::load_channel_memory_refresh_sleep_time_ms))) {
        if (ExecEnv::GetInstance()->initialized()) {
            doris::ExecEnv::GetInstance()->load_channel_mgr()->refresh_mem_tracker();
        }
    }
}

void Daemon::memory_tracker_profile_refresh_thread() {
    while (!_stop_background_threads_latch.wait_for(std::chrono::milliseconds(50))) {
        MemTracker::refresh_all_tracker_profile();
        MemTrackerLimiter::refresh_all_tracker_profile();
    }
}

/*
 * this thread will calculate some metrics at a fix interval(15 sec)
 * 1. push bytes per second
 * 2. scan bytes per second
 * 3. max io util of all disks
 * 4. max network send bytes rate
 * 5. max network receive bytes rate
 */
void Daemon::calculate_metrics_thread() {
    int64_t last_ts = -1L;
    int64_t lst_query_bytes = -1;

    std::map<std::string, int64_t> lst_disks_io_time;
    std::map<std::string, int64_t> lst_net_send_bytes;
    std::map<std::string, int64_t> lst_net_receive_bytes;

    do {
        DorisMetrics::instance()->metric_registry()->trigger_all_hooks(true);

        if (last_ts == -1L) {
            last_ts = GetMonoTimeMicros() / 1000;
            lst_query_bytes = DorisMetrics::instance()->query_scan_bytes->value();
            if (config::enable_system_metrics) {
                DorisMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);
                DorisMetrics::instance()->system_metrics()->get_network_traffic(
                        &lst_net_send_bytes, &lst_net_receive_bytes);
            }
        } else {
            int64_t current_ts = GetMonoTimeMicros() / 1000;
            long interval = (current_ts - last_ts) / 1000;
            last_ts = current_ts;

            // 1. query bytes per second
            int64_t current_query_bytes = DorisMetrics::instance()->query_scan_bytes->value();
            int64_t qps = (current_query_bytes - lst_query_bytes) / (interval + 1);
            DorisMetrics::instance()->query_scan_bytes_per_second->set_value(qps < 0 ? 0 : qps);
            lst_query_bytes = current_query_bytes;

            if (config::enable_system_metrics) {
                // 2. max disk io util
                DorisMetrics::instance()->system_metrics()->update_max_disk_io_util_percent(
                        lst_disks_io_time, 15);

                // update lst map
                DorisMetrics::instance()->system_metrics()->get_disks_io_time(&lst_disks_io_time);

                // 3. max network traffic
                int64_t max_send = 0;
                int64_t max_receive = 0;
                DorisMetrics::instance()->system_metrics()->get_max_net_traffic(
                        lst_net_send_bytes, lst_net_receive_bytes, 15, &max_send, &max_receive);
                DorisMetrics::instance()->system_metrics()->update_max_network_send_bytes_rate(
                        max_send);
                DorisMetrics::instance()->system_metrics()->update_max_network_receive_bytes_rate(
                        max_receive);
                // update lst map
                DorisMetrics::instance()->system_metrics()->get_network_traffic(
                        &lst_net_send_bytes, &lst_net_receive_bytes);
            }

            DorisMetrics::instance()->all_rowsets_num->set_value(
                    StorageEngine::instance()->tablet_manager()->get_rowset_nums());
            DorisMetrics::instance()->all_segments_num->set_value(
                    StorageEngine::instance()->tablet_manager()->get_segment_nums());
        }
    } while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(15)));
}

// clean up stale spilled files
void Daemon::block_spill_gc_thread() {
    while (!_stop_background_threads_latch.wait_for(std::chrono::seconds(60))) {
        if (ExecEnv::GetInstance()->initialized()) {
            ExecEnv::GetInstance()->block_spill_mgr()->gc(200);
        }
    }
}

static void init_doris_metrics(const std::vector<StorePath>& store_paths) {
    bool init_system_metrics = config::enable_system_metrics;
    std::set<std::string> disk_devices;
    std::vector<std::string> network_interfaces;
    std::vector<std::string> paths;
    for (auto& store_path : store_paths) {
        paths.emplace_back(store_path.path);
    }
    if (init_system_metrics) {
        auto st = DiskInfo::get_disk_devices(paths, &disk_devices);
        if (!st.ok()) {
            LOG(WARNING) << "get disk devices failed, status=" << st;
            return;
        }
        st = get_inet_interfaces(&network_interfaces, BackendOptions::is_bind_ipv6());
        if (!st.ok()) {
            LOG(WARNING) << "get inet interfaces failed, status=" << st;
            return;
        }
    }
    DorisMetrics::instance()->initialize(init_system_metrics, disk_devices, network_interfaces);
}

void signal_handler(int signal) {
    if (signal == SIGINT || signal == SIGTERM) {
        k_doris_exit = true;
    }
}

int install_signal(int signo, void (*handler)(int)) {
    struct sigaction sa;
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = handler;
    sigemptyset(&sa.sa_mask);
    auto ret = sigaction(signo, &sa, nullptr);
    if (ret != 0) {
        char buf[64];
        LOG(ERROR) << "install signal failed, signo=" << signo << ", errno=" << errno
                   << ", errmsg=" << strerror_r(errno, buf, sizeof(buf));
    }
    return ret;
}

void init_signals() {
    auto ret = install_signal(SIGINT, signal_handler);
    if (ret < 0) {
        exit(-1);
    }
    ret = install_signal(SIGTERM, signal_handler);
    if (ret < 0) {
        exit(-1);
    }
}

void Daemon::init(int argc, char** argv, const std::vector<StorePath>& paths) {
    // google::SetVersionString(get_build_version(false));
    // google::ParseCommandLineFlags(&argc, &argv, true);
    google::ParseCommandLineFlags(&argc, &argv, true);
    init_glog("be");

    LOG(INFO) << get_version_string(false);

    init_thrift_logging();
    CpuInfo::init();
    DiskInfo::init();
    MemInfo::init();
    UserFunctionCache::instance()->init(config::user_function_dir);

    LOG(INFO) << CpuInfo::debug_string();
    LOG(INFO) << DiskInfo::debug_string();
    LOG(INFO) << MemInfo::debug_string();

    init_doris_metrics(paths);
    init_signals();
}

void Daemon::start() {
    Status st;
    st = Thread::create(
            "Daemon", "tcmalloc_gc_thread", [this]() { this->tcmalloc_gc_thread(); },
            &_tcmalloc_gc_thread);
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "memory_maintenance_thread", [this]() { this->memory_maintenance_thread(); },
            &_memory_maintenance_thread);
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "memory_gc_thread", [this]() { this->memory_gc_thread(); },
            &_memory_gc_thread);
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "load_channel_tracker_refresh_thread",
            [this]() { this->load_channel_tracker_refresh_thread(); },
            &_load_channel_tracker_refresh_thread);
    CHECK(st.ok()) << st;
    st = Thread::create(
            "Daemon", "memory_tracker_profile_refresh_thread",
            [this]() { this->memory_tracker_profile_refresh_thread(); },
            &_memory_tracker_profile_refresh_thread);
    CHECK(st.ok()) << st;

    if (config::enable_metric_calculator) {
        st = Thread::create(
                "Daemon", "calculate_metrics_thread",
                [this]() { this->calculate_metrics_thread(); }, &_calculate_metrics_thread);
        CHECK(st.ok()) << st;
    }
    st = Thread::create(
            "Daemon", "block_spill_gc_thread", [this]() { this->block_spill_gc_thread(); },
            &_block_spill_gc_thread);
    CHECK(st.ok()) << st;
}

void Daemon::stop() {
    _stop_background_threads_latch.count_down();

    if (_tcmalloc_gc_thread) {
        _tcmalloc_gc_thread->join();
    }
    if (_memory_maintenance_thread) {
        _memory_maintenance_thread->join();
    }
    if (_memory_gc_thread) {
        _memory_gc_thread->join();
    }
    if (_load_channel_tracker_refresh_thread) {
        _load_channel_tracker_refresh_thread->join();
    }
    if (_memory_tracker_profile_refresh_thread) {
        _memory_tracker_profile_refresh_thread->join();
    }
    if (_calculate_metrics_thread) {
        _calculate_metrics_thread->join();
    }
    if (_block_spill_gc_thread) {
        _block_spill_gc_thread->join();
    }
}

} // namespace doris
