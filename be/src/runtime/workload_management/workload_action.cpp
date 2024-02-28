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

#include "runtime/workload_management/workload_action.h"

#include "runtime/fragment_mgr.h"

namespace doris {

void WorkloadActionCancelQuery::exec(WorkloadQueryInfo* query_info) {
    LOG(INFO) << "[workload_schedule]workload scheduler cancel query " << query_info->query_id;
    ExecEnv::GetInstance()->fragment_mgr()->cancel_query(
            query_info->tquery_id, PPlanFragmentCancelReason::INTERNAL_ERROR,
            std::string("query canceled by workload scheduler"));
}

void WorkloadActionMoveQuery::exec(WorkloadQueryInfo* query_info) {
    if (auto query_ctx_ptr = query_info->query_ctx_weak_ptr.lock()) {
        Status ret = query_ctx_ptr->move_to_group(_wg_id);
        if (!ret.ok()) {
            LOG(INFO) << "[workload_schedule] move query " << query_info->query_id << " to group "
                      << _wg_id << " failed, reason=" << ret.to_string_no_stack();
        }
    }
};

} // namespace doris