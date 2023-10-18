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

#include "agent/workload_group_listener.h"

#include "runtime/task_group/task_group.h"
#include "runtime/task_group/task_group_manager.h"
#include "util/parse_util.h"
#include "util/mem_info.h"

namespace doris {

void WorkloadGroupListener::handle_topic_info(const TPublishTopicRequest& topic_request) {
    for (const TopicInfo& topic_info : topic_request.topic_list) {
        if (topic_info.topic_type != doris::TTopicInfoType::type::WORKLOAD_GROUP) {
            continue;
        }

        uint64_t cpu_share = 0;
        auto iter = topic_info.info_map.find("cpu_share");
        std::from_chars(iter->second.c_str(), iter->second.c_str() + iter->second.size(),
                        cpu_share);

        int id = 0;
        auto iter2 = topic_info.info_map.find("id");
        std::from_chars(iter2->second.c_str(), iter2->second.c_str() + iter2->second.size(), id);

        int version = 0;
        auto iter3 = topic_info.info_map.find("version");
        std::from_chars(iter3->second.c_str(), iter3->second.c_str() + iter3->second.size(),
                        version);

        bool is_percent = true;
        std::string mem_limit_str = topic_info.info_map.find("memory_limit")->second;
        int64_t mem_limit =
                ParseUtil::parse_mem_spec(mem_limit_str, -1, MemInfo::mem_limit(), &is_percent);

        taskgroup::TaskGroupInfo task_group_info;
        task_group_info.id = id;
        task_group_info.name = topic_info.topic_key;
        task_group_info.version = version;
        task_group_info.cpu_share = cpu_share;
        task_group_info.memory_limit = mem_limit;

        auto ret = _exec_env->task_group_manager()->get_or_create_task_group(task_group_info);
        LOG(INFO) << "tg debug string=" << ret->debug_string();
    }
    LOG(INFO) << "finish update workload group info, size=" << topic_request.topic_list.size();
}
} // namespace doris