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

#include "olap/metadata_adder.h"

#include <bvar/bvar.h>

static bvar::Adder<int64_t> g_total_rowset_meta_mem_size("doris_total_rowset_meta_mem_size");
static bvar::Adder<int64_t> g_total_rowset_meta_num("doris_total_rowset_meta_num");
static bvar::Adder<int64_t> g_total_tablet_meta_mem_size("doris_total_tablet_meta_mem_size");
static bvar::Adder<int64_t> g_total_tablet_meta_num("doris_total_tablet_meta_num");
static bvar::Adder<int64_t> g_total_tablet_column_mem_size("doris_total_tablet_column_mem_size");
static bvar::Adder<int64_t> g_total_tablet_column_num("doris_total_tablet_column_num");
static bvar::Adder<int64_t> g_total_tablet_index_mem_size("doris_total_tablet_index_mem_size");
static bvar::Adder<int64_t> g_total_tablet_index_num("doris_total_tablet_index_num");
static bvar::Adder<int64_t> g_total_tablet_schema_mem_size("doris_total_tablet_schema_mem_size");
static bvar::Adder<int64_t> g_total_tablet_schema_num("doris_total_tablet_schema_num");
static bvar::Adder<int64_t> g_total_segment_mem_size("doris_total_segment_mem_size");
static bvar::Adder<int64_t> g_total_segment_num("doris_total_segment_num");

namespace doris {

template <typename T>
void MetadataAdder<T>::add_mem_size(int64_t val) {
    if (val == 0) {
        return;
    }
    if constexpr (std::is_same_v<T, RowsetMeta>) {
        g_total_rowset_meta_mem_size << val;
    } else if constexpr (std::is_same_v<T, TabletMeta>) {
        g_total_tablet_meta_mem_size << val;
    } else if constexpr (std::is_same_v<T, TabletColumn>) {
        g_total_tablet_column_mem_size << val;
    } else if constexpr (std::is_same_v<T, TabletIndex>) {
        g_total_tablet_index_mem_size << val;
    } else if constexpr (std::is_same_v<T, TabletSchema>) {
        g_total_tablet_schema_mem_size << val;
    } else if constexpr (std::is_same_v<T, segment_v2::Segment>) {
        g_total_segment_mem_size << val;
    }
}

template <typename T>
void MetadataAdder<T>::add_num(int64_t val) {
    if (val == 0) {
        return;
    }
    if constexpr (std::is_same_v<T, RowsetMeta>) {
        g_total_rowset_meta_num << val;
    } else if constexpr (std::is_same_v<T, TabletMeta>) {
        g_total_tablet_meta_num << val;
    } else if constexpr (std::is_same_v<T, TabletColumn>) {
        g_total_tablet_column_num << val;
    } else if constexpr (std::is_same_v<T, TabletIndex>) {
        g_total_tablet_index_num << val;
    } else if constexpr (std::is_same_v<T, TabletSchema>) {
        g_total_tablet_schema_num << val;
    } else if constexpr (std::is_same_v<T, segment_v2::Segment>) {
        g_total_segment_num << val;
    }
}

}; // namespace doris