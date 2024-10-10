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

#include <stdint.h>

namespace doris {

class RowsetMeta;
class TabletMeta;
class TabletColumn;
class TabletIndex;
class TabletSchema;

namespace segment_v2 {
class Segment;
};

template <typename T>
class MetadataAdder {
private:
    int64_t current_meta_size {0};
    void add_mem_size(int64_t val);
    void add_num(int64_t val);

protected:
    virtual ~MetadataAdder();
    virtual int64_t get_metadata_size() = 0;
    MetadataAdder(const MetadataAdder& other);

public:
    MetadataAdder();
    void update_metadata_size();
};

template <typename T>
MetadataAdder<T>::MetadataAdder(const MetadataAdder<T>& other) {
    this->current_meta_size = other.current_meta_size;
    add_num(1);
    add_mem_size(this->current_meta_size);
}

template <typename T>
MetadataAdder<T>::MetadataAdder() {
    update_metadata_size();
    add_num(1);
}

template <typename T>
MetadataAdder<T>::~MetadataAdder() {
    add_mem_size(-current_meta_size);
    add_num(-1);
}

template <typename T>
void MetadataAdder<T>::update_metadata_size() {
    int64_t old_size = current_meta_size;
    int64_t current_meta_size = get_metadata_size();
    int64_t size_diff = current_meta_size - old_size;
    add_mem_size(size_diff);
}

}; // namespace doris