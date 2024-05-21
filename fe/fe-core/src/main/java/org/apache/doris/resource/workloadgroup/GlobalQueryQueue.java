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

package org.apache.doris.resource.workloadgroup;

import org.apache.doris.common.UserException;
import org.apache.doris.qe.Coordinator;

import java.util.HashMap;

import org.apache.doris.thrift.TUniqueId;


public class GlobalQueryQueue extends QueryQueue{

    private Map<TUniqueId, QueueToken> queueTokenMap = new HashMap();

    public GlobalQueryQueue(long wgId, int maxConcurrency, int maxQueueSize, int queueTimeout, long propVersion) {
        super(wgId, maxConcurrency, maxQueueSize, queueTimeout, propVersion);
    }

    @Override
    public QueueToken getToken(Coordinator coord) throws UserException {
        QueueToken queueToken = new QueueToken(coord.getQueryId(), null, queueTimeout,
                coord.getQueryOptions().getExecutionTimeout() * 1000);
        queueTokenMap.put(queueToken.getQueryId(), queueToken);
        // todo: need a rpc here
        return queueToken;
    }

    public void notifyQueueToken(List<AdmissionResponse> list) {
        for (AdmissionResponse response : list) {
            TUniqueId query_id = response.query_id;
            QueueToken qt = queueTokenMap.get(query_id);
            if (qt != null) {
                if (response.is_success) {
                    qt.complete();
                } else {
                    qt.completeThrowable();
                }
            }
        }
    }

    @Override
    public void notifyQueuedQuery(QueueToken curQueryToken) {
        //todo need a rpc here, release
    }

}
