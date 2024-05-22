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


public class AdmissionRequest {

    private TUniqueId queryId;
    private TNetworkAddress clientFeAddr;
    private long wgId;
    private int  queryTimeout;
    private int queueTimeout;
    private long queryStartTime;
    private long feProcessId;

    private volatile boolean isQueryFinished = false;

    public AdmissionRequest(TUniqueId queryId, TNetworkAddress clientFeAddr, long wgId, int queryTimeout,
            int queueTimeout, long queryStartTime, long feProcessId) {
        this.queryId = queryId;
        this.clientFeAddr = clientFeAddr;
        this.wgId = wgId;
        this.queryTimeout = queryTimeout;
        this.queueTimeout = queueTimeout;
        this.queryStartTime = queryStartTime;
        this.feProcessId = feProcessId;
    }

    public static AdmissionRequest fromThrift(TAdmissionRequest tRequest) {
        AdmissionRequest request = new AdmissionRequest(tRequest.query_id, tRequest.client_fe_addr, tRequest.wg_id,
                tRequest.query_timeout, tRequest.queue_timeout, tRequest.query_start_time, tRequest.fe_process_id);
        return request;
    }

    public boolean isQueryTimeout(long currentTime) {
        return currentTime - queryStartTime > queryTimeout;
    }

    public boolean isQueueTimeout(long currentTime) {
        return currentTime - queryStartTime > queueTimeout;
    }

    public TAdmissionResponse toResponse(TStatusCode status, String errorMsg) {
        TAdmissionResponse response = new TAdmissionResponse();
        response.query_id = getQueryId();
        response.status = status;
        response.error_msg = errorMsg;
        return response;
    }

    public TAdmissionResponse toResponse(TStatusCode status) {
        return toResponse(status, "");
    }

    public void setQueryFinished() {
        this.isQueryFinished = true;
    }

    public boolean isQueryFinished() {
        return isQueryFinished;
    }

    public TUniqueId getQueryId() {
        return queryId;
    }

    public TNetworkAddress getClientFeAddr() {
        return clientFeAddr;
    }

    public long getWorkloadGroupId() {
        return wgId;
    }

    public int getQueryTimeout() {
        return queryTimeout;
    }

    public int getQueueTimeout() {
        return queueTimeout;
    }

    public long getQueryStartTime() {
        return queryStartTime;
    }

    public long getFeProcessId() {
        return feProcessId;
    }
}
