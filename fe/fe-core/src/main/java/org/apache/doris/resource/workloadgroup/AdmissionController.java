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

import org.apache.doris.catalog.Env;

import com.google.common.collect.Maps;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class AdmissionController {

    // workload group id -> request
    private Map<Long, WorkloadGroupAdmission> wgCtxMap = Maps.newConcurrentMap();
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private Queue<AdmissionRequest> rpcRequestList = new LinkedList<>();

    // Object is just a placeholder, it's meaningless
    private CompletableFuture<Object> future = new CompletableFuture<>();

    private static final int DEFAULT_RUN_INTERVAL_MS = 200;

    public void addAdmissionRequest(TAdmissionRequest req) {
        AdmissionRequest admissionRequest = AdmissionRequest.fromThrift(req);
        // this is a global lock across all fe, so we should make add operator lightweight
        writeLock();
        try {
            // todo: add capacity limitation
            rpcRequestList.offer(admissionRequest);
        } finally {
            writeUnlock();
        }
        future.complete(null);
    }

    public Queue<AdmissionRequest> getRpcRequestListCopy() {
        writeLock();
        try {
            Queue<AdmissionRequest> ret = rpcRequestList;
            rpcRequestList = new LinkedList<>();
            return ret;
        } finally {
            writeUnlock();
        }
    }

    public void run() {
        Queue<AdmissionRequest> requestList = getRpcRequestListCopy();
        Iterator<AdmissionRequest> iter = requestList.iterator();
        while (iter.hasNext()) {
            AdmissionRequest request = iter.next();
            WorkloadGroupAdmission wgCtx = wgCtxMap.get(request.getWorkloadGroupId());
            if (wgCtx == null) {
                wgCtx = new WorkloadGroupAdmission(request.getWorkloadGroupId());
                wgCtxMap.put(request.getWorkloadGroupId(), wgCtx);
            }
            wgCtx.addRpcReqQueue(request);
        }

        Iterator<Map.Entry<Long, WorkloadGroupAdmission>> wgIterator = wgCtxMap.entrySet().iterator();
        while (wgIterator.hasNext()) {
            try {
                WorkloadGroupAdmission wgCtx = wgIterator.next().getValue();
                List<TAdmissionResponse> responseList = wgCtx.execute();
                asyncResponse(responseList);
                if (wgCtx.canRelease()) {
                    wgIterator.remove();
                }
            } catch (Throwable t) {
            }
        }

        // when to sleep:
        // 1 no new rpc request arrives, rpc request list is empty
        // 2 when all workload group is released
        // when to notify:
        // 1 fe process missed, dead or restart.
        // 2 new rpc request arrives
        // 3 query finished
        if (wgCtxMap.size() == 0 || rpcRequestList.isEmpty()) {
            sleep(DEFAULT_RUN_INTERVAL_MS);
        }
    }

    private void sleep(int sleepTimeMs) {
        try {
            future.get(sleepTimeMs, TimeUnit.MILLISECONDS);
        } catch (Throwable t) {
        }
    }

    private void notifyWhenQueryFinish(long wgId, TUniqueId queryId) {
        WorkloadGroupAdmission wgCtx = wgCtxMap.get(Long.valueOf(wgId));
        if (wgCtx != null) {
            wgCtx.markQueryFinished(queryId);
            future.complete(null);
        }
    }

    private void notifyWhenFeProcessMissing() {
        future.complete(null);
    }

    private void asyncResponse(List<TAdmissionResponse> responseList) {
    }

    private void readLock() {
        lock.readLock().lock();
    }

    private void readUnlock() {
        lock.readLock().unlock();
    }

    private void writeLock() {
        lock.writeLock().lock();
    }

    private void writeUnlock() {
        lock.writeLock().unlock();
    }

}
