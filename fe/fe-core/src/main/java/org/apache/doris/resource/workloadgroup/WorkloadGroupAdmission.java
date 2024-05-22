package org.apache.doris.resource.workloadgroup;

import org.apache.doris.catalog.Env;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadGroupAdmission {

    private long wgId;
    private Queue<AdmissionRequest> rpcReqQueue = new LinkedList();
    private Queue<AdmissionRequest> waitingQueryQueue = new LinkedList();
    private Map<TUniqueId, AdmissionRequest> runningQueryMap = new HashMap();

    private boolean isWorkloadGroupDropped = false;
    int maxConcurrency;
    int maxQueueSize;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public WorkloadGroupAdmission(long wgId) {
        this.wgId = wgId;
    }

    public void addRpcReqQueue(AdmissionRequest req) {
        rpcReqQueue.offer(req);
    }

    // NOTE: query timeout = query queue time + query running time
    private void clearRunningQuery(long currentTime, Map<TNetworkAddress, Long> feProcessIdMap) {
        Iterator<Map.Entry<TUniqueId, AdmissionRequest>> iter = runningQueryMap.entrySet().iterator();
        while (iter.hasNext()) {
            Map.Entry<TUniqueId, AdmissionRequest> entry = iter.next();
            AdmissionRequest request = entry.getValue();
            if (request.isQueryTimeout(currentTime) || request.isQueryFinished()) {
                iter.remove();
                continue;
            }

            if (isFeProcessMissing(request, feProcessIdMap)) {
                iter.remove();
            }
        }
    }

    private void makeWaitingQueryRun(long currentTime, int maxConcurrency, Map<TNetworkAddress, Long> feIdMap,
            List<TAdmissionResponse> responseList) {
        while (runningQueryMap.size() < maxConcurrency) {
            AdmissionRequest request = waitingQueryQueue.poll();
            if (request == null) {
                break;
            }

            if (isFeProcessMissing(request, feIdMap)) {
                continue;
            }

            if (request.isQueueTimeout(currentTime)) {
                continue;
            }

            TAdmissionResponse response;
            if (isWorkloadGroupDropped) {
                response = request.toResponse(TStatusCode.RUNTIME_ERROR,
                        String.format("workload group %d is dropped", this.wgId));
            } else {
                runningQueryMap.put(request.getQueryId(), request);
                response = request.toResponse(TStatusCode.OK);
            }
            responseList.add(response);
        }
    }

    private void clearWaitingQuery(long currentTime, int maxQueueSize, Map<TNetworkAddress, Long> feIdMap,
            List<TAdmissionResponse> responseList) {
        Iterator<AdmissionRequest> iter = waitingQueryQueue.iterator();
        int currentWaitingQueryNum = 0;
        while (iter.hasNext()) {
            AdmissionRequest request = iter.next();

            if (request.isQueueTimeout(currentTime)) {
                iter.remove();
            }

            if (isFeProcessMissing(request, feIdMap)) {
                iter.remove();
            }

            if (isWorkloadGroupDropped) {
                String errorMsg = String.format("workload group %d is dropped", this.wgId);
                TAdmissionResponse response = request.toResponse(TStatusCode.RUNTIME_ERROR, errorMsg);
                responseList.add(response);
            } else if (currentWaitingQueryNum > maxQueueSize) {
                String errorMsg = String.format("workload group %d queue size exceeds %d", this.wgId,
                        this.maxQueueSize);
                TAdmissionResponse response = request.toResponse(TStatusCode.RUNTIME_ERROR, errorMsg);
                responseList.add(response);
            } else {
                currentWaitingQueryNum++;
            }
        }
    }

    private Queue<AdmissionRequest> getRpcRequestCopyList() {
        Queue<AdmissionRequest> retRequestList = rpcReqQueue;
        rpcReqQueue = new LinkedList();
        return retRequestList;
    }

    private void addCurrentRpcReqToWaitingQueue() {
        Queue<AdmissionRequest> currentRpcReqList = getRpcRequestCopyList();
        waitingQueryQueue.addAll(currentRpcReqList);
    }

    // NOTE: this method should be no exception
    // 1  the situation which not send response:
    //      1 query timeout
    //      2 query queue timeout
    //  because client fe could handle timeout on its own.
    // 2 the situation which should send response:
    //      1 queue size exceeds max limit
    //      2 query is allowed to execute
    public List<TAdmissionResponse> execute() {
        // todo: handle it when workload group id dropped
        WorkloadGroup wg = Env.getCurrentEnv().getWorkloadGroupMgr().getWorkloadGroupById(wgId);
        if (wg != null) {
            this.maxConcurrency = wg.getMaxConcurrency();
            this.maxQueueSize = wg.getMaxQueueSize();
        } else {
            // NOTE: isWorkloadGroupDropped should be only set once.
            // so that request with dropped workload group can be rejected
            // we should wait running query to finish.
            isWorkloadGroupDropped = true;
        }
        int maxConcurrency = this.maxConcurrency;
        int maxQueueSize = this.maxQueueSize;
        Map<TNetworkAddress, Long> feProcessIdMap = getFrontendInfoMap();

        long currentTime = System.currentTimeMillis();
        List<TAdmissionResponse> responseList = new ArrayList();

        addCurrentRpcReqToWaitingQueue();
        clearRunningQuery(currentTime, feProcessIdMap);
        makeWaitingQueryRun(currentTime, maxConcurrency, feProcessIdMap, responseList);
        clearWaitingQuery(currentTime, maxQueueSize, feProcessIdMap, responseList);

        return responseList;
    }

    Map<TNetworkAddress, Long> getFrontendInfoMap() {
        List<TFrontendInfo> feInfoList = Env.getCurrentEnv().getFrontendInfos();
        Map<TNetworkAddress, Long> feIdMap = new HashMap();
        for (TFrontendInfo feInfo : feInfoList) {
            feIdMap.put(feInfo.coordinator_address, feInfo.process_uuid);
        }
        return feIdMap;
    }

    boolean isFeProcessMissing(AdmissionRequest request, Map<TNetworkAddress, Long> feIdMap) {
        Long currentFeId = feIdMap.get(request.getClientFeAddr());
        return currentFeId == null || currentFeId != request.getFeProcessId();
    }

    public boolean canRelease() {
        return rpcReqQueue.size() == 0 && waitingQueryQueue.size() == 0 && runningQueryMap.size() == 0;
    }

    public boolean isRpcRequestQueueEmpty() {
        return rpcReqQueue.isEmpty();
    }

    public void markQueryFinished(TUniqueId queryId) {
        AdmissionRequest request = runningQueryMap.get(queryId);
        if (request != null) {
            request.setQueryFinished();
        }
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
