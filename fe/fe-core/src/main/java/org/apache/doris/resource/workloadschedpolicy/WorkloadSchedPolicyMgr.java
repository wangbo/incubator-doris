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

package org.apache.doris.resource.workloadschedpolicy;

import org.apache.doris.analysis.AlterWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.CreateWorkloadSchedPolicyStmt;
import org.apache.doris.analysis.DropWorkloadSchedPolicyStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.common.util.DebugUtil;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.service.ExecuteEnv;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.gson.annotations.SerializedName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WorkloadSchedPolicyMgr implements Writable, GsonPostProcessable {

    private static final Logger LOG = LogManager.getLogger(WorkloadSchedPolicyMgr.class);

    @SerializedName(value = "idToPolicy")
    private Map<Long, WorkloadSchedPolicy> idToPolicy = Maps.newConcurrentMap();
    private Map<String, WorkloadSchedPolicy> nameToPolicy = Maps.newHashMap();

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

    public static Comparator<WorkloadSchedPolicy> policyComparator = new Comparator<WorkloadSchedPolicy>() {
        @Override
        public int compare(WorkloadSchedPolicy p1, WorkloadSchedPolicy p2) {
            return p2.getPriority() - p1.getPriority();
        }
    };

    private Thread policyExecThread = new Thread() {
        @Override
        public void run() {
            while (true) {
                try {
                    // 1 get query info map
                    Map<Integer, ConnectContext> connectMap = ExecuteEnv.getInstance().getScheduler()
                            .getConnectionMap();
                    List<WorkloadQueryInfo> queryInfoList = new ArrayList<>();

                    // a snapshot for connect context
                    Set<Integer> keySet = new HashSet<>();
                    keySet.addAll(connectMap.keySet());

                    for (Integer connectId : keySet) {
                        ConnectContext cctx = connectMap.get(connectId);
                        if (cctx == null) {
                            continue;
                        }

                        String username = cctx.getQualifiedUser();
                        long queryTime = System.currentTimeMillis() - cctx.getStartTime();

                        WorkloadQueryInfo policyQueryInfo = new WorkloadQueryInfo();
                        policyQueryInfo.queryId = DebugUtil.printId(cctx.queryId());
                        policyQueryInfo.tUniqueId = cctx.queryId();
                        policyQueryInfo.context = cctx;
                        policyQueryInfo.metricMap = new HashMap<>();
                        policyQueryInfo.metricMap.put(WorkloadMetricType.username, username);
                        policyQueryInfo.metricMap.put(WorkloadMetricType.query_time, String.valueOf(queryTime));

                        queryInfoList.add(policyQueryInfo);
                    }

                    // 2 exec policy
                    if (queryInfoList.size() > 0) {
                        execPolicy(queryInfoList);
                    }
                } catch (Throwable t) {
                    LOG.error("[policy thread]error happens when exec policy");
                }

                // 3 sleep
                try {
                    Thread.sleep(Config.workload_sched_policy_interval_ms);
                } catch (InterruptedException e) {
                    LOG.error("error happends when policy exec thread sleep");
                }
            }
        }
    };

    public void start() {
        policyExecThread.start();
    }

    public void createWorkloadSchedPolicy(CreateWorkloadSchedPolicyStmt createStmt) throws UserException {
        // 0 check name
        String policyName = createStmt.getPolicyName();

        // 1 create condition
        List<WorkloadConditionMeta> originConditions = createStmt.getConditions();
        List<WorkloadCondition> policyConditionList = new ArrayList<>();
        for (WorkloadConditionMeta cm : originConditions) {
            WorkloadCondition cond = WorkloadCondition.createWorkloadCondition(cm.metricName, cm.op,
                    cm.value);
            policyConditionList.add(cond);
        }

        // 2 create action
        Map<String, String> originActions = createStmt.getActions();
        List<WorkloadAction> policyActionList = new ArrayList<>();
        for (Map.Entry<String, String> actionEntry : originActions.entrySet()) {
            String actionName = actionEntry.getKey();
            String actionArgs = actionEntry.getValue();

            // we need convert wgName to wgId, because wgName may change
            if (WorkloadActionType.move_query_to_group.toString().equals(actionName)) {
                Long wgId = Env.getCurrentEnv().getWorkloadGroupMgr().getWorkloadGroupIdByName(actionArgs);
                if (wgId == null) {
                    throw new UserException(
                            "can not find workload group " + actionArgs + " when set workload sched policy");
                }
                actionEntry.setValue(wgId.toString());
            }

            WorkloadAction ret = WorkloadAction.createWorkloadAction(actionEntry.getKey(), actionEntry.getValue());
            policyActionList.add(ret);
        }
        checkPolicyActionConflicts(policyActionList);

        // 3 create policy
        Map<String, String> propMap = createStmt.getProperties();
        checkProperties(propMap);
        writeLock();
        try {
            if (nameToPolicy.containsKey(createStmt.getPolicyName())) {
                if (createStmt.isIfNotExists()) {
                    return;
                } else {
                    throw new UserException("workload schedule policy " + policyName + " already exists ");
                }
            }
            long id = Env.getCurrentEnv().getNextId();
            WorkloadSchedPolicy policy = new WorkloadSchedPolicy(id, policyName,
                    policyConditionList, policyActionList, propMap);
            policy.setConditionMeta(originConditions);
            policy.setActionMeta(originActions);
            Env.getCurrentEnv().getEditLog().logCreateWorkloadSchedPolicy(policy);
            idToPolicy.put(id, policy);
            nameToPolicy.put(policyName, policy);
        } finally {
            writeUnlock();
        }
    }

    private void checkPolicyActionConflicts(List<WorkloadAction> actionList) throws UserException {
        Set<WorkloadActionType> actionTypeSet = new HashSet<>();
        for (WorkloadAction action : actionList) {
            if (!actionTypeSet.add(action.getWorkloadActionType())) {
                throw new UserException("duplicate action in one policy");
            }
        }
        if (actionTypeSet.contains(WorkloadActionType.cancel_query) && actionTypeSet.contains(
                WorkloadActionType.move_query_to_group)) {
            throw new UserException(String.format("%s and %s can not exist in one policy at same time",
                    WorkloadActionType.cancel_query, WorkloadActionType.move_query_to_group));
        }
    }

    public void execPolicy(List<WorkloadQueryInfo> queryInfoList) {
        // 1 get a snapshot of policy
        Set<Long> policyIdSet = new HashSet<>();
        readLock();
        try {
            policyIdSet.addAll(idToPolicy.keySet());
        } finally {
            readUnlock();
        }

        for (WorkloadQueryInfo queryInfo : queryInfoList) {
            try {
                // 1 check policy is match
                Map<WorkloadActionType, Queue<WorkloadSchedPolicy>> matchedPolicyMap = Maps.newHashMap();
                for (Long policyId : policyIdSet) {
                    WorkloadSchedPolicy policy = idToPolicy.get(policyId);
                    if (policy == null) {
                        continue;
                    }
                    if (policy.isEnabled() && policy.isMatch(queryInfo)) {
                        WorkloadActionType actionType = policy.getFirstActionType();
                        // add to priority queue
                        Queue<WorkloadSchedPolicy> queue = matchedPolicyMap.get(actionType);
                        if (queue == null) {
                            queue = new PriorityQueue<>(policyComparator);
                            matchedPolicyMap.put(actionType, queue);
                        }
                        queue.offer(policy);
                    }
                }

                if (matchedPolicyMap.size() == 0) {
                    continue;
                }

                // 2 pick higher priority policy when action conflicts
                List<WorkloadSchedPolicy> pickedPolicyList = pickPolicy(matchedPolicyMap);

                // 3 exec action
                for (WorkloadSchedPolicy policy : pickedPolicyList) {
                    policy.execAction(queryInfo);
                }
            } catch (Throwable e) {
                LOG.warn("exec policy with query {} failed ", queryInfo.queryId, e);
            }
        }
    }

    List<WorkloadSchedPolicy> pickPolicy(Map<WorkloadActionType, Queue<WorkloadSchedPolicy>> policyMap) {
        // NOTE(wb) currently all action share the same comparator which use priority.
        // But later we may design every action type's own comparator,
        // such as if two move group action has the same priority but move to different group,
        // then we may pick group by resource usage and query statistics.

        // 1 only need one policy with move action which has the highest priority
        WorkloadSchedPolicy policyWithMoveAction = null;
        Queue<WorkloadSchedPolicy> moveActionQueue = policyMap.get(WorkloadActionType.move_query_to_group);
        if (moveActionQueue != null) {
            policyWithMoveAction = moveActionQueue.peek();
        }

        // 2 only need one policy with cancel action which has the highest priority
        WorkloadSchedPolicy policyWithCancelQueryAction = null;
        Queue<WorkloadSchedPolicy> canelQueryActionQueue = policyMap.get(WorkloadActionType.cancel_query);
        if (canelQueryActionQueue != null) {
            policyWithCancelQueryAction = canelQueryActionQueue.peek();
        }

        // 3 compare policy with move action and cancel action
        List<WorkloadSchedPolicy> ret = new ArrayList<>();
        if (policyWithMoveAction != null && policyWithCancelQueryAction != null) {
            if (policyWithMoveAction.getPriority() > policyWithCancelQueryAction.getPriority()) {
                ret.add(policyWithMoveAction);
            } else {
                ret.add(policyWithCancelQueryAction);
            }
        } else {
            if (policyWithCancelQueryAction != null) {
                ret.add(policyWithCancelQueryAction);
            } else if (policyWithMoveAction != null) {
                ret.add(policyWithMoveAction);
            }
        }

        Preconditions.checkArgument(ret.size() > 0, "should pick at least one policy");
        return ret;
    }

    private void checkProperties(Map<String, String> properties) throws UserException {
        Set<String> allInputPropKeySet = new HashSet<>();
        allInputPropKeySet.addAll(properties.keySet());

        allInputPropKeySet.removeAll(WorkloadSchedPolicy.POLICY_PROPERTIES);
        if (allInputPropKeySet.size() > 0) {
            throw new UserException("illegal policy properties " + String.join(",", allInputPropKeySet));
        }

        String enabledStr = properties.get(WorkloadSchedPolicy.ENABLED);
        if (enabledStr != null) {
            if (!"true".equals(enabledStr) && !"false".equals(enabledStr)) {
                throw new UserException("invalid enabled property value, it can only true or false with lower case");
            }
        }

        String priorityStr = properties.get(WorkloadSchedPolicy.PRIORITY);
        if (priorityStr != null) {
            try {
                Long prioLongVal = Long.parseLong(priorityStr);
                if (prioLongVal < 0 || prioLongVal > 100) {
                    throw new UserException("policy's priority can only between 0 ~ 100");
                }
            } catch (NumberFormatException e) {
                throw new UserException("policy's priority must be a number, input value=" + priorityStr);
            }
        }
    }

    public void alterWorkloadSchedPolicy(AlterWorkloadSchedPolicyStmt alterStmt) throws UserException {
        writeLock();
        try {
            String policyName = alterStmt.getPolicyName();
            WorkloadSchedPolicy policy = nameToPolicy.get(policyName);
            if (policy == null) {
                throw new UserException("can not find workload schedule policy " + policyName);
            }

            Map<String, String> properties = alterStmt.getProperties();
            checkProperties(properties);
            policy.parseAndSetProperties(properties);
            policy.incrementVersion();
            Env.getCurrentEnv().getEditLog().logAlterWorkloadSchedPolicy(policy);
        } finally {
            writeUnlock();
        }
    }

    public void dropWorkloadSchedPolicy(DropWorkloadSchedPolicyStmt dropStmt) throws UserException {
        writeLock();
        try {
            String policyName = dropStmt.getPolicyName();
            WorkloadSchedPolicy schedPolicy = nameToPolicy.get(policyName);
            if (schedPolicy == null) {
                if (dropStmt.isIfExists()) {
                    return;
                } else {
                    throw new UserException("workload schedule policy " + policyName + "not exists");
                }
            }

            long id = schedPolicy.getId();
            idToPolicy.remove(id);
            nameToPolicy.remove(policyName);
            Env.getCurrentEnv().getEditLog().dropWorkloadSchedPolicy(id);
        } finally {
            writeUnlock();
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

    public void replayCreateWorkloadSchedPolicy(WorkloadSchedPolicy policy) {
        insertWorkloadSchedPolicy(policy);
    }

    public void replayAlterWorkloadSchedPolicy(WorkloadSchedPolicy policy) {
        insertWorkloadSchedPolicy(policy);
    }

    public void replayDropWorkloadSchedPolicy(long policyId) {
        writeLock();
        try {
            WorkloadSchedPolicy policy = idToPolicy.get(policyId);
            if (policy == null) {
                return;
            }
            idToPolicy.remove(policyId);
            nameToPolicy.remove(policy.getName());
        } finally {
            writeUnlock();
        }
    }

    private void insertWorkloadSchedPolicy(WorkloadSchedPolicy policy) {
        writeLock();
        try {
            idToPolicy.put(policy.getId(), policy);
            nameToPolicy.put(policy.getName(), policy);
        } finally {
            writeUnlock();
        }
    }

    public static WorkloadSchedPolicyMgr read(DataInput in) throws IOException {
        String json = Text.readString(in);
        return GsonUtils.GSON.fromJson(json, WorkloadSchedPolicyMgr.class);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        idToPolicy.forEach(
                (id, schedPolicy) -> nameToPolicy.put(schedPolicy.getName(), schedPolicy));
    }
}
