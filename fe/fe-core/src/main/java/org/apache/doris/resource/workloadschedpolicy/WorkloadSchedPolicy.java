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

import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.io.Writable;
import org.apache.doris.persist.gson.GsonPostProcessable;
import org.apache.doris.persist.gson.GsonUtils;

import com.esotericsoftware.minlog.Log;
import com.google.common.collect.ImmutableSet;
import com.google.gson.annotations.SerializedName;

import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class WorkloadSchedPolicy implements Writable, GsonPostProcessable {

    public static final String ENABLED = "enabled";
    public static final String PRIORITY = "priority";
    public static final ImmutableSet<String> POLICY_PROPERTIES = new ImmutableSet.Builder<String>()
            .add(ENABLED).add(PRIORITY).build();

    @SerializedName(value = "id")
    long id;
    @SerializedName(value = "name")
    String name;
    @SerializedName(value = "version")
    int version;

    @SerializedName(value = "enabled")
    private volatile boolean enabled;
    @SerializedName(value = "priority")
    private volatile int priority;

    @SerializedName(value = "conditionMetaList")
    List<ConditionMeta> conditionMetaList;
    // we regard action as a command, map's key is command, map's value is args, so it's a command list
    @SerializedName(value = "actionMetaList")
    Map<String, String> actionMetaList;

    private List<PolicyCondition> policyConditionList;
    private List<PolicyAction> policyActionList;

    public WorkloadSchedPolicy(long id, String name, List<PolicyCondition> policyConditionList,
            List<PolicyAction> policyActionList, Map<String, String> properties) throws UserException {
        this.id = id;
        this.name = name;
        this.policyConditionList = policyConditionList;
        this.policyActionList = policyActionList;
        // set enable and priority
        parseAndSetProperties(properties);
        this.version = 0;
    }

    // return true, this means all conditions in policy can match queryInfo's data
    // return false,
    //    1 metric not match
    //    2 condition value not match query info's value
    boolean isMatch(PolicyQueryInfo queryInfo) {
        boolean ret = true;
        for (PolicyCondition condition : policyConditionList) {
            PolicyMetricType metricType = condition.getMetricType();
            String value = queryInfo.metricMap.get(metricType);
            if (value == null) {
                return false; // query info's metric must match all condition's metric
            }
            ret = ret & condition.eval(value);
            if (!ret) {
                return ret;
            }
        }
        return ret;
    }

    public boolean isEnabled() {
        return enabled;
    }

    public int getPriority() {
        return priority;
    }

    public void execAction(PolicyQueryInfo queryInfo) {
        for (PolicyAction action : policyActionList) {
            action.exec(queryInfo);
        }
    }

    // move > log, cancel > log
    // move and cancel can not exist at same time
    public PolicyActionType getFirstActionType() {
        PolicyActionType retType = null;
        for (PolicyAction action : policyActionList) {
            PolicyActionType currentActionType = action.getPolicyActionType();
            if (retType == null) {
                retType = currentActionType;
                continue;
            }

            if (currentActionType == PolicyActionType.move_query_to_group
                    || currentActionType == PolicyActionType.cancel_query) {
                return currentActionType;
            }
        }
        return retType;
    }

    public void parseAndSetProperties(Map<String, String> properties) throws UserException {
        String enabledStr = properties.get(ENABLED);
        this.enabled = enabledStr == null ? true : Boolean.parseBoolean(enabledStr);

        String priorityStr = properties.get(PRIORITY);
        this.priority = priorityStr == null ? 0 : Integer.parseInt(priorityStr);
    }

    void incrementVersion() {
        this.version++;
    }

    public void setConditionMeta(List<ConditionMeta> conditionMeta) {
        this.conditionMetaList = conditionMeta;
    }

    public void setActionMeta(Map<String, String> actionMeta) {
        this.actionMetaList = actionMeta;
    }

    public long getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        String json = GsonUtils.GSON.toJson(this);
        Text.writeString(out, json);
    }

    @Override
    public void gsonPostProcess() throws IOException {
        List<PolicyCondition> policyConditionList = new ArrayList<>();
        for (ConditionMeta cm : conditionMetaList) {
            try {
                PolicyCondition cond = PolicyCondition.createPolicyCondition(cm.metricName, cm.op,
                        cm.value);
                policyConditionList.add(cond);
            } catch (UserException ue) {
                Log.error("unexpected condition data error when replay log ", ue);
            }
        }
        this.policyConditionList = policyConditionList;

        List<PolicyAction> actionList = new ArrayList<>();
        for (Map.Entry<String, String> entry : actionMetaList.entrySet()) {
            try {
                PolicyAction ret = PolicyAction.createPolicyAction(entry.getKey(), entry.getValue());
                actionList.add(ret);
            } catch (UserException ue) {
                Log.error("unexpected action data error when replay log ", ue);
            }
        }
        this.policyActionList = actionList;

    }
}
