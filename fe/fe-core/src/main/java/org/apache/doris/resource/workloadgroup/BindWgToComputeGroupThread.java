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

import org.apache.doris.analysis.GrantStmt;
import org.apache.doris.analysis.RevokeStmt;
import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.analysis.WorkloadGroupPattern;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.FeConstants;
import org.apache.doris.common.Pair;
import org.apache.doris.ha.FrontendNodeType;
import org.apache.doris.mysql.privilege.PrivBitSet;
import org.apache.doris.mysql.privilege.Privilege;
import org.apache.doris.mysql.privilege.Role;
import org.apache.doris.resource.computegroup.ComputeGroup;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class BindWgToComputeGroupThread extends Thread {

    private static final Logger LOG = LogManager.getLogger(BindWgToComputeGroupThread.class);

    public BindWgToComputeGroupThread() {
        super("BindWgToComputeGroupThread");
    }

    public void run() {
        if (!FeConstants.bindWgToComputeGroup) {
            return;
        }
        try {
            waitCatalogReady();
            createWorkloadGroupForComputeGroup();
            grantAuthToNewWorkloadGroup();
        } catch (Throwable t) {
            LOG.info("[init_wg]Error happens when drop old workload group, ", t);
        }
    }

    private void waitCatalogReady() throws InterruptedException {
        boolean isReady = false;
        while (!isReady) {
            FrontendNodeType feType = Env.getCurrentEnv().getFeType();
            isReady = feType.equals(FrontendNodeType.INIT) || feType.equals(FrontendNodeType.UNKNOWN);
            if (isReady) {
                LOG.info("[init_wg]FE is ready");
                break;
            } else {
                LOG.info("[init_wg]FE is not ready, just wait.");
                Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            }
        }
        Thread.currentThread().join(Config.resource_not_ready_sleep_seconds * 1000L);
    }

    private void createWorkloadGroupForComputeGroup() throws InterruptedException {
        WorkloadGroupMgr wgMgr = Env.getCurrentEnv().getWorkloadGroupMgr();
        wgMgr.tryCreateNormalWorkloadGroup();

        LOG.info("[init_wg] print current cg before, id map:{}, name map : {}",
                wgMgr.getIdToWorkloadGroup(),
                wgMgr.getNameToWorkloadGroup());
        List<WorkloadGroup> oldWgList = wgMgr.getOldWorkloadGroup();
        if (oldWgList.isEmpty()) {
            LOG.info("[init_wg]There is no old workload group, just return.");
            return;
        }

        ComputeGroup allComputeGroup = Env.getCurrentEnv().getComputeGroupMgr().getAllBackendComputeGroup();
        Set<String> cgIdents = allComputeGroup.getIdentifiers();
        while (cgIdents.size() == 0) {
            LOG.info("[init_wg]Not get any backends, sleep");
            Thread.sleep(Config.resource_not_ready_sleep_seconds * 1000);
            cgIdents = allComputeGroup.getIdentifiers();
        }
        LOG.info("[init_wg]Get cgs from backend, {}", String.join(",", cgIdents));
        for (WorkloadGroup wg : oldWgList) {
            wgMgr.bindWorkloadGroupToComputeGroup(allComputeGroup.getIdentifiers(), wg);
        }
        LOG.info(
                "[init_wg]Finish bing workload group to compute group, wg size: {}, cg size: {}, "
                        + "id map:{}, name map :{}",
                oldWgList.size(), allComputeGroup.getIdentifiers(),
                wgMgr.getIdToWorkloadGroup(),
                wgMgr.getNameToWorkloadGroup());
    }

    private void grantAuthToNewWorkloadGroup() throws DdlException {
        Map<String, Role> roleMap = Env.getCurrentEnv().getAuth().getCopiedRoles();
        // <roleName, <wgName, <priv>>>
        List<Pair<String, Pair<String, Set<Privilege>>>> roleAuthList = Lists.newArrayList();

        // 1 get priv list
        for (Map.Entry<String, Role> entry : roleMap.entrySet()) {
            String roleName = entry.getKey();
            Role role = entry.getValue();

            Map<WorkloadGroupPattern, PrivBitSet> wgPrivMap = role.getWorkloadGroupPatternToPrivs();
            if (wgPrivMap.isEmpty()) {
                LOG.info("skip empty privs, role={}.", role);
                continue;
            }

            Set<UserIdentity> uidSet = Env.getCurrentEnv().getAuth().getRoleUsers(role.getRoleName());
            Optional<UserIdentity> uid = uidSet.stream().findFirst();

            if (uidSet.size() == 1 && uid.isPresent() && uid.get().isSystemUser()) {
                LOG.info("skip sys user, user={} , role={}.", uid, role);
                continue;
            }

            LOG.info("begin deal role={}", role);

            for (Map.Entry<WorkloadGroupPattern, PrivBitSet> wgPrivEntry : wgPrivMap.entrySet()) {
                String wgPrivName = wgPrivEntry.getKey().getworkloadGroupName();
                // skip new auth data or %
                if (wgPrivName.contains(".")) {
                    LOG.info("skip new workload group, role={}, wg={}", roleName, wgPrivName);
                    continue;
                }

                if (wgPrivName.contains("%")) {
                    LOG.info("skip % workload group, role={}", roleName);
                    continue;
                }

                List<Privilege> privList = wgPrivEntry.getValue().toPrivilegeList();
                if (privList.size() > 0) {
                    roleAuthList.add(Pair.of(roleName, Pair.of(wgPrivName, Sets.newHashSet(privList))));
                    LOG.info("get priv list for role={}, priv={}", roleName, privList);
                } else {
                    LOG.info("not find priv list for role={}, ", roleName);
                }
            }
        }

        // 2 grant priv for new workload group
        Map<String, List<String>> wgNameMap = Env.getCurrentEnv().getWorkloadGroupMgr().getWorkloadGroupNameMap();
        for (Pair<String, Pair<String, Set<Privilege>>> role : roleAuthList) {
            String roleName = role.first;
            String oldWgName = role.second.first;
            Set<Privilege> privileges = role.second.second;

            List<String> newWgNameList = wgNameMap.get(oldWgName);
            for (String newWgName : newWgNameList) {
                WorkloadGroupPattern wgPattern = new WorkloadGroupPattern(newWgName);
                GrantStmt grantStmt = new GrantStmt(null, roleName, wgPattern, null);
                grantStmt.set(privileges);
                Env.getCurrentEnv().getAuth().grant(grantStmt);
                LOG.info("grant roleName {} to new wg {}, priv list {}", roleName, newWgName, privileges);
            }

            RevokeStmt revokeOldWgStmt = new RevokeStmt(null, roleName, new WorkloadGroupPattern(oldWgName), null);
            revokeOldWgStmt.setPrivileges(privileges);
            LOG.info("revoke roleName {} from old wg {}, priv list {}", roleName, oldWgName, privileges);
            Env.getCurrentEnv().getAuth().revoke(revokeOldWgStmt);
        }
    }

}
