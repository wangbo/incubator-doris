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

package org.apache.doris.resource.resourcegroup;


import org.apache.doris.analysis.AlterWorkloadGroupStmt;
import org.apache.doris.analysis.CreateWorkloadGroupStmt;
import org.apache.doris.analysis.DropWorkloadGroupStmt;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.UserException;
import org.apache.doris.persist.EditLog;
import org.apache.doris.thrift.TPipelineResourceGroup;
import org.apache.doris.workload.WorkloadGroup;
import org.apache.doris.workload.WorkloadGroupMgr;

import com.amazonaws.services.wellarchitected.model.Workload;
import com.google.common.collect.Maps;
import mockit.Delegate;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class ResourceGroupMgrTest {

    @Injectable
    private EditLog editLog;

    @Mocked
    private Env env;

    private AtomicLong id = new AtomicLong(10);

    @Before
    public void setUp() throws DdlException {
        new Expectations() {
            {
                env.getEditLog();
                minTimes = 0;
                result = editLog;

                env.getNextId();
                minTimes = 0;
                result = new Delegate() {
                    long delegate() {
                        return id.addAndGet(1);
                    }
                };

                editLog.logCreateWorkloadGroup((WorkloadGroup) any);
                minTimes = 0;

                Env.getCurrentEnv();
                minTimes = 0;
                result = env;
            }
        };
    }

    @Test
    public void testCreateResourceGroup() throws DdlException {
        Config.enable_workload_group = true;
        WorkloadGroupMgr resourceGroupMgr = new WorkloadGroupMgr();
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.CPU_SHARE, "10");
        properties1.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name1 = "g1";
        CreateWorkloadGroupStmt stmt1 = new CreateWorkloadGroupStmt(false, name1, properties1);
        resourceGroupMgr.createWorkloadGroup(stmt1);

        Map<String, WorkloadGroup> nameToRG = resourceGroupMgr.getNameToWorkloadGroup();
        Assert.assertEquals(1, nameToRG.size());
        Assert.assertTrue(nameToRG.containsKey(name1));
        WorkloadGroup group1 = nameToRG.get(name1);
        Assert.assertEquals(name1, group1.getName());

        Map<Long, WorkloadGroup> idToRG = resourceGroupMgr.getIdToWorkloadGroup();
        Assert.assertEquals(1, idToRG.size());
        Assert.assertTrue(idToRG.containsKey(group1.getId()));

        Map<String, String> properties2 = Maps.newHashMap();
        properties2.put(WorkloadGroup.CPU_SHARE, "20");
        properties2.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name2 = "g2";
        CreateWorkloadGroupStmt stmt2 = new CreateWorkloadGroupStmt(false, name2, properties2);
        resourceGroupMgr.createWorkloadGroup(stmt2);

        nameToRG = resourceGroupMgr.getNameToWorkloadGroup();
        Assert.assertEquals(2, nameToRG.size());
        Assert.assertTrue(nameToRG.containsKey(name2));
        WorkloadGroup group2 = nameToRG.get(name2);
        idToRG = resourceGroupMgr.getIdToWorkloadGroup();
        Assert.assertEquals(2, idToRG.size());
        Assert.assertTrue(idToRG.containsKey(group2.getId()));

        try {
            resourceGroupMgr.createWorkloadGroup(stmt2);
            Assert.fail();
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("already exist"));
        }

        CreateWorkloadGroupStmt stmt3 = new CreateWorkloadGroupStmt(true, name2, properties2);
        resourceGroupMgr.createWorkloadGroup(stmt3);
        Assert.assertEquals(2, resourceGroupMgr.getIdToWorkloadGroup().size());
        Assert.assertEquals(2, resourceGroupMgr.getNameToWorkloadGroup().size());
    }

    @Test
    public void testGetResourceGroup() throws UserException {
        Config.enable_workload_group = true;
        WorkloadGroupMgr resourceGroupMgr = new WorkloadGroupMgr();
        Map<String, String> properties1 = Maps.newHashMap();
        properties1.put(WorkloadGroup.CPU_SHARE, "10");
        properties1.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name1 = "g1";
        CreateWorkloadGroupStmt stmt1 = new CreateWorkloadGroupStmt(false, name1, properties1);
        resourceGroupMgr.createWorkloadGroup(stmt1);
        List<TPipelineResourceGroup> tResourceGroups1 = resourceGroupMgr.getWorkloadGroup(name1);
        Assert.assertEquals(1, tResourceGroups1.size());
        TPipelineResourceGroup tResourceGroup1 = tResourceGroups1.get(0);
        Assert.assertEquals(name1, tResourceGroup1.getName());
        Assert.assertTrue(tResourceGroup1.getProperties().containsKey(WorkloadGroup.CPU_SHARE));

        try {
            resourceGroupMgr.getWorkloadGroup("g2");
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testDropResourceGroup() throws UserException {
        Config.enable_workload_group = true;
        WorkloadGroupMgr resourceGroupMgr = new WorkloadGroupMgr();
        Map<String, String> properties = Maps.newHashMap();
        properties.put(WorkloadGroup.CPU_SHARE, "10");
        properties.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        String name = "g1";
        CreateWorkloadGroupStmt createStmt = new CreateWorkloadGroupStmt(false, name, properties);
        resourceGroupMgr.createWorkloadGroup(createStmt);
        Assert.assertEquals(1, resourceGroupMgr.getWorkloadGroup(name).size());

        DropWorkloadGroupStmt dropStmt = new DropWorkloadGroupStmt(false, name);
        resourceGroupMgr.dropWorkloadGroup(dropStmt);
        try {
            resourceGroupMgr.getWorkloadGroup(name);
            Assert.fail();
        } catch (UserException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }

        DropWorkloadGroupStmt dropDefaultStmt = new DropWorkloadGroupStmt(false, WorkloadGroupMgr.DEFAULT_GROUP_NAME);
        try {
            resourceGroupMgr.dropWorkloadGroup(dropDefaultStmt);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("is not allowed"));
        }
    }

    @Test
    public void testAlterResourceGroup() throws UserException {
        Config.enable_workload_group = true;
        WorkloadGroupMgr resourceGroupMgr = new WorkloadGroupMgr();
        Map<String, String> properties = Maps.newHashMap();
        String name = "g1";
        try {
            AlterWorkloadGroupStmt stmt1 = new AlterWorkloadGroupStmt(name, properties);
            resourceGroupMgr.alterWorkloadGroup(stmt1);
        } catch (DdlException e) {
            Assert.assertTrue(e.getMessage().contains("does not exist"));
        }

        properties.put(WorkloadGroup.CPU_SHARE, "10");
        properties.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        CreateWorkloadGroupStmt createStmt = new CreateWorkloadGroupStmt(false, name, properties);
        resourceGroupMgr.createWorkloadGroup(createStmt);

        Map<String, String> newProperties = Maps.newHashMap();
        newProperties.put(WorkloadGroup.CPU_SHARE, "5");
        newProperties.put(WorkloadGroup.MEMORY_LIMIT, "30%");
        AlterWorkloadGroupStmt stmt2 = new AlterWorkloadGroupStmt(name, newProperties);
        resourceGroupMgr.alterWorkloadGroup(stmt2);

        List<TPipelineResourceGroup> tResourceGroups = resourceGroupMgr.getWorkloadGroup(name);
        Assert.assertEquals(1, tResourceGroups.size());
        TPipelineResourceGroup tResourceGroup1 = tResourceGroups.get(0);
        Assert.assertEquals(tResourceGroup1.getProperties().get(WorkloadGroup.CPU_SHARE), "5");
    }
}
