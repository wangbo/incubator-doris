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

package org.apache.doris.load.loadv2;

import org.apache.doris.analysis.DataDescription;
import org.apache.doris.analysis.DataProcessorDesc;
import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.analysis.LabelName;
import org.apache.doris.analysis.LoadStmt;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EtlClusterMgr;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.SparkEtlCluster;
import org.apache.doris.catalog.SparkEtlCluster.DeployMode;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.jmockit.Deencapsulation;
import org.apache.doris.load.EtlJobType;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.task.MasterTaskExecutor;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.GlobalTransactionMgr;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Map;

public class SparkLoadJobTest {
    private long loadJobId;
    private long dbId;
    private String dbName;
    private String tableName;
    private String label;
    private String clusterName;
    private String broker;
    private long transactionId;
    private long pendingTaskId;

    @Before
    public void setUp() {
        loadJobId = 0L;
        dbId = 1L;
        dbName = "database0";
        tableName = "table0";
        label = "label0";
        clusterName = "cluster0";
        broker = "broker0";
        transactionId = 2L;
        pendingTaskId = 3L;
    }

    @Test
    public void testCreateFromLoadStmt(@Mocked Catalog catalog, @Injectable LoadStmt loadStmt,
                                       @Injectable DataDescription dataDescription, @Injectable LabelName labelName,
                                       @Injectable Database db, @Injectable OlapTable olapTable,
                                       @Injectable String originStmt, @Injectable EtlClusterMgr etlClusterMgr) {
        List<DataDescription> dataDescriptionList = Lists.newArrayList();
        dataDescriptionList.add(dataDescription);
        Map<String, String> clusterProperties = Maps.newHashMap();
        clusterProperties.put("spark_configs", "spark.executor.memory=1g");
        clusterProperties.put("broker", broker);
        clusterProperties.put("broker.username", "user0");
        clusterProperties.put("broker.password", "password0");
        DataProcessorDesc dataProcessorDesc = new EtlClusterDesc(clusterName, clusterProperties);
        Map<String, String> jobProperties = Maps.newHashMap();
        String hiveTable = "hivedb.table0";
        jobProperties.put("bitmap_data", hiveTable);
        SparkEtlCluster etlCluster = new SparkEtlCluster(clusterName);

        new Expectations() {
            {
                catalog.getDb(dbName);
                result = db;
                catalog.getEtlClusterMgr();
                result = etlClusterMgr;
                db.getTable(tableName);
                result = olapTable;
                db.getId();
                result = dbId;
                loadStmt.getLabel();
                result = labelName;
                loadStmt.getDataDescriptions();
                result = dataDescriptionList;
                loadStmt.getDataProcessorDesc();
                result = dataProcessorDesc;
                loadStmt.getProperties();
                result = jobProperties;
                labelName.getDbName();
                result = dbName;
                labelName.getLabelName();
                result = label;
                dataDescription.getTableName();
                result = tableName;
                dataDescription.getPartitionNames();
                result = null;
                etlClusterMgr.getEtlCluster(clusterName);
                result = etlCluster;
            }
        };

        try {
            Assert.assertTrue(etlCluster.getSparkConfigsMap().isEmpty());
            BulkLoadJob bulkLoadJob = BulkLoadJob.fromLoadStmt(loadStmt, new OriginStatement(originStmt, 0));
            SparkLoadJob sparkLoadJob = (SparkLoadJob) bulkLoadJob;
            // check member
            Assert.assertEquals(dbId, (long) Deencapsulation.getField(bulkLoadJob, "dbId"));
            Assert.assertEquals(label, Deencapsulation.getField(bulkLoadJob, "label"));
            Assert.assertEquals(JobState.PENDING, Deencapsulation.getField(bulkLoadJob, "state"));
            Assert.assertEquals(EtlJobType.SPARK, Deencapsulation.getField(bulkLoadJob, "jobType"));
            Assert.assertEquals(clusterName, sparkLoadJob.getEtlClusterName());
            Assert.assertEquals(-1L, sparkLoadJob.getEtlStartTimestamp());
            Assert.assertEquals(hiveTable, sparkLoadJob.getHiveTableName());

            // check update etl cluster properties
            Assert.assertEquals(broker, bulkLoadJob.brokerDesc.getName());
            Assert.assertEquals("user0", bulkLoadJob.brokerDesc.getProperties().get("username"));
            Assert.assertEquals("password0", bulkLoadJob.brokerDesc.getProperties().get("password"));
            SparkEtlCluster sparkEtlCluster = Deencapsulation.getField(sparkLoadJob, "etlCluster");
            Assert.assertTrue(sparkEtlCluster.getSparkConfigsMap().containsKey("spark.executor.memory"));
            Assert.assertEquals("1g", sparkEtlCluster.getSparkConfigsMap().get("spark.executor.memory"));
        } catch (DdlException e) {
            Assert.fail(e.getMessage());
        }
    }

    /*
    @Test
    public void testExecute(@Mocked Catalog catalog, @Injectable String originStmt,
                            @Injectable GlobalTransactionMgr transactionMgr, @Injectable SparkLoadPendingTask pendingTask,
                            @Injectable TxnCoordinator txnCoordinator, @Injectable MasterTaskExecutor executor)
            throws MetaNotFoundException, LabelAlreadyUsedException, AnalysisException, LoadException,
                    BeginTransactionException, DuplicatedRequestException {
        EtlClusterDesc etlClusterDesc = new EtlClusterDesc(clusterName, Maps.newHashMap());
        SparkLoadJob job = new SparkLoadJob(dbId, label, etlClusterDesc, new OriginStatement(originStmt, 0));

        new Expectations() {
            {
                Catalog.getCurrentGlobalTransactionMgr();
                result = transactionMgr;
                transactionMgr.beginTransaction(dbId, Lists.newArrayList(), label, null, txnCoordinator,
                                                LoadJobSourceType.FRONTEND, loadJobId, 3600);
                result = transactionId;
                pendingTask.getSignature();
                result = pendingTaskId;
                pendingTask.init();
                catalog.getLoadTaskScheduler();
                result = executor;
                executor.submit(pendingTask);
            }
        };

        job.execute();
        Assert.assertEquals(transactionId, job.getTransactionId());
    }
    */
}
