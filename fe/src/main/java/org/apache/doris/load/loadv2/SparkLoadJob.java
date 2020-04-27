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

import org.apache.doris.analysis.Analyzer;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.analysis.CastExpr;
import org.apache.doris.analysis.DescriptorTable;
import org.apache.doris.analysis.EtlClusterDesc;
import org.apache.doris.analysis.Expr;
import org.apache.doris.analysis.SlotDescriptor;
import org.apache.doris.analysis.SlotRef;
import org.apache.doris.analysis.TupleDescriptor;
import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.Column;
import org.apache.doris.catalog.Database;
import org.apache.doris.catalog.EtlCluster;
import org.apache.doris.catalog.FsBroker;
import org.apache.doris.catalog.MaterializedIndex;
import org.apache.doris.catalog.MaterializedIndex.IndexExtState;
import org.apache.doris.catalog.OlapTable;
import org.apache.doris.catalog.Partition;
import org.apache.doris.catalog.PrimitiveType;
import org.apache.doris.catalog.Replica;
import org.apache.doris.catalog.ScalarType;
import org.apache.doris.catalog.SparkEtlCluster;
import org.apache.doris.catalog.Tablet;
import org.apache.doris.catalog.Type;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.Config;
import org.apache.doris.common.DdlException;
import org.apache.doris.common.DuplicatedRequestException;
import org.apache.doris.common.LabelAlreadyUsedException;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.MetaNotFoundException;
import org.apache.doris.common.Pair;
import org.apache.doris.common.UserException;
import org.apache.doris.common.io.Text;
import org.apache.doris.common.util.LogBuilder;
import org.apache.doris.common.util.LogKey;
import org.apache.doris.load.EtlJobType;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.FailMsg;
import org.apache.doris.load.loadv2.dpp.DppResult;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.planner.PlanNodeId;
import org.apache.doris.planner.ScanNode;
import org.apache.doris.qe.OriginStatement;
import org.apache.doris.service.FrontendOptions;
import org.apache.doris.system.Backend;
import org.apache.doris.task.AgentBatchTask;
import org.apache.doris.task.AgentTaskExecutor;
import org.apache.doris.task.AgentTaskQueue;
import org.apache.doris.task.PushTask;
import org.apache.doris.thrift.TBrokerRangeDesc;
import org.apache.doris.thrift.TBrokerScanRange;
import org.apache.doris.thrift.TBrokerScanRangeParams;
import org.apache.doris.thrift.TDescriptorTable;
import org.apache.doris.thrift.TFileFormatType;
import org.apache.doris.thrift.TFileType;
import org.apache.doris.thrift.TNetworkAddress;
import org.apache.doris.thrift.TPlanNode;
import org.apache.doris.thrift.TPriority;
import org.apache.doris.thrift.TPushType;
import org.apache.doris.thrift.TScanRangeLocations;
import org.apache.doris.thrift.TUniqueId;
import org.apache.doris.transaction.BeginTransactionException;
import org.apache.doris.transaction.TabletCommitInfo;
import org.apache.doris.transaction.TabletQuorumFailedException;
import org.apache.doris.transaction.TransactionState.LoadJobSourceType;
import org.apache.doris.transaction.TransactionState.TxnCoordinator;
import org.apache.doris.transaction.TransactionState.TxnSourceType;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.gson.annotations.SerializedName;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * There are 4 steps in SparkLoadJob:
 * Step1: SparkLoadPendingTask will be created by unprotectedExecuteJob method and submit spark etl job.
 * Step2: LoadEtlChecker will check spark etl job status periodly and submit push tasks when spark etl job is finished.
 * Step3: LoadLoadingChecker will check loading status periodly and commit transaction when push tasks are finished.
 * Step4: CommitTxn will be called by updateLoadingStatus method when push tasks are finished.
 */
public class SparkLoadJob extends BulkLoadJob {
    private static final Logger LOG = LogManager.getLogger(SparkLoadJob.class);

    // for global dict
    public static final String BITMAP_DATA_PROPERTY = "bitmap_data";

    // --- members below need persist ---
    // create from etlClusterDesc when job created
    private SparkEtlCluster etlCluster;
    // members below updated when job state changed to etl
    private long etlStartTimestamp = -1;
    // for spark yarn
    private String appId = "";
    // spark job outputPath
    private String etlOutputPath = "";
    // members below updated when job state changed to loading
    // { tableId.partitionId.indexId.bucket.schemaHash -> (etlFilePath, etlFileSize) }
    private Map<String, Pair<String, Long>> tabletMetaToFileInfo = Maps.newHashMap();

    // --- members below not persist ---
    // temporary use
    // one SparkLoadJob has only one table to load
    // hivedb.table for global dict
    private String hiveTableName = "";
    private EtlClusterDesc etlClusterDesc;
    // for spark standalone
    private SparkAppHandle sparkAppHandle;
    // for straggler wait long time to commit transaction
    private long quorumFinishTimestamp = -1;
    // below for push task
    private Map<Long, Set<Long>> tableToLoadPartitions = Maps.newHashMap();
    private Map<Long, PushBrokerScannerParams> indexToPushBrokerReaderParams = Maps.newHashMap();
    private Map<Long, Integer> indexToSchemaHash = Maps.newHashMap();
    private Map<Long, Set<Long>> tabletToSentReplicas = Maps.newHashMap();
    private Set<Long> finishedReplicas = Sets.newHashSet();
    private Set<Long> quorumTablets = Sets.newHashSet();
    private Set<Long> fullTablets = Sets.newHashSet();

    private static class PushBrokerScannerParams {
        TBrokerScanRange tBrokerScanRange;
        TDescriptorTable tDescriptorTable;

        public void init(List<Column> columns, BrokerDesc brokerDesc) throws UserException {
            Analyzer analyzer = new Analyzer(null, null);
            // Generate tuple descriptor
            DescriptorTable descTable = analyzer.getDescTbl();
            TupleDescriptor destTupleDesc = descTable.createTupleDescriptor();
            // use index schema to fill the descriptor table
            for (Column column : columns) {
                SlotDescriptor destSlotDesc = descTable.addSlotDescriptor(destTupleDesc);
                destSlotDesc.setIsMaterialized(true);
                destSlotDesc.setColumn(column);
                if (column.isAllowNull()) {
                    destSlotDesc.setIsNullable(true);
                } else {
                    destSlotDesc.setIsNullable(false);
                }
            }
            // Push broker scan node
            PushBrokerScanNode scanNode = new PushBrokerScanNode(destTupleDesc);
            scanNode.setLoadInfo(columns, brokerDesc);
            scanNode.init(analyzer);
            tBrokerScanRange = scanNode.getTBrokerScanRange();

            // descTable
            descTable.computeMemLayout();
            tDescriptorTable = descTable.toThrift();
        }
    }

    private static class PushBrokerScanNode extends ScanNode {
        private TBrokerScanRange tBrokerScanRange;
        private List<Column> columns;
        private BrokerDesc brokerDesc;

        public PushBrokerScanNode(TupleDescriptor destTupleDesc) {
            super(new PlanNodeId(0), destTupleDesc, "PushBrokerScanNode");
            this.tBrokerScanRange = new TBrokerScanRange();
        }

        public void setLoadInfo(List<Column> columns, BrokerDesc brokerDesc) {
            this.columns = columns;
            this.brokerDesc = brokerDesc;
        }

        public void init(Analyzer analyzer) throws UserException {
            super.init(analyzer);

            // scan range params
            TBrokerScanRangeParams params = new TBrokerScanRangeParams();
            params.setStrict_mode(false);
            params.setProperties(brokerDesc.getProperties());
            TupleDescriptor srcTupleDesc = analyzer.getDescTbl().createTupleDescriptor();
            Map<String, SlotDescriptor> srcSlotDescByName = Maps.newHashMap();
            for (Column column : columns) {
                SlotDescriptor srcSlotDesc = analyzer.getDescTbl().addSlotDescriptor(srcTupleDesc);
                srcSlotDesc.setType(ScalarType.createType(PrimitiveType.VARCHAR));
                srcSlotDesc.setIsMaterialized(true);
                srcSlotDesc.setIsNullable(true);
                srcSlotDesc.setColumn(new Column(column.getName(), PrimitiveType.VARCHAR));
                params.addToSrc_slot_ids(srcSlotDesc.getId().asInt());
                srcSlotDescByName.put(column.getName(), srcSlotDesc);
            }

            TupleDescriptor destTupleDesc = desc;
            Map<Integer, Integer> destSidToSrcSidWithoutTrans = Maps.newHashMap();
            for (SlotDescriptor destSlotDesc : destTupleDesc.getSlots()) {
                if (!destSlotDesc.isMaterialized()) {
                    continue;
                }

                SlotDescriptor srcSlotDesc = srcSlotDescByName.get(destSlotDesc.getColumn().getName());
                destSidToSrcSidWithoutTrans.put(destSlotDesc.getId().asInt(), srcSlotDesc.getId().asInt());
                Expr expr = new SlotRef(srcSlotDesc);
                if (destSlotDesc.getType().getPrimitiveType() == PrimitiveType.BOOLEAN) {
                    // there is no cast string to boolean function
                    // so we cast string to tinyint first, then cast tinyint to boolean
                    expr = new CastExpr(Type.BOOLEAN, new CastExpr(Type.TINYINT, expr));
                } else {
                    expr = castToSlot(destSlotDesc, expr);
                }
                params.putToExpr_of_dest_slot(destSlotDesc.getId().asInt(), expr.treeToThrift());
            }
            params.setDest_sid_to_src_sid_without_trans(destSidToSrcSidWithoutTrans);
            params.setSrc_tuple_id(srcTupleDesc.getId().asInt());
            params.setDest_tuple_id(destTupleDesc.getId().asInt());
            tBrokerScanRange.setParams(params);

            // broker address updated for each replica
            tBrokerScanRange.setBroker_addresses(Lists.newArrayList());

            // broker range desc
            TBrokerRangeDesc tBrokerRangeDesc = new TBrokerRangeDesc();
            tBrokerScanRange.setRanges(Lists.newArrayList(tBrokerRangeDesc));
            tBrokerRangeDesc.setFile_type(TFileType.FILE_BROKER);
            tBrokerRangeDesc.setFormat_type(TFileFormatType.FORMAT_PARQUET);
            tBrokerRangeDesc.setSplittable(false);
            tBrokerRangeDesc.setStart_offset(0);
            tBrokerRangeDesc.setSize(-1);
            // path and file size updated for each replica
        }

        public TBrokerScanRange getTBrokerScanRange() {
            return tBrokerScanRange;
        }

        @Override
        public List<TScanRangeLocations> getScanRangeLocations(long maxScanRangeLength) {
            return null;
        }

        @Override
        protected void toThrift(TPlanNode msg) {}
    }

    // only for log replay
    public SparkLoadJob() {
        super();
        jobType = EtlJobType.SPARK;
    }

    public SparkLoadJob(long dbId, String label, EtlClusterDesc etlClusterDesc, OriginStatement originStmt)
            throws MetaNotFoundException {
        super(dbId, label, originStmt);
        this.etlClusterDesc = etlClusterDesc;
        timeoutSecond = Config.spark_load_default_timeout_second;
        jobType = EtlJobType.SPARK;
    }

    public String getHiveTableName() {
        return hiveTableName;
    }

    @Override
    protected void setJobProperties(Map<String, String> properties) throws DdlException {
        super.setJobProperties(properties);

        // set etl cluster and broker desc
        setEtlClusterInfo();

        // global dict
        if (properties != null) {
            if (properties.containsKey(BITMAP_DATA_PROPERTY)) {
                hiveTableName = properties.get(BITMAP_DATA_PROPERTY);
            }
        }
    }

    /**
     * merge system conf with load stmt
     * @throws DdlException
     */
    private void setEtlClusterInfo() throws DdlException {
        // etl cluster
        String clusterName = etlClusterDesc.getName();
        EtlCluster oriEtlCluster = Catalog.getCurrentCatalog().getEtlClusterMgr().getEtlCluster(clusterName);
        if (oriEtlCluster == null) {
            throw new DdlException("Etl cluster does not exist. name: " + clusterName);
        }
        Preconditions.checkState(oriEtlCluster instanceof SparkEtlCluster);
        etlCluster = ((SparkEtlCluster) oriEtlCluster).getCopiedEtlCluster();
        etlCluster.update(etlClusterDesc);

        // broker desc
        Map<String, String> brokerProperties = Maps.newHashMap();
        for (Map.Entry<String, String> entry : etlClusterDesc.getProperties().entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(EtlClusterDesc.BROKER_PROPERTY_KEY_PREFIX)) {
                brokerProperties.put(key.substring(key.indexOf(".") + 1), entry.getValue());
            }
        }
        brokerDesc = new BrokerDesc(etlCluster.getBroker(), brokerProperties);
    }

    @Override
    public void beginTxn()
            throws LabelAlreadyUsedException, BeginTransactionException, AnalysisException, DuplicatedRequestException {
       transactionId = Catalog.getCurrentGlobalTransactionMgr()
                .beginTransaction(dbId, Lists.newArrayList(fileGroupAggInfo.getAllTableIds()), label, null,
                                  new TxnCoordinator(TxnSourceType.FE, FrontendOptions.getLocalHostAddress()),
                                  LoadJobSourceType.FRONTEND, id, timeoutSecond);
    }

    @Override
    protected void unprotectedExecuteJob() throws LoadException {
        // create pending task
        LoadTask task = new SparkLoadPendingTask(this, fileGroupAggInfo.getAggKeyToFileGroups(),
                                                 etlCluster, brokerDesc);
        task.init();
        idToTasks.put(task.getSignature(), task);
        Catalog.getCurrentCatalog().getLoadTaskScheduler().submit(task);
    }

    @Override
    public void onTaskFinished(TaskAttachment attachment) {
        if (attachment instanceof SparkPendingTaskAttachment) {
            onPendingTaskFinished((SparkPendingTaskAttachment) attachment);
        }
    }

    private void onPendingTaskFinished(SparkPendingTaskAttachment attachment) {
        writeLock();
        try {
            // check if job has been cancelled
            if (isTxnDone()) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                                 .add("state", state)
                                 .add("error_msg", "this task will be ignored when job is: " + state)
                                 .build());
                return;
            }

            if (finishedTaskIds.contains(attachment.getTaskId())) {
                LOG.warn(new LogBuilder(LogKey.LOAD_JOB, id)
                                 .add("task_id", attachment.getTaskId())
                                 .add("error_msg", "this is a duplicated callback of pending task "
                                         + "when broker already has loading task")
                                 .build());
                return;
            }

            // add task id into finishedTaskIds
            finishedTaskIds.add(attachment.getTaskId());

            sparkAppHandle = attachment.getHandle();
            appId = attachment.getAppId();
            etlOutputPath = attachment.getOutputPath();

            executeEtl();
            // log etl state
            logUpdateStateInfo();
        } finally {
            writeUnlock();
        }
    }

    /**
     * update etl start time and state in spark load job
     */
    private void executeEtl() {
        etlStartTimestamp = System.currentTimeMillis();
        state = JobState.ETL;
    }

    private boolean checkState(JobState expectState) {
        readLock();
        try {
            if (state == expectState) {
                return true;
            }
            return false;
        } finally {
            readUnlock();
        }
    }

    public void updateEtlStatus() throws Exception {
        if (!checkState(JobState.ETL)) {
            return;
        }

        // get etl status
        SparkEtlJobHandler handler = new SparkEtlJobHandler();
        EtlStatus status = handler.getEtlJobStatus(sparkAppHandle, appId, id, etlCluster, etlOutputPath, brokerDesc);
        switch (status.getState()) {
            case RUNNING:
                updateEtlStatusInternal(status);
                break;
            case FINISHED:
                processEtlFinish(status, handler);
                break;
            case CANCELLED:
                throw new LoadException("spark etl job failed, msg: " + status.getFailMsg());
            default:
                LOG.warn("unknown etl state: {}", status.getState().name());
                break;
        }
    }

    private void updateEtlStatusInternal(EtlStatus etlStatus) {
        writeLock();
        try {
            loadingStatus = etlStatus;
            progress = etlStatus.getProgress();
            if (!etlCluster.isYarnMaster()) {
                loadingStatus.setTrackingUrl(appId);
            }

            // update load statis and counters
            DppResult dppResult = etlStatus.getDppResult();
            if (dppResult != null) {
                loadStatistic.fileNum = (int) dppResult.fileNumber;
                loadStatistic.totalFileSizeB = dppResult.fileSize;
                TUniqueId zeroId = new TUniqueId(0, 0);
                loadStatistic.initLoad(zeroId, Sets.newHashSet(zeroId));
                loadStatistic.updateLoad(zeroId, zeroId, dppResult.scannedRows);

                Map<String, String> counters = loadingStatus.getCounters();
                counters.put(DPP_NORMAL_ALL, String.valueOf(dppResult.normalRows));
                counters.put(DPP_ABNORMAL_ALL, String.valueOf(dppResult.abnormalRows));
                counters.put(UNSELECTED_ROWS, String.valueOf(dppResult.unselectRows));
            }
        } finally {
            writeUnlock();
        }
    }

    private void processEtlFinish(EtlStatus etlStatus, SparkEtlJobHandler handler) throws Exception {
        updateEtlStatusInternal(etlStatus);
        // checkDataQuality
        if (!checkDataQuality()) {
            cancelJobWithoutCheck(new FailMsg(FailMsg.CancelType.ETL_QUALITY_UNSATISFIED, QUALITY_FAIL_MSG),
                                  true, true);
            return;
        }

        // get etl output files and update loading state
        updateToLoadingState(etlStatus, handler.getEtlFilePaths(etlOutputPath, brokerDesc));
        // log loading state
        logUpdateStateInfo();

        // create push tasks
        prepareLoadingInfos();
        submitPushTasks();
    }

    private void updateToLoadingState(EtlStatus etlStatus, Map<String, Long> filePathToSize) throws LoadException {
        writeLock();
        try {
            for (Map.Entry<String, Long> entry : filePathToSize.entrySet()) {
                String filePath = entry.getKey();
                if (!filePath.endsWith(EtlJobConfig.ETL_OUTPUT_FILE_FORMAT)) {
                    continue;
                }
                String tabletMetaStr = EtlJobConfig.getTabletMetaStr(filePath);
                tabletMetaToFileInfo.put(tabletMetaStr, Pair.create(filePath, entry.getValue()));
            }

            loadingStatus = etlStatus;
            progress = 0;
            unprotectedUpdateState(JobState.LOADING);
        } catch (Exception e) {
            LOG.warn("update to loading state failed. job id: {}", id, e);
            throw new LoadException(e.getMessage(), e);
        } finally {
            writeUnlock();
        }
    }

    private void prepareLoadingInfos() {
        writeLock();
        try {
            for (String tabletMetaStr : tabletMetaToFileInfo.keySet()) {
                String[] fileNameArr = tabletMetaStr.split("\\.");
                // tableId.partitionId.indexId.bucket.schemaHash
                Preconditions.checkState(fileNameArr.length == 5);
                long tableId = Long.parseLong(fileNameArr[0]);
                long partitionId = Long.parseLong(fileNameArr[1]);
                long indexId = Long.parseLong(fileNameArr[2]);
                int schemaHash = Integer.parseInt(fileNameArr[4]);

                if (!tableToLoadPartitions.containsKey(tableId)) {
                    tableToLoadPartitions.put(tableId, Sets.newHashSet());
                }
                tableToLoadPartitions.get(tableId).add(partitionId);

                indexToSchemaHash.put(indexId, schemaHash);
            }
        } finally {
            writeUnlock();
        }
    }

    private PushBrokerScannerParams getPushBrokerReaderParams(OlapTable table, long indexId) throws UserException {
        if (!indexToPushBrokerReaderParams.containsKey(indexId)) {
            PushBrokerScannerParams pushBrokerScannerParams = new PushBrokerScannerParams();
            pushBrokerScannerParams.init(table.getSchemaByIndexId(indexId), brokerDesc);
            indexToPushBrokerReaderParams.put(indexId, pushBrokerScannerParams);
        }
        return indexToPushBrokerReaderParams.get(indexId);
    }

    private Set<Long> submitPushTasks() throws UserException {
        // check db exist
        Database db = null;
        try {
            db = getDb();
        } catch (MetaNotFoundException e) {
            String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                    .add("database_id", dbId)
                    .add("label", label)
                    .add("error_msg", "db has been deleted when job is loading")
                    .build();
            throw new MetaNotFoundException(errMsg);
        }

        AgentBatchTask batchTask = new AgentBatchTask();
        boolean hasLoadPartitions = false;
        Set<Long> totalTablets = Sets.newHashSet();
        db.readLock();
        try {
            writeLock();
            try {
                for (Map.Entry<Long, Set<Long>> entry : tableToLoadPartitions.entrySet()) {
                    long tableId = entry.getKey();
                    OlapTable table = (OlapTable) db.getTable(tableId);
                    if (table == null) {
                        LOG.warn("table does not exist. id: {}", tableId);
                        continue;
                    }

                    Set<Long> partitionIds = entry.getValue();
                    for (long partitionId : partitionIds) {
                        Partition partition = table.getPartition(partitionId);
                        if (partition == null) {
                            LOG.warn("partition does not exist. id: {}", partitionId);
                            continue;
                        }

                        hasLoadPartitions = true;
                        int quorumReplicaNum = table.getPartitionInfo().getReplicationNum(partitionId) / 2 + 1;

                        List<MaterializedIndex> indexes = partition.getMaterializedIndices(IndexExtState.ALL);
                        for (MaterializedIndex index : indexes) {
                            long indexId = index.getId();
                            int schemaHash = indexToSchemaHash.get(indexId);

                            int bucket = 0;
                            for (Tablet tablet : index.getTablets()) {
                                long tabletId = tablet.getId();
                                totalTablets.add(tabletId);
                                Set<Long> tabletAllReplicas = Sets.newHashSet();
                                Set<Long> tabletFinishedReplicas = Sets.newHashSet();
                                for (Replica replica : tablet.getReplicas()) {
                                    long replicaId = replica.getId();
                                    tabletAllReplicas.add(replicaId);
                                    if (!tabletToSentReplicas.containsKey(tabletId)
                                            || !tabletToSentReplicas.get(tabletId).contains(replica.getId())) {
                                        long backendId = replica.getBackendId();
                                        long taskSignature = Catalog.getCurrentGlobalTransactionMgr()
                                                .getTransactionIDGenerator().getNextTransactionId();

                                        PushBrokerScannerParams params = getPushBrokerReaderParams(table, indexId);
                                        // deep copy TBrokerScanRange because filePath and fileSize will be updated
                                        // in different tablet push task
                                        TBrokerScanRange tBrokerScanRange = new TBrokerScanRange(params.tBrokerScanRange);
                                        // update filePath fileSize
                                        TBrokerRangeDesc tBrokerRangeDesc = tBrokerScanRange.getRanges().get(0);
                                        tBrokerRangeDesc.setPath("");
                                        tBrokerRangeDesc.setFile_size(-1);
                                        String tabletMetaStr = String.format("%d.%d.%d.%d.%d", tableId, partitionId,
                                                                             indexId, bucket++, schemaHash);
                                        if (tabletMetaToFileInfo.containsKey(tabletMetaStr)) {
                                            Pair<String, Long> fileInfo = tabletMetaToFileInfo.get(tabletMetaStr);
                                            tBrokerRangeDesc.setPath(fileInfo.first);
                                            tBrokerRangeDesc.setFile_size(fileInfo.second);
                                        }

                                        // update broker address
                                        Backend backend = Catalog.getCurrentCatalog().getCurrentSystemInfo()
                                                .getBackend(backendId);
                                        FsBroker fsBroker = Catalog.getCurrentCatalog().getBrokerMgr().getBroker(
                                                brokerDesc.getName(), backend.getHost());
                                        tBrokerScanRange.getBroker_addresses().add(
                                                new TNetworkAddress(fsBroker.ip, fsBroker.port));

                                        PushTask pushTask = new PushTask(replica.getBackendId(), dbId, tableId, partitionId,
                                                                         indexId, tabletId, replica.getId(), schemaHash,
                                                                         0, getId(), TPushType.LOAD_V2,
                                                                         TPriority.NORMAL, transactionId, taskSignature,
                                                                         tBrokerScanRange, params.tDescriptorTable);
                                        if (AgentTaskQueue.addTask(pushTask)) {
                                            batchTask.addTask(pushTask);

                                            if (!tabletToSentReplicas.containsKey(tabletId)) {
                                                tabletToSentReplicas.put(tabletId, Sets.newHashSet());
                                            }
                                            tabletToSentReplicas.get(tabletId).add(replicaId);
                                        }
                                    }

                                    if (finishedReplicas.contains(replicaId) && replica.getLastFailedVersion() < 0) {
                                        tabletFinishedReplicas.add(replicaId);
                                    }
                                }

                                if (tabletAllReplicas.size() == 0) {
                                    LOG.error("invalid situation. tablet is empty. id: {}", tabletId);
                                }

                                // check tablet push states
                                if (tabletFinishedReplicas.size() >= quorumReplicaNum) {
                                    quorumTablets.add(tabletId);
                                    if (tabletFinishedReplicas.size() == tabletAllReplicas.size()) {
                                        fullTablets.add(tabletId);
                                    }
                                }
                            }
                        }
                    }
                }

                if (batchTask.getTaskNum() > 0) {
                    AgentTaskExecutor.submit(batchTask);
                }

                if (!hasLoadPartitions) {
                    String errMsg = new LogBuilder(LogKey.LOAD_JOB, id)
                            .add("database_id", dbId)
                            .add("label", label)
                            .add("error_msg", "all partitions have no load data")
                            .build();
                    throw new LoadException(errMsg);
                }

                return totalTablets;
            } finally {
                writeUnlock();
            }
        } finally {
            db.readUnlock();
        }
    }

    public void addFinishedReplica(long replicaId, long tabletId, long backendId) {
        writeLock();
        try {
            if (finishedReplicas.add(replicaId)) {
                commitInfos.add(new TabletCommitInfo(tabletId, backendId));
            }
        } finally {
            writeUnlock();
        }
    }

    public void updateLoadingStatus() throws UserException {
        if (!checkState(JobState.LOADING)) {
            return;
        }

        // submit push tasks
        Set<Long> totalTablets = submitPushTasks();

        // update status
        boolean canCommitJob = false;
        writeLock();
        try {
            // loading progress
            // 100: txn status is visible and load has been finished
            progress = fullTablets.size() * 100 / totalTablets.size();
            if (progress == 100) {
                progress = 99;
            }

            // quorum finish ts
            if (quorumFinishTimestamp < 0 && quorumTablets.containsAll(totalTablets)) {
                quorumFinishTimestamp = System.currentTimeMillis();
            }

            // if all replicas are finished or stay in quorum finished for long time, try to commit it.
            long stragglerTimeout = Config.load_straggler_wait_second * 1000;
            if ((quorumFinishTimestamp > 0 && System.currentTimeMillis() - quorumFinishTimestamp > stragglerTimeout)
                    || fullTablets.containsAll(totalTablets)) {
                canCommitJob = true;
            }
        } finally {
            writeUnlock();
        }

        // try commit transaction
        if (canCommitJob) {
            tryCommitJob();
        }
    }

    private void tryCommitJob() throws UserException {
        LOG.info(new LogBuilder(LogKey.LOAD_JOB, id)
                         .add("txn_id", transactionId)
                         .add("msg", "Load job try to commit txn")
                         .build());
        Database db = getDb();
        db.writeLock();
        try {
            Catalog.getCurrentGlobalTransactionMgr().commitTransaction(
                    dbId, transactionId, commitInfos,
                    new LoadJobFinalOperation(id, loadingStatus, progress, loadStartTimestamp,
                                              finishTimestamp, state, failMsg));
        } catch (TabletQuorumFailedException e) {
            // retry in next loop
        } finally {
            db.writeUnlock();
        }
    }

    @Override
    protected String getEtlClusterName() {
        return etlCluster.getName();
    }

    @Override
    protected long getEtlStartTimestamp() {
        return etlStartTimestamp;
    }

    @Override
    public void write(DataOutput out) throws IOException {
        super.write(out);
        etlCluster.write(out);
        out.writeLong(etlStartTimestamp);
        Text.writeString(out, appId);
        Text.writeString(out, etlOutputPath);
        out.writeInt(tabletMetaToFileInfo.size());
        for (Map.Entry<String, Pair<String, Long>> entry : tabletMetaToFileInfo.entrySet()) {
            Text.writeString(out, entry.getKey());
            Text.writeString(out, entry.getValue().first);
            out.writeLong(entry.getValue().second);
        }
    }

    public void readFields(DataInput in) throws IOException {
        super.readFields(in);
        etlCluster = (SparkEtlCluster) EtlCluster.read(in);
        etlStartTimestamp = in.readLong();
        appId = Text.readString(in);
        etlOutputPath = Text.readString(in);
        int size = in.readInt();
        for (int i = 0; i < size; i++) {
            String tabletMetaStr = Text.readString(in);
            Pair<String, Long> fileInfo = Pair.create(Text.readString(in), in.readLong());
            tabletMetaToFileInfo.put(tabletMetaStr, fileInfo);
        }
    }

    /**
     * log load job update info when job state changed to etl or loading
     */
    private void logUpdateStateInfo() {
        SparkLoadJobStateUpdateInfo info = new SparkLoadJobStateUpdateInfo(
                id, state, transactionId, etlStartTimestamp, appId, etlOutputPath,
                loadStartTimestamp, tabletMetaToFileInfo);
        Catalog.getCurrentCatalog().getEditLog().logUpdateLoadJob(info);
    }

    @Override
    public void replayUpdateStateInfo(LoadJobStateUpdateInfo info) {
        super.replayUpdateStateInfo(info);
        SparkLoadJobStateUpdateInfo sparkJobStateInfo = (SparkLoadJobStateUpdateInfo) info;
        etlStartTimestamp = sparkJobStateInfo.getEtlStartTimestamp();
        appId = sparkJobStateInfo.getAppId();
        etlOutputPath = sparkJobStateInfo.getEtlOutputPath();
        tabletMetaToFileInfo = sparkJobStateInfo.getTabletMetaToFileInfo();

        switch (state) {
            case ETL:
                // nothing to do
                break;
            case LOADING:
                prepareLoadingInfos();
                break;
            default:
                LOG.warn("replay update load job state info failed, error: wrong state. job id: {}, state: {}",
                         id, state);
                break;
        }
    }

    public static class SparkLoadJobStateUpdateInfo extends LoadJobStateUpdateInfo {
        @SerializedName(value = "etl_start_timestamp")
        private long etlStartTimestamp;
        @SerializedName(value = "app_id")
        private String appId;
        @SerializedName(value = "etl_output_path")
        private String etlOutputPath;
        @SerializedName(value = "tablet_meta_to_file_info")
        private Map<String, Pair<String, Long>> tabletMetaToFileInfo;

        public SparkLoadJobStateUpdateInfo(long jobId, JobState state, long transactionId, long etlStartTimestamp,
                                           String appId, String etlOutputPath, long loadStartTimestamp,
                                           Map<String, Pair<String, Long>> tabletMetaToFileInfo) {
            super(jobId, state, transactionId, loadStartTimestamp);
            this.etlStartTimestamp = etlStartTimestamp;
            this.appId = appId;
            this.etlOutputPath = etlOutputPath;
            this.tabletMetaToFileInfo = tabletMetaToFileInfo;
        }

        public long getEtlStartTimestamp() {
            return etlStartTimestamp;
        }

        public String getAppId() {
            return appId;
        }

        public String getEtlOutputPath() {
            return etlOutputPath;
        }

        public Map<String, Pair<String, Long>> getTabletMetaToFileInfo() {
            return tabletMetaToFileInfo;
        }
    }
}
