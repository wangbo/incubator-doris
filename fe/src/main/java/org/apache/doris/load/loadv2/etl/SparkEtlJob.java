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

package org.apache.doris.load.loadv2.etl;

import com.esotericsoftware.kryo.Kryo;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.commons.collections.map.MultiValueMap;
import org.apache.doris.load.loadv2.dpp.GlobalDictBuilder;
import org.apache.doris.load.loadv2.dpp.SparkDpp;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlColumn;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlColumnMapping;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlFileGroup;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlIndex;
import org.apache.doris.load.loadv2.etl.EtlJobConfig.EtlTable;

import org.apache.spark.SparkConf;
import org.apache.spark.serializer.KryoRegistrator;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
/**
 * SparkEtlJob is responsible for global dict building, data partition, data sort and data aggregation.
 * 1. init job config
 * 2. check if job has bitmap_dict function columns
 * 3. build global dict if step 2 is true
 * 4. dpp (data partition, data sort and data aggregation)
 */
public class SparkEtlJob {
    private static final String BITMAP_DICT_FUNC = "bitmap_dict";
    private static final String TO_BITMAP_FUNC = "to_bitmap";

    private String jobConfigFilePath;
    private EtlJobConfig etlJobConfig;
    private Map<Long, Set<String>> tableToBitmapDictColumns;
    private SparkSession spark;

    private SparkEtlJob(String jobConfigFilePath) {
        this.jobConfigFilePath = jobConfigFilePath;
        this.etlJobConfig = null;
        this.tableToBitmapDictColumns = Maps.newHashMap();
    }

    public class DorisKryoRegistrator implements KryoRegistrator {

        public DorisKryoRegistrator() {}

        @Override
        public void registerClasses(Kryo kryo) {
            kryo.register(org.apache.doris.load.loadv2.Roaring64Map.class);
        }
    }

    private void initSparkEnvironment() {

        SparkConf conf = new SparkConf();
        //serialization conf
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrator", "org.apache.doris.load.loadv2.dpp.DorisKryoRegistrator");
        conf.set("spark.kryo.registrationRequired", "false");

        spark = SparkSession.builder().enableHiveSupport().config(conf).getOrCreate();
//        SparkContext sparkContext = spark.sparkContext();
//        sparkContext.conf().set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
//        sparkContext.conf().set("spark.kryo.registrator", "org.apache.doris.load.loadv2.etl.SparkEtlJob$DorisKryoRegistrator");

    }

    private void initConfig() {
        System.err.println("****** job config file path: " + jobConfigFilePath);
        Dataset<String> ds = spark.read().textFile(jobConfigFilePath);
        String jsonConfig = ds.first();
        System.err.println("****** rdd read json config: " + jsonConfig);
        etlJobConfig = EtlJobConfig.configFromJson(jsonConfig);
        System.err.println("****** etl job config: " + etlJobConfig.toString());
    }

    /*
     * 1. check bitmap column
     * 2. fill tableToBitmapDictColumns
     * 3. remove bitmap_dict and to_bitmap mapping from columnMappings
     */
    private void checkConfig() throws Exception {
        Map<Long, EtlTable> tables = etlJobConfig.tables;
        for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
            Set<String> bitmapDictColumns = Sets.newHashSet();
            for (EtlFileGroup fileGroup : entry.getValue().fileGroups) {
                Map<String, EtlColumnMapping> newColumnMappings = Maps.newHashMap();
                for (Map.Entry<String, EtlColumnMapping> mappingEntry : fileGroup.columnMappings.entrySet()) {
                    String columnName = mappingEntry.getKey();
                    String exprStr = mappingEntry.getValue().toDescription();
                    String funcName = functions.expr(exprStr).expr().prettyName();
                    if (funcName.equalsIgnoreCase(BITMAP_DICT_FUNC)) {
                        bitmapDictColumns.add(columnName);
                    } else if (!funcName.equalsIgnoreCase(TO_BITMAP_FUNC)) {
                        newColumnMappings.put(mappingEntry.getKey(), mappingEntry.getValue());
                    }
                }
                // reset new columnMappings
                // System.err.println("****** new column mappings: " + newColumnMappings);
                fileGroup.columnMappings = newColumnMappings;
            }
            if (!bitmapDictColumns.isEmpty()) {
                tableToBitmapDictColumns.put(entry.getKey(), bitmapDictColumns);
            }
        }
        System.err.println("****** tableToBitmapDictColumns: " + tableToBitmapDictColumns);

        // spark etl must have only one table with bitmap type column to process.
        if (tableToBitmapDictColumns.size() > 1) {
            throw new Exception("spark etl job must have only one table with bitmap type column to process");
        }
    }

    private void processDpp() throws Exception {
        SparkDpp sparkDpp = new SparkDpp(spark, etlJobConfig);
        sparkDpp.init();
        sparkDpp.doDpp();
    }

    private void buildGlobalDictAndEncodeSourceTable(EtlTable table, long tableId) {
        List<String> distinctColumnList = tableToBitmapDictColumns.get(tableId) == null ? new ArrayList<>() : Lists.newArrayList(tableToBitmapDictColumns.get(tableId));
        List<String> dorisOlapTableColumnList = Lists.newArrayList();
        List<String> mapSideJoinColumns = Lists.newArrayList();
        List<EtlColumn> baseSchema = null;
        for (EtlIndex etlIndex : table.indexes) {
            if (etlIndex.isBaseIndex) {
                baseSchema = etlIndex.columns;
            }
        }
        for (EtlColumn column : baseSchema) {
            dorisOlapTableColumnList.add(column.columnName);
        }

        EtlFileGroup fileGroup = table.fileGroups.get(0);
        String sourceHiveDBTableName = fileGroup.hiveTableName;
//        String dorisHiveDB = sourceHiveDBTableName.split("\\.")[0];
        String dorisHiveDB = "kylin2x_test";
        String sourceHiveFilter = fileGroup.where;

        String taskId = etlJobConfig.outputPath.substring(etlJobConfig.outputPath.lastIndexOf("/") + 1);
        String globalDictTableName = String.format(EtlJobConfig.GLOBAL_DICT_TABLE_NAME, tableId);
        String distinctKeyTableName = String.format(EtlJobConfig.DISTINCT_KEY_TABLE_NAME, tableId, taskId);
        String dorisIntermediateHiveTable = String.format(EtlJobConfig.DORIS_INTERMEDIATE_HIVE_TABLE_NAME,
                                                          tableId, taskId);

        // TODO remove hard code
        mapSideJoinColumns.add("base_info_unionid");
        mapSideJoinColumns.add("total_check_cardno");
        mapSideJoinColumns.add("landing_page_unionid");
        mapSideJoinColumns.add("apply_now_unionid");
        mapSideJoinColumns.add("base_next_unionid");
        mapSideJoinColumns.add("tl1_pop_unionid");
        mapSideJoinColumns.add("personal_info_unionid");
        mapSideJoinColumns.add("job_info_unionid");
        mapSideJoinColumns.add("apply_submit_unionid");
        mapSideJoinColumns.add("apply_result_unionid");
        mapSideJoinColumns.add("td_apply_flowno");
        mapSideJoinColumns.add("td_apply_check_cardno");
        mapSideJoinColumns.add("td_check_cardno");
        mapSideJoinColumns.add("td_actv_cardno");
        mapSideJoinColumns.add("td_trade_cardno");
        mapSideJoinColumns.add("td_mk_trade_cardno");
        mapSideJoinColumns.add("total_trade_cardno");
        mapSideJoinColumns.add("b14b7_apply_flowno");
        mapSideJoinColumns.add("b14b7_check_cardno");
        mapSideJoinColumns.add("total_t30_actv_cardno");
        mapSideJoinColumns.add("total_t30_actv_cardno");
        mapSideJoinColumns.add("total_t14_actv_cardno");
        mapSideJoinColumns.add("total_t7_actv_cardno");
        mapSideJoinColumns.add("total_mk_trade_cardno");
        mapSideJoinColumns.add("total_actv_cardno");
        mapSideJoinColumns.add("apply_flow_no");
        mapSideJoinColumns.add("click_unionid");
        mapSideJoinColumns.add("card_no_cipher");
        mapSideJoinColumns.add("total_t7_check_cardno");
        mapSideJoinColumns.add("total_apply_flowno");
        mapSideJoinColumns.add("click_all_unionid");

        MultiValueMap dictColumns = new MultiValueMap();
        dictColumns.put("union_id", "exposure_all_unionid");
        dictColumns.put("union_id", "click_all_unionid");
        dictColumns.put("union_id", "exposure_unionid");
        dictColumns.put("union_id", "click_unionid");
        dictColumns.put("union_id", "landing_page_unionid");
        dictColumns.put("union_id", "apply_now_unionid");
        dictColumns.put("union_id", "base_info_unionid");
        dictColumns.put("union_id", "base_next_unionid");
        dictColumns.put("union_id", "tl1_pop_unionid");
        dictColumns.put("union_id", "personal_info_unionid");
        dictColumns.put("union_id", "job_info_unionid");
        dictColumns.put("union_id", "apply_submit_unionid");
        dictColumns.put("union_id", "apply_result_unionid");
        dictColumns.put("card_no_cipher", "td_apply_check_cardno");
        dictColumns.put("card_no_cipher", "td_check_cardno");
        dictColumns.put("card_no_cipher", "td_actv_cardno");
        dictColumns.put("card_no_cipher", "td_trade_cardno");
        dictColumns.put("card_no_cipher", "td_mk_trade_cardno");
        dictColumns.put("card_no_cipher", "total_check_cardno");
        dictColumns.put("card_no_cipher", "total_actv_cardno");
        dictColumns.put("card_no_cipher", "total_trade_cardno");
        dictColumns.put("card_no_cipher", "total_mk_trade_cardno");
        dictColumns.put("card_no_cipher", "total_t7_check_cardno");
        dictColumns.put("card_no_cipher", "total_t7_actv_cardno");
        dictColumns.put("card_no_cipher", "total_t14_actv_cardno");
        dictColumns.put("card_no_cipher", "total_t30_actv_cardno");
        dictColumns.put("card_no_cipher", "b14b7_check_cardno");
        dictColumns.put("apply_flow_no", "td_apply_flowno");
        dictColumns.put("apply_flow_no", "b14b7_apply_flowno");
        dictColumns.put("apply_flow_no", "total_apply_flowno");
        dictColumns.put("mt_user_id", null);

        System.out.println("****** distinctColumnList: " + distinctColumnList);
        System.out.println("dorisOlapTableColumnList: " + dorisOlapTableColumnList);
        System.out.println("mapSideJoinColumns: " + mapSideJoinColumns);
        System.out.println("sourceHiveDBTableName: " + sourceHiveDBTableName);
        System.out.println("sourceHiveFilter: " + sourceHiveFilter);
        System.out.println("dorisHiveDB: " + dorisHiveDB);
        System.out.println("distinctKeyTableName: " + distinctKeyTableName);
        System.out.println("globalDictTableName: " + globalDictTableName);
        System.out.println("dict reuse:" + dictColumns);
        System.out.println("****** dorisIntermediateHiveTable: " + dorisIntermediateHiveTable);

        try {
            List<String> veryHighCardinalityColumn = new ArrayList<>();
            veryHighCardinalityColumn.add("mt_user_id");
            veryHighCardinalityColumn.add("union_id");
            veryHighCardinalityColumn.add("exposure_all_unionid");
            veryHighCardinalityColumn.add("exposure_unionid");
            veryHighCardinalityColumn.add("total_apply_flowno");
            veryHighCardinalityColumn.add("apply_flow_no");
            GlobalDictBuilder buildGlobalDict = new GlobalDictBuilder(dictColumns, dorisOlapTableColumnList,
                    mapSideJoinColumns, sourceHiveDBTableName,
                    sourceHiveFilter, dorisHiveDB, distinctKeyTableName,
                    globalDictTableName, dorisIntermediateHiveTable, 10, veryHighCardinalityColumn, 5, false, spark);
            buildGlobalDict.createHiveIntermediateTable();
            if (tableToBitmapDictColumns.isEmpty()) {
                return;
            }
            buildGlobalDict.extractDistinctColumn();
            buildGlobalDict.buildGlobalDict();
            buildGlobalDict.encodeDorisIntermediateHiveTable();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void processData() throws Exception {
        // build global dict if has bitmap dict columns
//        if (!tableToBitmapDictColumns.isEmpty()) {
            // only one table
            long tableId = -1;
            EtlTable table = null;
            for (Map.Entry<Long, EtlTable> entry : etlJobConfig.tables.entrySet()) {
                tableId = entry.getKey();
                table = entry.getValue();
                break;
            }
            // build global dict and encode source hive table
            buildGlobalDictAndEncodeSourceTable(table, tableId);
//        }

        // data partition sort and aggregation
        processDpp();
    }

    private void run() throws Exception {
        initSparkEnvironment();
        initConfig();
        checkConfig();
        processData();
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("missing job config file path arg");
            System.exit(-1);
        }

        try {
            new SparkEtlJob(args[0]).run();
        } catch (Exception e) {
            System.err.println("spark etl job run fail");
            e.printStackTrace();
            System.exit(-1);
        }
    }

}
