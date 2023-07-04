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
import groovy.sql.Sql
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId
import org.apache.commons.math3.stat.StatUtils

suite("test_wlm_tpxx_fixed_query") {
    sql "set global enable_pipeline_engine = true;"

    def url = 'jdbc:mysql://10.16.10.8:9230/clickbench?useSSL=false'
    def username = 'root'
    def password = ''

    def bigquery = 'SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM clickbench.hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;'
    def smallquery = 'SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM clickbench.hits'

    def query_func = { label, test_sql, time_array ->
        def start_time = System.currentTimeMillis()
        sql test_sql
        def end_time = System.currentTimeMillis()
        def exec_time = end_time - start_time
        time_array.add(exec_time)
        logger.info(label + " " + exec_time)
    }

    def thread_query_func = { label, group_name, test_sql, concurrency, iterations, time_array ->
        def threads = []

        for (int i = 0; i < concurrency; i++) {
            def cur_array = []
            time_array.add(cur_array);
            threads.add(Thread.startDaemon {
                if (group_name != "") {
                    sql "set workload_group='" + group_name +  "'"
                }
                for (int j = 0; j < iterations; j++) {
                    query_func(label + " " +j , test_sql, cur_array)
                }
            })
        }

        for (Thread t in threads) {
            t.join()
        }
    }

    def calculate_tpxx = { label, timecost_array ->
        List<Double> ret_val1 = new ArrayList<>();
        for (int[] array1 : timecost_array) {
            for (int val : array1) {
                ret_val1.add((double)val);
            }
        }

        double[] arr = ret_val1.toArray()
        double tp_50 = StatUtils.percentile(arr, 50)
        double tp_75 = StatUtils.percentile(arr, 75)
        double tp_90 = StatUtils.percentile(arr, 90)
        double tp_95 = StatUtils.percentile(arr, 95)
        double tp_99 = StatUtils.percentile(arr, 99)

        logger.info(label + " tp50=" + tp_50)
        logger.info(label + " tp75=" + tp_75)
        logger.info(label + " tp90=" + tp_90)
        logger.info(label + " tp95=" + tp_95)
        logger.info(label + " tp99=" + tp_99)
    }

    int bigquery_c = 1
    int bigquery_i = 20
    def bigquery_timecost = [][]
    def bigquery_group_name = ""
    def bigquery_thread = Thread.start {
        thread_query_func('bigquery', bigquery_group_name, bigquery, bigquery_c, bigquery_i, bigquery_timecost);
    }

    int smallquery_c = 1
    int smallquery_i = 20
    def smallquery_timecost = [][]
    def small_group_name = ""
    def smallquery_thread = Thread.start {
        thread_query_func('smallquery', small_group_name, smallquery, smallquery_c, smallquery_i, smallquery_timecost);
    }

    bigquery_thread.join()
    smallquery_thread.join()

    calculate_tpxx("bigquery", bigquery_timecost)
    calculate_tpxx("small query", smallquery_timecost)

    println "==========Test finish"
}
