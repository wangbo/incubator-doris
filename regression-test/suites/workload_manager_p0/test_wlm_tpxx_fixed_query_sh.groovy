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
import org.apache.commons.math3.stat.StatUtils
import org.apache.groovy.parser.antlr4.util.StringUtils

import java.util.concurrent.atomic.AtomicBoolean

def begin_time = System.currentTimeMillis()

//def url = 'jdbc:mysql://127.0.0.1:9230/hits?useSSL=false'

def url = 'jdbc:mysql://127.0.0.1:9030/hits?useSSL=false'
def username = 'root'
def password = ''

def bigquery = 'SELECT WatchID, ClientIP, COUNT(*) AS c, SUM(IsRefresh), AVG(ResolutionWidth) FROM hits.hits GROUP BY WatchID, ClientIP ORDER BY c DESC LIMIT 10;'
def smallquery = 'SELECT SUM(AdvEngineID), COUNT(*), AVG(ResolutionWidth) FROM hits.hits'

AtomicBoolean should_stop = new AtomicBoolean(false);

def query_func = { sql, label, test_sql, time_array ->
    def start_time = System.currentTimeMillis()
    sql.execute(test_sql)
    def end_time = System.currentTimeMillis()
    def exec_time = end_time - start_time
    time_array.add(exec_time)
    println(label + " : " + exec_time)
    println()
}

def thread_query_func = { label, group_name, test_sql, file_name, concurrency, iterations, time_array ->
    def threads = []
    def cur_sql = test_sql
    if (!StringUtils.isEmpty(file_name)) {
        def q_file = new File(file_name)
        cur_sql = q_file.text
    }

    for (int i = 0; i < concurrency; i++) {
        def cur_array = []
        time_array.add(cur_array);
        threads.add(Thread.startDaemon {
            def sql = Sql.newInstance(url, username, password, 'com.mysql.jdbc.Driver')
            if (group_name != "") {
                sql.execute("set workload_group='" + group_name + "'")
            }
            for (int j = 0; j < iterations; j++) {
                if (should_stop.get()) {
                    break
                }
                query_func(sql, label + " " + j, cur_sql, cur_array)
            }
        })
    }

    for (Thread t in threads) {
        t.join()
    }
    println(label + " query finished")
    should_stop.set(true)
}

def calculate_tpxx = { label, timecost_array ->
    List<Double> ret_val1 = new ArrayList<>();
    for (int[] array1 : timecost_array) {
        for (int val : array1) {
            ret_val1.add((double) val);
        }
    }

    double[] arr = ret_val1.toArray()
    double tp_50 = StatUtils.percentile(arr, 50)
    double tp_75 = StatUtils.percentile(arr, 75)
    double tp_90 = StatUtils.percentile(arr, 90)
    double tp_95 = StatUtils.percentile(arr, 95)
    double tp_99 = StatUtils.percentile(arr, 99)

    println(label + " tp50=" + tp_50)
    println(label + " tp75=" + tp_75)
    println(label + " tp90=" + tp_90)
    println(label + " tp95=" + tp_95)
    println(label + " tp99=" + tp_99)
}

int bigquery_c = 8
int bigquery_i = 100
def bigquery_timecost = [][]
def bigquery_group_name = "g1"
def bigquery_file = "/home/ec2-user/reg_test/bigquery"
//def bigquery_file= "/mnt/disk2/wangbo/tmp/bquery/q29"
def bigquery_thread = Thread.start {
    thread_query_func('bigq', bigquery_group_name, bigquery, bigquery_file, bigquery_c, bigquery_i, bigquery_timecost);
}

Thread.sleep(10000)

int smallquery_c = 1
int smallquery_i = 400
def smallquery_timecost = [][]
def small_group_name = "g2"
def small_query_file = ""
def smallquery_thread = Thread.start {
    thread_query_func('smallq', small_group_name, smallquery, small_query_file, smallquery_c, smallquery_i, smallquery_timecost);
}

bigquery_thread.join()
smallquery_thread.join()

calculate_tpxx("bigquery", bigquery_timecost)
calculate_tpxx("small query", smallquery_timecost)

println "==========Test finish, time cost=" + (System.currentTimeMillis() - begin_time) / 1000

