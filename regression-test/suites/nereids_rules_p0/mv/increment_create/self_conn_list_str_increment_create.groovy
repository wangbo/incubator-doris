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

suite("self_conn_list_str_increment_create") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "SET enable_nereids_planner=true"
    sql "SET enable_fallback_to_original_planner=false"
    sql "SET enable_materialized_view_rewrite=false"
    sql "SET enable_nereids_timeout = false"

    sql """
    drop table if exists orders_self_conn_1
    """

    sql """CREATE TABLE `orders_self_conn_1` (
      `o_orderkey` BIGINT not NULL,
      `o_custkey` INT NULL,
      `o_orderstatus` VARCHAR(1) NULL,
      `o_totalprice` DECIMAL(15, 2)  NULL,
      `o_orderpriority` VARCHAR(15) NULL,
      `o_clerk` VARCHAR(15) NULL,
      `o_shippriority` INT NULL,
      `o_comment` VARCHAR(79) NULL,
      `o_orderdate` DATE NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(`o_orderkey`, `o_custkey`)
    COMMENT 'OLAP'
    PARTITION BY list(o_orderkey) (
    PARTITION p1 VALUES in ('1'),
    PARTITION p2 VALUES in ('2'),
    PARTITION p3 VALUES in ('3'),
    PARTITION p4 VALUES in ('4')
    )
    DISTRIBUTED BY HASH(`o_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    drop table if exists lineitem_selt_conn_1
    """

    sql """CREATE TABLE `lineitem_selt_conn_1` (
      `l_orderkey` BIGINT not NULL,
      `l_linenumber` INT NULL,
      `l_partkey` INT NULL,
      `l_suppkey` INT NULL,
      `l_quantity` DECIMAL(15, 2) NULL,
      `l_extendedprice` DECIMAL(15, 2) NULL,
      `l_discount` DECIMAL(15, 2) NULL,
      `l_tax` DECIMAL(15, 2) NULL,
      `l_returnflag` VARCHAR(1) NULL,
      `l_linestatus` VARCHAR(1) NULL,
      `l_commitdate` DATE NULL,
      `l_receiptdate` DATE NULL,
      `l_shipinstruct` VARCHAR(25) NULL,
      `l_shipmode` VARCHAR(10) NULL,
      `l_comment` VARCHAR(44) NULL,
      `l_shipdate` DATE NULL
    ) ENGINE=OLAP
    DUPLICATE KEY(l_orderkey, l_linenumber, l_partkey, l_suppkey )
    COMMENT 'OLAP'
    PARTITION BY list(l_orderkey) (
    PARTITION p1 VALUES in ('1'),
    PARTITION p2 VALUES in ('2'),
    PARTITION p3 VALUES in ('3')
    )
    DISTRIBUTED BY HASH(`l_orderkey`) BUCKETS 96
    PROPERTIES (
    "replication_allocation" = "tag.location.default: 1"
    );"""

    sql """
    insert into orders_self_conn_1 values 
    (2, 1, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (1, null, 'k', 109.2, 'c','d',2, 'mm', '2023-10-17'),
    (3, 3, null, 99.5, 'a', 'b', 1, 'yy', '2023-10-19'),
    (1, 2, 'o', null, 'a', 'b', 1, 'yy', '2023-10-20'),
    (2, 3, 'k', 109.2, null,'d',2, 'mm', '2023-10-21'),
    (3, 1, 'o', 99.5, 'a', null, 1, 'yy', '2023-10-22'),
    (1, 3, 'k', 99.5, 'a', 'b', null, 'yy', '2023-10-19'),
    (2, 1, 'o', 109.2, 'c','d',2, null, '2023-10-18'),
    (3, 2, 'k', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'),
    (4, 5, 'o', 99.5, 'a', 'b', 1, 'yy', '2023-10-19'); 
    """

    sql """
    insert into lineitem_selt_conn_1 values 
    (2, 1, 2, 3, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (1, null, 3, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-18', '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (3, 3, null, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', '2023-10-19', 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 2, 3, null, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17'),
    (2, 3, 2, 1, 5.5, 6.5, 7.5, 8.5, 'o', 'k', null, '2023-10-18', 'a', 'b', 'yyyyyyyyy', '2023-10-18'),
    (3, 1, 1, 2, 7.5, 8.5, 9.5, 10.5, 'k', 'o', '2023-10-19', null, 'c', 'd', 'xxxxxxxxx', '2023-10-19'),
    (1, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
    """

    sql """analyze table orders_self_conn_1 with sync;"""
    sql """analyze table lineitem_selt_conn_1 with sync;"""


    def mv_name = "mv_self_conn_list_str"
    def partition_by_part_col = "l_orderkey"
    def partition_by_not_part_col = "l_shipdate"
    def partition_by_part_col_right = "o_orderkey"
    def partition_by_not_part_col_right = "o_orderdate"
    def create_mv_left_part = { def mv_sql, def partition_col ->
        sql """DROP MATERIALIZED VIEW IF EXISTS ${mv_name};"""
        sql """DROP TABLE IF EXISTS ${mv_name}"""
        return """
        CREATE MATERIALIZED VIEW ${mv_name} 
        BUILD IMMEDIATE REFRESH AUTO ON MANUAL  
        partition by(${partition_col}) 
        DISTRIBUTED BY RANDOM BUCKETS 2  
        PROPERTIES ('replication_num' = '1')  
        AS  
        ${mv_sql}
        """
    }
    def refresh_mv = {
        sql """refresh MATERIALIZED VIEW ${mv_name} AUTO"""
    }
    def delete_mv = {
        sql """DROP MATERIALIZED VIEW ${mv_name};"""
    }

    def is_increment_change = { def cur_job_name ->
        def mv_task_infos = sql """select 
            JobName, Status, RefreshMode, NeedRefreshPartitions, CompletedPartitions, Progress 
            from tasks("type"="mv") where JobName="${cur_job_name}" order by CreateTime desc"""
        assert (mv_task_infos.size() == 2)

        def refresh_info = sql """select 
            JobName, Status, RefreshMode, NeedRefreshPartitions, CompletedPartitions, Progress 
            from tasks("type"="mv") where JobName="${cur_job_name}" order by CreateTime desc limit 1;"""
        assert (refresh_info[0][0] == cur_job_name)
        assert (refresh_info[0][1] == "SUCCESS")
        assert (refresh_info[0][2] == "PARTIAL")
        assert (refresh_info[0][3] == "[\"p_2\"]")
        assert (refresh_info[0][4] == "[\"p_2\"]")
        assert (refresh_info[0][5] == "100.00% (1/1)")
    }
    def is_complete_change = { def cur_job_name ->
        def mv_task_infos = sql """select 
            JobName, Status, RefreshMode, NeedRefreshPartitions, CompletedPartitions, Progress 
            from tasks("type"="mv") where JobName="${cur_job_name}" order by CreateTime desc"""
        assert (mv_task_infos.size() == 2)

        def refresh_info = sql """select 
            JobName, Status, RefreshMode, NeedRefreshPartitions, CompletedPartitions, Progress 
            from tasks("type"="mv") where JobName="${cur_job_name}" order by CreateTime desc limit 1;"""
        assert (refresh_info[0][0] == cur_job_name)
        assert (refresh_info[0][1] == "SUCCESS")
        assert (refresh_info[0][2] == "COMPLETE")
        assert (refresh_info[0][5] == "100.00% (3/3)")
    }

    def is_complete_change_right = { def cur_job_name ->
        def mv_task_infos = sql """select 
            JobName, Status, RefreshMode, NeedRefreshPartitions, CompletedPartitions, Progress 
            from tasks("type"="mv") where JobName="${cur_job_name}" order by CreateTime desc"""
        assert (mv_task_infos.size() == 2)

        def refresh_info = sql """select 
            JobName, Status, RefreshMode, NeedRefreshPartitions, CompletedPartitions, Progress 
            from tasks("type"="mv") where JobName="${cur_job_name}" order by CreateTime desc limit 1;"""
        assert (refresh_info[0][0] == cur_job_name)
        assert (refresh_info[0][1] == "SUCCESS")
        assert (refresh_info[0][2] == "COMPLETE")
        assert (refresh_info[0][5] == "100.00% (4/4)")
    }

    def primary_tb_change = {
        sql """
        insert into lineitem_selt_conn_1 values 
        (2, 3, 2, 2, 5.5, 6.5, 7.5, 8.5, 'o', 'k', '2023-10-17', '2023-10-17', 'a', 'b', 'yyyyyyyyy', '2023-10-17');
        """
    }
    def slave_tb_change = {
        sql"""
        insert into orders_self_conn_1 values 
        (2, 5, 'ok', 99.5, 'a', 'b', 1, 'yy', '2023-10-17'); 
        """
    }

    // no window func + on partition col
    def mv_sql_1 = """select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 1 
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_shipdate = t2.l_shipdate 
        group by t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey """

    def mv_sql_3 = """select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 1 
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_shipdate = t2.l_shipdate
        """

    // no window func + on not partition col
    def mv_sql_4 = """select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 1 
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_orderkey = t2.l_orderkey 
        group by t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey"""

    def mv_sql_6 = """select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 1   
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_orderkey = t2.l_orderkey
        """

    // window func + left col
    def mv_sql_7 = """select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 
        count(t1.l_shipdate) over (partition by t1.L_SHIPDATE order by t1.L_ORDERKEY) as window_count 
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_orderkey = t2.l_orderkey 
        group by t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey"""

    def mv_sql_8 = """select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 
        count(t1.l_shipdate) over (partition by t1.l_orderkey order by t1.l_orderkey) as window_count
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_orderkey = t2.l_orderkey 
        group by t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey"""

    def mv_sql_9 = """select t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey, 
        count(t1.l_shipdate) over (order by t1.l_orderkey) as window_count 
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_orderkey = t2.l_orderkey 
        group by t1.l_shipdate, t1.l_orderkey, t1.l_partkey, t1.l_suppkey"""

    // window func + right col
    def mv_sql_10 = """select t2.l_shipdate, t2.l_orderkey, t2.l_partkey, t2.l_suppkey,
        count(t2.l_shipdate) over (partition by t2.l_shipdate order by t2.L_ORDERKEY) as window_count 
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_orderkey = t2.l_orderkey 
        group by t2.l_shipdate, t2.l_orderkey, t2.l_partkey, t2.l_suppkey"""

    def mv_sql_11 = """select t2.l_shipdate, t2.l_orderkey, t2.l_partkey, t2.l_suppkey,
        count(t2.l_shipdate) over (partition by t2.l_orderkey order by t2.l_orderkey) as window_count
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_orderkey = t2.l_orderkey  
        group by t2.l_shipdate, t2.l_orderkey, t2.l_partkey, t2.l_suppkey"""

    def mv_sql_12 = """select t2.l_shipdate, t2.l_orderkey, t2.l_partkey, t2.l_suppkey,
        count(t2.l_shipdate) over (order by t2.l_orderkey) as window_count
        from lineitem_selt_conn_1 t1 
        join lineitem_selt_conn_1 t2 
        on t1.l_orderkey = t2.l_orderkey  
        group by t2.l_shipdate, t2.l_orderkey, t2.l_partkey, t2.l_suppkey"""

    def compare_res = { def stmt ->
        def origin_res = sql stmt
        logger.info("origin_res: " + origin_res)
        def mv_origin_res = sql """select * from ${mv_name} order by 1,2,3,4,5"""
        logger.info("mv_origin_res: " + mv_origin_res)
        assertTrue((mv_origin_res == [] && origin_res == []) || (mv_origin_res.size() == origin_res.size()))
        for (int row = 0; row < mv_origin_res.size(); row++) {
            assertTrue(mv_origin_res[row].size() == origin_res[row].size())
            for (int col = 0; col < mv_origin_res[row].size(); col++) {
                assertTrue(mv_origin_res[row][col] == origin_res[row][col])
            }
        }
    }

    def list_judgement = { def all_list, def increment_list, def complete_list,
                           def error_list, def cur_col, Closure date_change, Closure judge_func ->
        for (int i = 0; i < all_list.size(); i++) {
            logger.info("i: " + i)
            if (all_list[i] in error_list) {
                test {
                    def cur_sql = create_mv_left_part(all_list[i], cur_col)
                    sql cur_sql
                    exception "Unable to find a suitable base table for partitioning"
                }
            } else {
                def cur_sql = create_mv_left_part(all_list[i], cur_col)
                sql cur_sql

                def job_name = getJobName(db, mv_name)
                waitingMTMVTaskFinished(job_name)
                compare_res(all_list[i] + " order by 1,2,3,4,5")

                date_change()
                refresh_mv()
                waitingMTMVTaskFinished(job_name)
                compare_res(all_list[i] + " order by 1,2,3,4,5")

                if (all_list[i] in increment_list) {
                    is_increment_change(job_name)
                }
                if (all_list[i] in complete_list) {
                    judge_func(job_name)
                }

                delete_mv()
            }

        }
    }

    def sql_all_list = [mv_sql_1, mv_sql_3, mv_sql_4, mv_sql_6, mv_sql_7, mv_sql_8, mv_sql_9, mv_sql_10, mv_sql_11, mv_sql_12]
    def sql_increment_list = []
    def sql_complete_list = []

    // change left table data
    // create mv base on left table with partition col
    def sql_error_list = [mv_sql_1, mv_sql_3, mv_sql_4, mv_sql_6, mv_sql_7, mv_sql_8, mv_sql_9, mv_sql_10, mv_sql_11, mv_sql_12]
    list_judgement(sql_all_list, sql_increment_list, sql_complete_list, sql_error_list,
            partition_by_part_col, primary_tb_change, is_complete_change)

    // create mv base on left table with no partition col
    list_judgement(sql_all_list, sql_increment_list, sql_complete_list, sql_error_list,
            partition_by_not_part_col, primary_tb_change, is_complete_change)

    // create mv base on right table with partition col
    list_judgement(sql_all_list, sql_increment_list, sql_complete_list, sql_error_list,
            partition_by_part_col_right, primary_tb_change, is_complete_change_right)

    // create mv base on right table with no partition col
    list_judgement(sql_all_list, sql_increment_list, sql_complete_list, sql_error_list,
            partition_by_not_part_col_right, primary_tb_change, is_complete_change)

}
