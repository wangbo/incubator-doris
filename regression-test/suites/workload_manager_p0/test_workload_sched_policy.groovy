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

suite("test_workload_sched_policy") {

    sql "set experimental_enable_nereids_planner = false;"

    sql "drop workload schedule policy if exists test_cancel_policy;"
    sql "drop workload schedule policy if exists set_action_policy;"
    sql "drop workload schedule policy if exists fe_policy;"
    sql "drop workload schedule policy if exists be_policy;"

    // 1 create cancel policy
    sql "create workload schedule policy test_cancel_policy " +
            " conditions(query_time > 10) " +
            " actions(cancel_query) properties('enabled'='false'); "

    // 2 create set policy
    sql "create workload schedule policy set_action_policy " +
            "conditions(username='root') " +
            "actions(set_session_variable 'workload_group=normal');"

    // 3 create policy run in fe
    sql "create workload schedule policy fe_policy " +
            "conditions(username='root') " +
            "actions(set_session_variable 'workload_group=normal') " +
            "properties( " +
            "'enabled' = 'false', " +
            "'priority'='10' " +
            ");"

    // 4 create policy run in be
    sql "create workload schedule policy be_policy " +
            "conditions(query_time > 10) " +
            "actions(cancel_query) " +
            "properties( " +
            "'enabled' = 'false', " +
            "'priority'='10' " +
            ");"

    qt_select_policy_tvf "select name,condition,action,priority,enabled,version from workload_schedule_policy where name in('be_policy','fe_policy','set_action_policy','test_cancel_policy') order by name;"

    // test_alter
    sql "alter workload schedule policy fe_policy properties('priority'='2', 'enabled'='false');"

    // create failed check
    test {
        sql "create workload schedule policy failed_policy " +
                "conditions(abc > 123) actions(cancel_query);"

        exception "invalid metric name"
    }

    test {
        sql "create workload schedule policy failed_policy " +
                "conditions(query_time > 123) actions(abc);"

        exception "invalid action type"
    }

    test {
        sql "alter workload schedule policy fe_policy properties('priority'='abc');"

        exception "invalid priority property value"
    }

    test {
        sql "alter workload schedule policy fe_policy properties('enabled'='abc');"

        exception "invalid enabled property value"
    }

    test {
        sql "alter workload schedule policy fe_policy properties('priority'='10000');"

        exception "priority can only between"
    }

    test {
        sql "create workload schedule policy conflict_policy " +
                "conditions (query_time > 0) " +
                "actions(cancel_query, cancel_query);"

        exception "duplicate action in one policy"
    }

    test {
        sql "create workload schedule policy conflict_policy " +
                "conditions (username = 'root') " +
                "actions(set_session_variable 'workload_group=normal', set_session_variable 'workload_group=abc');"

        exception "duplicate set_session_variable action args one policy"
    }

    // drop
    sql "drop workload schedule policy test_cancel_policy;"
    sql "drop workload schedule policy set_action_policy;"
    sql "drop workload schedule policy fe_policy;"
    sql "drop workload schedule policy be_policy;"

    qt_select_policy_tvf_after_drop "select name,condition,action,priority,enabled,version from workload_schedule_policy where name in('be_policy','fe_policy','set_action_policy','test_cancel_policy') order by name;"

    // test workload schedule policy
    sql "ADMIN SET FRONTEND CONFIG ('workload_sched_policy_interval_ms' = '500');"
    sql """drop user if exists test_workload_sched_user"""
    sql """create user test_workload_sched_user identified by '12345'"""
    sql """grant ADMIN_PRIV on *.*.* to test_workload_sched_user"""

    // 1 create test_set_var_policy
    sql "create workload schedule policy test_set_var_policy conditions(username='test_workload_sched_user')" +
            "actions(set_session_variable 'parallel_pipeline_task_num=33');"
    def result1 = connect(user = 'test_workload_sched_user', password = '12345', url = context.config.jdbcUrl) {
        logger.info("begin sleep 15s to wait")
        Thread.sleep(15000)
        sql "show variables like '%parallel_pipeline_task_num%';"
    }
    assertEquals("parallel_pipeline_task_num", result1[0][0])
    assertEquals("33", result1[0][1])

    // 2 create test_set_var_policy2 with higher priority
    sql "create workload schedule policy test_set_var_policy2 conditions(username='test_workload_sched_user') " +
            "actions(set_session_variable 'parallel_pipeline_task_num=22') properties('priority'='10');"
    def result2 = connect(user = 'test_workload_sched_user', password = '12345', url = context.config.jdbcUrl) {
        Thread.sleep(3000)
        sql "show variables like '%parallel_pipeline_task_num%';"
    }
    assertEquals("parallel_pipeline_task_num", result2[0][0])
    assertEquals("22", result2[0][1])

    // 3 disable test_set_var_policy2
    sql "alter workload schedule policy test_set_var_policy2 properties('enabled'='false');"
    def result3 = connect(user = 'test_workload_sched_user', password = '12345', url = context.config.jdbcUrl) {
        Thread.sleep(3000)
        sql "show variables like '%parallel_pipeline_task_num%';"
    }
    assertEquals("parallel_pipeline_task_num", result3[0][0])
    assertEquals("33", result3[0][1])

    sql "ADMIN SET FRONTEND CONFIG ('workload_sched_policy_interval_ms' = '10000');"

    sql "drop workload schedule policy if exists test_set_var_policy;"
    sql "drop workload schedule policy if exists test_set_var_policy2;"

    sql "drop user if exists test_policy_user"
    sql "drop workload schedule policy if exists test_cancel_query_policy"
    sql "drop workload schedule policy if exists test_cancel_query_policy2"
    sql "drop workload schedule policy if exists test_set_session"
    sql "drop workload group if exists policy_group;"
    sql "CREATE USER 'test_policy_user'@'%' IDENTIFIED BY '12345';"
    sql """grant SELECT_PRIV on *.*.* to test_policy_user;"""
    sql "create workload group if not exists policy_group properties ('cpu_share'='1024');"
    sql "create workload group if not exists policy_group2 properties ('cpu_share'='1024');"
    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'policy_group' TO 'test_policy_user'@'%';"
    sql "GRANT USAGE_PRIV ON WORKLOAD GROUP 'policy_group2' TO 'test_policy_user'@'%';"
    sql "create workload schedule policy test_cancel_query_policy conditions(query_time > 1000) actions(cancel_query) properties('workload_group'='policy_group')"
    sql "create workload schedule policy test_cancel_query_policy2 conditions(query_time > 0, be_scan_rows>1) actions(cancel_query) properties('workload_group'='policy_group')"
    sql "create workload schedule policy test_set_session conditions(username='test_policy_user') actions(set_session_variable 'parallel_pipeline_task_num=1')"

    test {
        sql "drop workload group policy_group;"
        exception "because it has related policy"
    }

    test {
        sql "alter workload schedule policy test_cancel_query_policy properties('workload_group'='invalid_gorup');"
        exception "unknown workload group"
    }

    // daemon thread alter test
    def thread1 = new Thread({
        def startTime = System.currentTimeMillis()
        def curTime = System.currentTimeMillis()
        def totalTime = 30 * 60 * 1000 // 30min

        connect(user = 'test_policy_user', password = '12345', url = context.config.jdbcUrl) {
            sql "set workload_group=policy_group"
            boolean flag = false
            long lastTime = System.currentTimeMillis()

            while (curTime - startTime <= totalTime) {
                if (curTime - lastTime > 20000) {
                    if (flag) {
                        connect(user = 'root', password = '', url = context.config.jdbcUrl) {
                            sql "alter workload schedule policy test_cancel_query_policy properties('workload_group'='policy_group2');"
                            sql "alter workload schedule policy test_cancel_query_policy2 properties('workload_group'='policy_group');"
                        }
                        flag = false
                    } else {
                        connect(user = 'root', password = '', url = context.config.jdbcUrl) {
                            sql "alter workload schedule policy test_cancel_query_policy properties('workload_group'='policy_group');"
                            sql "alter workload schedule policy test_cancel_query_policy2 properties('workload_group'='policy_group2');"
                        }
                        flag = true
                    }
                    lastTime = System.currentTimeMillis()
                }
                try {
                    sql "select k0,k1,k2,k3,k4,k5,k6,count(distinct k13) from regression_test_load_p0_insert.baseall group by k0,k1,k2,k3,k4,k5,k6"
                } catch (Exception e) {
                    assertTrue(e.getMessage().contains("query canceled by workload scheduler"))
                }

                try {
                    sql "select count(1) from regression_test_load_p0_insert.baseall"
                } catch (Exception e) {
                    assertTrue(e.getMessage().contains("query canceled by workload scheduler"))
                }

                Thread.sleep(1000)
                curTime = System.currentTimeMillis()
            }
        }
    })
    thread1.setDaemon(true)
    thread1.start()

}