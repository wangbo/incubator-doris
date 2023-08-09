import groovy.sql.Sql
import org.apache.groovy.parser.antlr4.util.StringUtils

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import org.apache.commons.math3.stat.StatUtils

import java.util.concurrent.locks.ReentrantLock

def begin_time = System.currentTimeMillis()

ReentrantLock write_ret_lock = new ReentrantLock()
List<String> print_ret = new ArrayList<>()

def test_conf = new ConfigSlurper()
        .parse(
                new File("conf/mixed_query_test_conf.groovy")
                        .toURI()
                        .toURL()
        )


boolean enable_test = Boolean.parseBoolean(test_conf.global_conf.enable_test)
if (!enable_test) {
    System.exit(0)
}

url = test_conf.global_conf.url
username = test_conf.global_conf.username
password = test_conf.global_conf.password
boolean enable_pipe = Boolean.parseBoolean(test_conf.global_conf.enable_pipe)
boolean enable_group = Boolean.parseBoolean(test_conf.global_conf.enable_group)

AtomicBoolean should_stop = new AtomicBoolean(false);

def calculate_tpxx = { label, timecost_array, list ->
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

    list.add(label + " tp50=" + tp_50)
    list.add(label + " tp75=" + tp_75)
    list.add(label + " tp90=" + tp_90)
    list.add(label + " tp95=" + tp_95)
    list.add(label + " tp99=" + tp_99)
}


def query_func = { conf ->
    AtomicInteger succ_num = new AtomicInteger(0)
    AtomicInteger failed_num = new AtomicInteger(0)

    // 1 get sql list
    def sql_file = new File(conf.dir)
    String[] sql_array = sql_file.text.split(";")

    List<Long> timeCost = new ArrayList<>()
    // 2 submit query
    int concurrency = Integer.parseInt(conf.c)
    int iteration = Integer.parseInt(conf.i)
    def threads = []
    for (int i = 0; i < concurrency; i++) {
        threads.add(Thread.startDaemon {
            def sql = Sql.newInstance(url, username, password, 'com.mysql.jdbc.Driver')
            if (enable_group) {
                sql.execute("set workload_group='" + conf.group + "'")
            }
            if (enable_pipe) {
                sql.execute("set enable_pipeline_engine='" + conf.group + "'")
            }
            for (int j = 0; j < iteration; j++) {
                if (should_stop.get()) {
                    break
                }

                for (int k = 0; k < sql_array.length; k++) {
                    if (should_stop.get()) {
                        break
                    }

                    String query_sql = sql_array[k]
                    if (StringUtils.isEmpty(query_sql)) {
                        continue
                    }

                    def query_start_time = System.currentTimeMillis()
                    boolean is_succ = true;
                    try {
                        sql.execute(query_sql)
                        succ_num.incrementAndGet()
                    } catch (Exception e) {
                        is_succ = false;
                        failed_num.incrementAndGet()
                    }
                    if (!is_succ) {
                        continue
                    }
                    int query_time = System.currentTimeMillis() - query_start_time
                    println conf.label + " " + i + "," + j + "," + k + " : " + query_time + " ms"
                    timeCost.add(query_time)
                }
            }
        })
    }

    for (Thread t : threads) {
        t.join()
    }
    // 3 print test result
    write_ret_lock.lock()
    print_ret.add("\n")
    print_ret.add("=============" + conf.label + "=============")
    print_ret.add(conf.label + " iteration=" + iteration)
    print_ret.add(conf.label + " concurrency=" + concurrency)
    calculate_tpxx(conf.label, timeCost, print_ret)
    print_ret.add(conf.label + " succ sum=" + succ_num.get())
    print_ret.add(conf.label + " failed num=" + failed_num.get())
    print_ret.add("==========================")

    write_ret_lock.unlock()
}

def t1 = Thread.start { query_func(test_conf.tiny_query) }
def t2 = Thread.start { query_func(test_conf.small_query) }

t1.join()
t2.join()

for (int i = 0; i < print_ret.size(); i++) {
    println(print_ret.get(i))
}

def end_time = System.currentTimeMillis()

println "time cost=" + (end_time - begin_time)