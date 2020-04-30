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

import com.google.gson.FieldNamingPolicy;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonSyntaxException;
import org.apache.doris.PaloFe;
import org.apache.doris.analysis.BrokerDesc;
import org.apache.doris.catalog.SparkEtlCluster;
import org.apache.doris.common.LoadException;
import org.apache.doris.common.UserException;
import org.apache.doris.common.util.BrokerUtil;
import org.apache.doris.load.EtlStatus;
import org.apache.doris.load.loadv2.dpp.DppResult;
import org.apache.doris.load.loadv2.etl.EtlJobConfig;
import org.apache.doris.thrift.TBrokerFileStatus;
import org.apache.doris.thrift.TEtlState;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
//import org.apache.hadoop.yarn.exceptions.ApplicationNotFoundException;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.launcher.SparkAppHandle;
import org.apache.spark.launcher.SparkAppHandle.Listener;
import org.apache.spark.launcher.SparkAppHandle.State;
import org.apache.spark.launcher.SparkLauncher;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

public class SparkEtlJobHandler {
    private static final Logger LOG = LogManager.getLogger(SparkEtlJobHandler.class);

    private static final String APP_RESOURCE_NAME = "palo-fe.jar";
    private static final String CONFIG_FILE_NAME = "jobconfig.json";
    private static final String APP_RESOURCE_LOCAL_PATH = PaloFe.DORIS_HOME_DIR + "/lib/" + APP_RESOURCE_NAME;
    private static final String JOB_CONFIG_DIR = "configs";
    private static final String MAIN_CLASS = "org.apache.doris.load.loadv2.etl.SparkEtlJob";
    private static final String ETL_JOB_NAME = "doris__%s";
    private static final String SPARK_HADOOP_CONFIG_PREFIX = "spark.hadoop.";
    // 5min
    private static final int GET_APPID_MAX_RETRY = 300;

    class SparkAppListener implements Listener {
        @Override
        public void stateChanged(SparkAppHandle sparkAppHandle) {}

        @Override
        public void infoChanged(SparkAppHandle sparkAppHandle) {}
    }

    public void submitEtlJob(long loadJobId, String loadLabel, SparkEtlCluster etlCluster, BrokerDesc brokerDesc,
                             EtlJobConfig etlJobConfig, SparkPendingTaskAttachment attachment) throws LoadException {
        // delete outputPath
        deleteEtlOutputPath(etlJobConfig.outputPath, brokerDesc);

        // upload app resource and jobconfig to hdfs
        String configsHdfsDir = etlJobConfig.outputPath + "/" + JOB_CONFIG_DIR + "/";
        String appResourceHdfsPath = configsHdfsDir + APP_RESOURCE_NAME;
        String jobConfigHdfsPath = configsHdfsDir + CONFIG_FILE_NAME;
        try {
            BrokerUtil.writeBrokerFile(APP_RESOURCE_LOCAL_PATH, appResourceHdfsPath, brokerDesc);
            byte[] configData = configToJson(etlJobConfig).getBytes("UTF-8");
            BrokerUtil.writeBrokerFile(configData, jobConfigHdfsPath, brokerDesc);
        } catch (UserException | UnsupportedEncodingException e) {
            throw new LoadException(e.getMessage());
        }

        Map<String, String> env = Maps.newHashMap();
        //env.put("HADOOP_CONF_DIR", "");
        //env.put("SPARK_HOME", "");
        SparkLauncher launcher = new SparkLauncher(env);
        // master      |  deployMode
        // ------------|-------------
        // yarn        |  cluster
        // spark://xx  |  cluster
        // spark://xx  |  client
        launcher.setMaster(etlCluster.getMaster())
                .setDeployMode(etlCluster.getDeployMode().name().toLowerCase())
                .setAppResource(appResourceHdfsPath)
                .setMainClass(MAIN_CLASS)
                .setAppName(String.format(ETL_JOB_NAME, loadLabel))
                .addAppArgs(jobConfigHdfsPath.replace("hdfs://dfsrouter.vip.sankuai.com:8888",""));
        // spark args: --jars, --files, --queue
        for (Map.Entry<String, String> entry : etlCluster.getSparkArgsMap().entrySet()) {
            launcher.addSparkArg(entry.getKey(), entry.getValue());
        }
        // spark configs
        for (Map.Entry<String, String> entry : etlCluster.getSparkConfigsMap().entrySet()) {
            launcher.setConf(entry.getKey(), entry.getValue());
        }
        // yarn configs:
        // yarn.resourcemanager.address
        // fs.defaultFS
        if (etlCluster.isYarnMaster()) {
            for (Map.Entry<String, String> entry : etlCluster.getYarnConfigsMap().entrySet()) {
                launcher.setConf(SPARK_HADOOP_CONFIG_PREFIX + entry.getKey(), entry.getValue());
            }
        }

        // start app
        SparkAppHandle handle = null;
        State state = null;
        String appId = null;
        int retry = 0;
        String errMsg = "start spark app failed. error: ";
        try {
            handle = launcher.startApplication(new SparkAppListener());
        } catch (IOException e) {
            LOG.warn(errMsg, e);
            throw new LoadException(errMsg + e.getMessage());
        }

        while (retry++ < GET_APPID_MAX_RETRY) {
            appId = handle.getAppId();
            if (appId != null) {
                break;
            }

            // check state and retry
            state = handle.getState();
            if (fromSparkState(state) == TEtlState.CANCELLED) {
                throw new LoadException(errMsg + "spark app state: " + state.toString());
            }
            if (retry >= GET_APPID_MAX_RETRY) {
                throw new LoadException(errMsg + "wait too much time for getting appid, spark app state: "
                                                + state.toString());
            }

            // log
            if (retry % 10 == 0) {
                LOG.info("spark appid that handle get is null, load job id: {}, state: {}, retry times: {}",
                         loadJobId, state.toString(), retry);
            }
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                LOG.warn(e.getMessage());
            }
        }

        // success
        attachment.setAppId(appId);
        attachment.setHandle(handle);
    }

    public EtlStatus getEtlJobStatus(SparkAppHandle handle, String appId, long loadJobId, SparkEtlCluster etlCluster,
                                     String etlOutputPath, BrokerDesc brokerDesc) {
        EtlStatus status = new EtlStatus();

        if (etlCluster.isYarnMaster()) {
            // state from yarn
            Preconditions.checkState(appId != null && !appId.isEmpty());
            try {
                CloseableHttpClient httpclient = HttpClients.createDefault();
                HttpGet httpget = new HttpGet("http://rz-data-hdp-rm01.rz.sankuai.com:8088/ws/v1/cluster/apps/" + appId);
                HttpResponse httpresponse = httpclient.execute(httpget);
                Scanner sc = new Scanner(httpresponse.getEntity().getContent());

                boolean isSucc = false;
                boolean isFailed = false;
                String ret = "";
                while(sc.hasNext()) {
                    String line = sc.nextLine();
                    if (line.contains("\"finalStatus\":\"SUCCEEDED\"")) {
                        isSucc = true;
                    } else if (line.contains("\"finalStatus\":\"FAILED\"")) {
                        isFailed = true;
                    }
                    ret = line;
                }
                LOG.info(ret);
                if (isSucc) {
                    status.setState(TEtlState.FINISHED);
                    status.setProgress(100);
                } else if (isFailed) {
                    status.setState(TEtlState.CANCELLED);
                    status.setProgress(100);
                }else {
                    status.setState(TEtlState.RUNNING);
                    status.setProgress(0);
                }
                status.setTrackingUrl(appId);
            } catch (Exception e) {
                LOG.error(e);
            }
//            YarnClient client = startYarnClient(etlCluster);
//            try {
//                ApplicationReport report = client.getApplicationReport(ConverterUtils.toApplicationId(appId));
//                LOG.info("yarn application -status {}, load job id: {}, result: {}", appId, loadJobId, report);
//
//                YarnApplicationState state = report.getYarnApplicationState();
//                FinalApplicationStatus faStatus = report.getFinalApplicationStatus();
//                status.setState(fromYarnState(state, faStatus));
//                if (status.getState() == TEtlState.CANCELLED) {
//                    if (state == YarnApplicationState.FINISHED) {
//                        status.setFailMsg("spark app state: " + faStatus.toString());
//                    } else {
//                        status.setFailMsg("yarn app state: " + state.toString());
//                    }
//                }
//                status.setTrackingUrl(report.getTrackingUrl());
//                status.setProgress((int) (report.getProgress() * 100));
//            } catch (ApplicationNotFoundException e) {
//                LOG.warn("spark app not found, spark app id: {}, load job id: {}", appId, loadJobId, e);
//                status.setState(TEtlState.CANCELLED);
//                status.setFailMsg(e.getMessage());
//            } catch (YarnException | IOException e) {
//                LOG.warn("yarn application status failed, spark app id: {}, load job id: {}", appId, loadJobId, e);
//            } finally {
//                stopYarnClient(client);
//            }
        } else {
            // state from handle
            if (handle == null) {
                status.setFailMsg("spark app handle is null");
                status.setState(TEtlState.CANCELLED);
                return status;
            }

            State state = handle.getState();
            status.setState(fromSparkState(state));
            if (status.getState() == TEtlState.CANCELLED) {
                status.setFailMsg("spark app state: " + state.toString());
            }
            LOG.info("spark app id: {}, load job id: {}, app state: {}", appId, loadJobId, state);
        }

        if (status.getState() == TEtlState.FINISHED || status.getState() == TEtlState.CANCELLED) {
            // get dpp result
            String dppResultFilePath = EtlJobConfig.getDppResultFilePath(etlOutputPath);
            try {
                String dppResultStr = BrokerUtil.readBrokerFile(dppResultFilePath, brokerDesc);
                DppResult dppResult = new Gson().fromJson(dppResultStr, DppResult.class);
                status.setDppResult(dppResult);
                if (status.getState() == TEtlState.CANCELLED) {
                    status.setFailMsg(dppResult.failedReason);
                }
            } catch (UserException | JsonSyntaxException e) {
                LOG.warn("read broker file failed, path: {}", dppResultFilePath, e);
            }
        }

        return status;
    }

    public void killEtlJob(SparkAppHandle handle, String appId, long loadJobId, SparkEtlCluster etlCluster) {
        if (etlCluster.isYarnMaster()) {
            Preconditions.checkNotNull(appId);
            YarnClient client = startYarnClient(etlCluster);
            try {
                try {
                    client.killApplication(ConverterUtils.toApplicationId(appId));
                    LOG.info("yarn application -kill {}", appId);
                } catch (YarnException | IOException e) {
                    LOG.warn("yarn application kill failed, app id: {}, load job id: {}", appId, loadJobId, e);
                }
            } finally {
                stopYarnClient(client);
            }
        } else {
            if (handle != null) {
                handle.stop();
            }
        }
    }

    public Map<String, Long> getEtlFilePaths(String outputPath, BrokerDesc brokerDesc) throws Exception {
        Map<String, Long> filePathToSize = Maps.newHashMap();

        List<TBrokerFileStatus> fileStatuses = Lists.newArrayList();
        String etlFilePaths = outputPath + "/*";
        try {
            BrokerUtil.parseBrokerFile(etlFilePaths, brokerDesc, fileStatuses);
        } catch (UserException e) {
            throw new Exception(e);
        }

        for (TBrokerFileStatus fstatus : fileStatuses) {
            if (fstatus.isDir) {
                continue;
            }
            filePathToSize.put(fstatus.getPath(), fstatus.getSize());
        }
        LOG.debug("get spark etl file paths. files map: {}", filePathToSize);

        return filePathToSize;
    }

    public void deleteEtlOutputPath(String outputPath, BrokerDesc brokerDesc) {
        try {
            BrokerUtil.deleteBrokerPath(outputPath, brokerDesc);
            LOG.info("delete path success. path: {}", outputPath);
        } catch (UserException e) {
            LOG.warn("delete path failed. path: {}", outputPath, e);
        }
    }

    private String configToJson(EtlJobConfig etlJobConfig) {
        GsonBuilder gsonBuilder = new GsonBuilder();
        gsonBuilder.setFieldNamingPolicy(FieldNamingPolicy.LOWER_CASE_WITH_UNDERSCORES);
        Gson gson = gsonBuilder.create();
        return gson.toJson(etlJobConfig);
    }

    private YarnClient startYarnClient(SparkEtlCluster etlCluster) {
        YarnClient client = YarnClient.createYarnClient();
        Configuration conf = new YarnConfiguration();
        // set yarn.resourcemanager.address
        for (Map.Entry<String, String> entry : etlCluster.getYarnConfigsMap().entrySet()) {
            conf.set(entry.getKey(), entry.getValue());
        }
        client.init(conf);
        client.start();
        return client;
    }

    private void stopYarnClient(YarnClient client) {
        client.stop();
    }

    private TEtlState fromYarnState(YarnApplicationState state, FinalApplicationStatus faStatus) {
        switch (state) {
            case FINISHED:
                if (faStatus == FinalApplicationStatus.SUCCEEDED) {
                    // finish and success
                    return TEtlState.FINISHED;
                } else {
                    // finish but fail
                    return TEtlState.CANCELLED;
                }
            case FAILED:
            case KILLED:
                // not finish
                return TEtlState.CANCELLED;
            default:
                // ACCEPTED NEW NEW_SAVING RUNNING SUBMITTED
                return TEtlState.RUNNING;
        }
    }

    private TEtlState fromSparkState(State state) {
        switch (state) {
            case FINISHED:
                return TEtlState.FINISHED;
            case FAILED:
            case KILLED:
            case LOST:
                return TEtlState.CANCELLED;
            default:
                // UNKNOWN CONNECTED SUBMITTED RUNNING
                return TEtlState.RUNNING;
        }
    }
}
