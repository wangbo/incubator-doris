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

package org.apache.doris.analysis;

import org.apache.doris.catalog.Catalog;
import org.apache.doris.catalog.EtlCluster;
import org.apache.doris.catalog.EtlCluster.EtlClusterType;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.EtlJobType;

import java.io.DataInput;
import java.io.IOException;
import java.util.Map;

// Etl cluster descriptor
//
// Spark example:
// WITH CLUSTER "cluster0"
// (
//     "master"="yarn",
//     "deploy_mode" = "cluster",
//     "spark_args" = "--jars=xxx.jar,yyy.jar;--files=/tmp/aaa,/tmp/bbb",
//     "spark_configs" = "spark.driver.memory=1g;spark.executor.memory=1g",
//     "yarn_configs" = "yarn.resourcemanager.address=host:port;fs.defaultFS=hdfs://host:port",
//     "hdfs_etl_path" = "hdfs://127.0.0.1:10000/tmp/doris",
//     "broker" = "broker0",
//     "broker.username" = "xxx",
//     "broker.password" = "yyy"
// )
public class EtlClusterDesc extends DataProcessorDesc {
    public static final String BROKER_PROPERTY_KEY_PREFIX = "broker.";

    private EtlClusterDesc() {
        this(null, null);
    }

    public EtlClusterDesc(String name, Map<String, String> properties) {
        super(name, properties);
    }

    @Override
    public void analyze() throws AnalysisException {
        super.analyze();

        // check etl cluster exist
        if (!Catalog.getCurrentCatalog().getEtlClusterMgr().containsEtlCluster(getName())) {
            throw new AnalysisException("Etl cluster does not exist. name: " + getName());
        }
    }

    @Override
    public EtlJobType getEtlJobType() throws DdlException {
        EtlCluster etlCluster = Catalog.getCurrentCatalog().getEtlClusterMgr().getEtlCluster(getName());
        if (etlCluster == null) {
            throw new DdlException("Etl cluster does not exist. name: " + getName());
        }

        if (etlCluster.getType() == EtlClusterType.SPARK) {
            return EtlJobType.SPARK;
        }

        return null;
    }

    public static EtlClusterDesc read(DataInput in) throws IOException {
        EtlClusterDesc etlClusterDesc = new EtlClusterDesc();
        etlClusterDesc.readFields(in);
        return etlClusterDesc;
    }
}