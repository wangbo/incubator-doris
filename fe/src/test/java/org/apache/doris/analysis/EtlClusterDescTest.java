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
import org.apache.doris.catalog.EtlClusterMgr;
import org.apache.doris.catalog.SparkEtlCluster;
import org.apache.doris.common.AnalysisException;
import org.apache.doris.common.DdlException;
import org.apache.doris.load.EtlJobType;

import com.google.common.collect.Maps;
import org.junit.Assert;
import org.junit.Test;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;

import java.util.Map;

public class EtlClusterDescTest {

    @Test
    public void testNormal(@Mocked Catalog catalog, @Injectable EtlClusterMgr etlClusterMgr)
            throws AnalysisException, DdlException {
        String clusterName = "cluster0";
        Map<String, String> properties = Maps.newHashMap();
        String key = "outputPath";
        String value = "/tmp/output";
        properties.put(key, value);
        EtlClusterDesc etlClusterDesc = new EtlClusterDesc(clusterName, properties);
        SparkEtlCluster etlCluster = new SparkEtlCluster(clusterName);

        new Expectations() {
            {
                catalog.getEtlClusterMgr();
                result = etlClusterMgr;
                etlClusterMgr.getEtlCluster(clusterName);
                result = etlCluster;
            }
        };

        Assert.assertEquals(clusterName, etlClusterDesc.getName());
        Assert.assertEquals(value, etlClusterDesc.getProperties().get(key));
        Assert.assertEquals(EtlJobType.SPARK, etlClusterDesc.getEtlJobType());
    }

    @Test(expected = AnalysisException.class)
    public void testNoTable(@Mocked Catalog catalog, @Injectable EtlClusterMgr etlClusterMgr) throws AnalysisException {
        String clusterName = "cluster1";
        EtlClusterDesc etlClusterDesc = new EtlClusterDesc(clusterName, null);

        new Expectations() {
            {
                catalog.getEtlClusterMgr();
                result = etlClusterMgr;
                etlClusterMgr.containsEtlCluster(clusterName);
                result = false;
            }
        };

        etlClusterDesc.analyze();
    }
}
