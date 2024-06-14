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

package org.apache.doris.resource;

import org.apache.doris.common.ClientPool;
import org.apache.doris.common.Config;
import org.apache.doris.common.util.MasterDaemon;
import org.apache.doris.system.Backend;
import org.apache.doris.system.SystemInfoService;
import org.apache.doris.thrift.BackendService;
import org.apache.doris.thrift.TGetResourceUsageRequest;
import org.apache.doris.thrift.TGetResourceUsageResult;
import org.apache.doris.thrift.TNetworkAddress;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collection;

public class AdmissionControl extends MasterDaemon {

    private static final Logger LOG = LogManager.getLogger(AdmissionControl.class);

    private volatile boolean isMemoryEnough = true;

    private SystemInfoService clusterInfoService;

    public AdmissionControl(SystemInfoService clusterInfoService) {
        super("get-be-resource-usage-thread", Config.get_be_resource_usage_interval_ms);
        this.clusterInfoService = clusterInfoService;
    }

    @Override
    protected void runAfterCatalogReady() {
        getBeMemoryUsage();
        notifyWaitingMemoryQuery();
    }

    private void getBeMemoryUsage() {
        Collection<Backend> backends = clusterInfoService.getIdToBackend().values();
        boolean tmpIsMemoryEnough = true;
        for (Backend be : backends) {
            if (!be.isAlive()) {
                continue;
            }
            if (!tmpIsMemoryEnough) {
                break;
            }

            BackendService.Client client = null;
            TNetworkAddress address = null;
            boolean ok = false;
            try {
                address = new TNetworkAddress(be.getHost(), be.getBePort());
                client = ClientPool.backendPool.borrowObject(address);
                TGetResourceUsageResult ret = client.getResourceUsage(new TGetResourceUsageRequest());
                double memUsagePercent = ret.getMemoryUsagePercent();
                tmpIsMemoryEnough &= memUsagePercent < Config.query_queue_by_be_used_memory_percent;
                ok = true;
                LOG.info("get be memory usage success, {}", memUsagePercent);
            } catch (Exception e) {
                LOG.warn("error happens when get be {} memory",
                        be.getHost(), e);
            } finally {
                try {
                    if (ok) {
                        ClientPool.backendPool.returnObject(address, client);
                    } else {
                        ClientPool.backendPool.invalidateObject(address, client);
                    }
                } catch (Throwable e) {
                    LOG.warn("recycle topic publish client failed. related backend[{}]", be.getHost(),
                            e);
                }
            }
        }
        isMemoryEnough = tmpIsMemoryEnough;
    }

    public void notifyWaitingMemoryQuery() {
    }

    public boolean getIsMemoryEnough() {
        return isMemoryEnough;
    }

}
