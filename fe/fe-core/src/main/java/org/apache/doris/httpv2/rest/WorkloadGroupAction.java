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

package org.apache.doris.httpv2.rest;

import org.apache.doris.analysis.UserIdentity;
import org.apache.doris.catalog.Env;
import org.apache.doris.common.Config;
import org.apache.doris.httpv2.entity.ResponseEntityBuilder;
import org.apache.doris.qe.ConnectContext;
import org.apache.doris.system.SystemInfoService;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

@RestController
public class WorkloadGroupAction extends RestBaseController {

    private static final String NAMESPACES = "namespaces";
    private static final String WORKLOAD_GROUPS = "workloadgroups";

    private static final String USERNAME = "username";

    @RequestMapping(path = "/api/meta/" + NAMESPACES + "/{" + NS_KEY + "}/" + WORKLOAD_GROUPS + "/{" + USERNAME + "}",
            method = {RequestMethod.GET})
    public Object getUserGrant(
            @PathVariable(value = NS_KEY) String ns,
            @PathVariable(value = USERNAME) String usernameArg,
            HttpServletRequest request, HttpServletResponse response) {
        if (StringUtils.isEmpty(usernameArg)) {
            return ResponseEntityBuilder.badRequest("username should not be empty");
        }
        boolean checkAuth = Config.enable_all_http_auth ? true : false;
        checkWithCookie(request, response, checkAuth);

        if (!ns.equalsIgnoreCase(SystemInfoService.DEFAULT_CLUSTER)) {
            return ResponseEntityBuilder.badRequest("Only support 'default_cluster' now");
        }

        UserIdentity userIdentity = null;
        String inputUserName = "";
        if (!usernameArg.contains("@")) {
            inputUserName = usernameArg;
            userIdentity = new UserIdentity(usernameArg, "%");
        } else {
            String[] ss = usernameArg.split("@");
            if (ss.length != 2) {
                return ResponseEntityBuilder.badRequest(
                        "invalid username format, expected input is username@xxx or username");
            }
            String username = ss[0].trim();
            if (StringUtils.isEmpty(username)) {
                return ResponseEntityBuilder.badRequest("username should not be empty");
            }
            String ip = ss[1].trim();
            if (StringUtils.isEmpty(ip)) {
                return ResponseEntityBuilder.badRequest("ip should not be empty ");
            }
            userIdentity = new UserIdentity(username, ip);
            inputUserName = username;
        }
        userIdentity.setIsAnalyzed();

        String loginUser = ConnectContext.get().getQualifiedUser();
        UserIdentity loginUserId = ConnectContext.get().getUserIdentity();

        if ((!loginUserId.isAdminUser() && !loginUserId.isRootUser()) && !inputUserName.equals(loginUser)) {
            return ResponseEntityBuilder.badRequest(
                    "only root/admin user can see other user's grants, current login user is " + loginUser);
        }

        List<List<String>> infos = Env.getCurrentEnv().getAuth().getAuthInfo(userIdentity);
        Map<String, List<String>> retMap = Maps.newHashMap();
        for (List<String> authLine : infos) {
            String userName = authLine.get(0).trim();
            String authString = authLine.get(13);
            List<String> wgList = new ArrayList<>();
            String[] ss1 = authString.split(";");
            for (String wgAuth : ss1) {
                String[] ss2 = wgAuth.trim().split(":");
                wgList.add(ss2[0].trim());
            }
            retMap.put(userName, wgList);
        }

        return ResponseEntityBuilder.ok(retMap);
    }
}
