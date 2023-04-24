/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.healthcheck;

import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.naming.core.Cluster;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.push.PushService;
import com.fasterxml.jackson.annotation.JsonIgnore;

import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Thread to update ephemeral instance triggered by client beat.
 *
 * @author nkorange
 */
public class ClientBeatProcessor implements Runnable {

    public static final long CLIENT_BEAT_TIMEOUT = TimeUnit.SECONDS.toMillis(15);

    private RsInfo rsInfo;

    private Service service;

    @JsonIgnore
    public PushService getPushService() {
        return ApplicationUtils.getBean(PushService.class);
    }

    public RsInfo getRsInfo() {
        return rsInfo;
    }

    public void setRsInfo(RsInfo rsInfo) {
        this.rsInfo = rsInfo;
    }

    public Service getService() {
        return service;
    }

    public void setService(Service service) {
        this.service = service;
    }

    @Override
    public void run() {
        Service service = this.service; // 获取要调度处理的服务
        if (Loggers.EVT_LOG.isDebugEnabled()) {
            Loggers.EVT_LOG.debug("[CLIENT-BEAT] processing beat: {}", rsInfo.toString());
        }

        String ip = rsInfo.getIp(); // 客户端IP
        String clusterName = rsInfo.getCluster(); // 客户端集群
        int port = rsInfo.getPort(); // 客户端端口
        Cluster cluster = service.getClusterMap().get(clusterName);
        List<Instance> instances = cluster.allIPs(true); // 获取集群下所有实例

        for (Instance instance : instances) { // 遍历实例处理
            if (instance.getIp().equals(ip) && instance.getPort() == port) { // ip port与当前相等
                if (Loggers.EVT_LOG.isDebugEnabled()) {
                    Loggers.EVT_LOG.debug("[CLIENT-BEAT] refresh beat: {}", rsInfo.toString());
                }
                instance.setLastBeat(System.currentTimeMillis()); // 修改最后心跳时间
                if (!instance.isMarked()) { // 临时实例
                    if (!instance.isHealthy()) { // 当前不是健康状态，但是收到了客户端的心跳，重新标记为健康状态并向其他nacos服务同步信息
                        instance.setHealthy(true);
                        Loggers.EVT_LOG
                                .info("service: {} {POS} {IP-ENABLED} valid: {}:{}@{}, region: {}, msg: client beat ok",
                                        cluster.getService().getName(), ip, port, cluster.getName(),
                                        UtilsAndCommons.LOCALHOST_SITE);
                        getPushService().serviceChanged(service); // 发布服务变更事件
                    }
                }
            }
        }
    }
}
