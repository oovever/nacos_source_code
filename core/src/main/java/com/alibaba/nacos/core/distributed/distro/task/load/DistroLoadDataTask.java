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

package com.alibaba.nacos.core.distributed.distro.task.load;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.DistroConfig;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Distro load data task.
 *
 * @author xiweng.yy
 */
public class DistroLoadDataTask implements Runnable {

    private final ServerMemberManager memberManager;

    private final DistroComponentHolder distroComponentHolder;

    private final DistroConfig distroConfig;

    private final DistroCallback loadCallback;

    private final Map<String, Boolean> loadCompletedMap;

    public DistroLoadDataTask(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder,
            DistroConfig distroConfig, DistroCallback loadCallback) {
        this.memberManager = memberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.distroConfig = distroConfig;
        this.loadCallback = loadCallback;
        loadCompletedMap = new HashMap<>(1);
    }

    @Override
    public void run() {
        try {
            load(); // 远程加载数据
            if (!checkCompleted()) { // 没有加载完成
                GlobalExecutor.submitLoadDataTask(this, distroConfig.getLoadDataRetryDelayMillis()); // 重新提交任务加载
            } else {
                loadCallback.onSuccess();
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot data success");
            }
        } catch (Exception e) {
            loadCallback.onFailed(e);
            Loggers.DISTRO.error("[DISTRO-INIT] load snapshot data failed. ", e);
        }
    }

    private void load() throws Exception {
        while (memberManager.allMembersWithoutSelf().isEmpty()) { // 集群节点为空 sleep 1s
            Loggers.DISTRO.info("[DISTRO-INIT] waiting server list init...");
            TimeUnit.SECONDS.sleep(1);
        }
        while (distroComponentHolder.getDataStorageTypes().isEmpty()) { // 数据类型为空，distroComponentHolder的组件注册器还未初始化完毕，不处理，休眠1秒，@PostConstruct标记的#DistroHttpRegistry.doRegister方法没执行完
            Loggers.DISTRO.info("[DISTRO-INIT] waiting distro data storage register...");
            TimeUnit.SECONDS.sleep(1);
        }
        for (String each : distroComponentHolder.getDataStorageTypes()) { // 遍历所有数据类型
            if (!loadCompletedMap.containsKey(each) || !loadCompletedMap.get(each)) { // 如果当前数据类型没有被加载
                loadCompletedMap.put(each, loadAllDataSnapshotFromRemote(each)); // 远程加载快照数据并存储
            }
        }
    }

    private boolean loadAllDataSnapshotFromRemote(String resourceType) { // 加载特定数据类型的数据
        DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
        DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
        if (null == transportAgent || null == dataProcessor) {
            Loggers.DISTRO.warn("[DISTRO-INIT] Can't find component for type {}, transportAgent: {}, dataProcessor: {}",
                    resourceType, transportAgent, dataProcessor);
            return false;
        }
        for (Member each : memberManager.allMembersWithoutSelf()) { // 遍历除自己外的所有服务器，这里有问题，应该遍历的是存活的服务器
            try {
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot {} from {}", resourceType, each.getAddress());
                DistroData distroData = transportAgent.getDatumSnapshot(each.getAddress()); // 根据服务器地址，远程获取快照数据distro/datums
                boolean result = dataProcessor.processSnapshot(distroData); // 处理获取到的数据
                Loggers.DISTRO
                        .info("[DISTRO-INIT] load snapshot {} from {} result: {}", resourceType, each.getAddress(),
                                result);
                if (result) {
                    return true;
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("[DISTRO-INIT] load snapshot {} from {} failed.", resourceType, each.getAddress(), e); // 发送失败，提示异常
            }
        }
        return false;
    }

    private boolean checkCompleted() {
        if (distroComponentHolder.getDataStorageTypes().size() != loadCompletedMap.size()) { // 存储类型不等于外层的size
            return false;
        }
        for (Boolean each : loadCompletedMap.values()) {
            if (!each) {
                return false;
            }
        }
        return true;
    }
}
