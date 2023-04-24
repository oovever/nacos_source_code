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

package com.alibaba.nacos.naming.core;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.utils.IPUtil;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.consistency.ConsistencyService;
import com.alibaba.nacos.naming.consistency.Datum;
import com.alibaba.nacos.naming.consistency.KeyBuilder;
import com.alibaba.nacos.naming.consistency.RecordListener;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeer;
import com.alibaba.nacos.naming.consistency.persistent.raft.RaftPeerSet;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.Message;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.naming.misc.ServiceStatusSynchronizer;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.Synchronizer;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.InstanceOperationContext;
import com.alibaba.nacos.naming.pojo.InstanceOperationInfo;
import com.alibaba.nacos.naming.push.PushService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.Sets;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.springframework.util.CollectionUtils;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_REMOVE;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_UPDATE;

/**
 * Core manager storing all services in Nacos.
 * nacos 服务（service）管理
 * @author nkorange
 */
@Component
public class ServiceManager implements RecordListener<Service> {

    /**
     * Map(namespace, Map(group::serviceName, Service)).
     * 双层map key namespaceId
     * 第二层map key group::serviceName value 服务
     * 服务注册表
     */
    private final Map<String, Map<String, Service>> serviceMap = new ConcurrentHashMap<>();

    private final LinkedBlockingDeque<ServiceKey> toBeUpdatedServicesQueue = new LinkedBlockingDeque<>(1024 * 1024);

    private final Synchronizer synchronizer = new ServiceStatusSynchronizer();

    private final Lock lock = new ReentrantLock();

    @Resource(name = "consistencyDelegate")
    private ConsistencyService consistencyService;

    private final SwitchDomain switchDomain;

    private final DistroMapper distroMapper;

    private final ServerMemberManager memberManager;

    private final PushService pushService;

    private final RaftPeerSet raftPeerSet;

    private int maxFinalizeCount = 3;

    private final Object putServiceLock = new Object();

    @Value("${nacos.naming.empty-service.auto-clean:false}")
    private boolean emptyServiceAutoClean;

    @Value("${nacos.naming.empty-service.clean.initial-delay-ms:60000}")
    private int cleanEmptyServiceDelay;

    @Value("${nacos.naming.empty-service.clean.period-time-ms:20000}")
    private int cleanEmptyServicePeriod;

    public ServiceManager(SwitchDomain switchDomain, DistroMapper distroMapper, ServerMemberManager memberManager,
            PushService pushService, RaftPeerSet raftPeerSet) {
        this.switchDomain = switchDomain;
        this.distroMapper = distroMapper;
        this.memberManager = memberManager;
        this.pushService = pushService;
        this.raftPeerSet = raftPeerSet;
    }

    /**
     * Init service maneger.
     */
    @PostConstruct
    public void init() {
//       发送本机注册表到其他nacos server端；以各个服务的checksum(字串拼接)形式被发送
        GlobalExecutor.scheduleServiceReporter(new ServiceReporter(), 60000, TimeUnit.MILLISECONDS);
//      从nacos服务端获取注册表数据健康状态更新到本机
        GlobalExecutor.submitServiceUpdateManager(new UpdatedServiceProcessor());

        if (emptyServiceAutoClean) {

            Loggers.SRV_LOG.info("open empty service auto clean job, initialDelay : {} ms, period : {} ms",
                    cleanEmptyServiceDelay, cleanEmptyServicePeriod);

            // delay 60s, period 20s;

            // This task is not recommended to be performed frequently in order to avoid
            // the possibility that the service cache information may just be deleted
            // and then created due to the heartbeat mechanism

            GlobalExecutor.scheduleServiceAutoClean(new EmptyServiceAutoClean(), cleanEmptyServiceDelay,
                    cleanEmptyServicePeriod); // 启动定时任务清理空service(没有instance的服务) 20s执行一次
        }

        try {
            Loggers.SRV_LOG.info("listen for service meta change");
            consistencyService.listen(KeyBuilder.SERVICE_META_KEY_PREFIX, this);
        } catch (NacosException e) {
            Loggers.SRV_LOG.error("listen for service meta change failed!");
        }
    }

    public Map<String, Service> chooseServiceMap(String namespaceId) {
        return serviceMap.get(namespaceId);
    }

    /**
     * Add a service into queue to update.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param serverIP    target server ip
     * @param checksum    checksum of service
     */
    public void addUpdatedServiceToQueue(String namespaceId, String serviceName, String serverIP, String checksum) {
        lock.lock();
        try {
            toBeUpdatedServicesQueue
                    .offer(new ServiceKey(namespaceId, serviceName, serverIP, checksum), 5, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            toBeUpdatedServicesQueue.poll();
            toBeUpdatedServicesQueue.add(new ServiceKey(namespaceId, serviceName, serverIP, checksum));
            Loggers.SRV_LOG.error("[DOMAIN-STATUS] Failed to add service to be updated to queue.", e);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public boolean interests(String key) {
        return KeyBuilder.matchServiceMetaKey(key) && !KeyBuilder.matchSwitchKey(key);
    }

    @Override
    public boolean matchUnlistenKey(String key) {
        return KeyBuilder.matchServiceMetaKey(key) && !KeyBuilder.matchSwitchKey(key);
    }

    @Override
    public void onChange(String key, Service service) throws Exception {
        try {
            if (service == null) {
                Loggers.SRV_LOG.warn("received empty push from raft, key: {}", key);
                return;
            }

            if (StringUtils.isBlank(service.getNamespaceId())) { // 设置默认的nameSpace
                service.setNamespaceId(Constants.DEFAULT_NAMESPACE_ID);
            }

            Loggers.RAFT.info("[RAFT-NOTIFIER] datum is changed, key: {}, value: {}", key, service);

            Service oldDom = getService(service.getNamespaceId(), service.getName()); // 获取旧的服务实例

            if (oldDom != null) {
                oldDom.update(service); // 旧的不为空，更新服务实例
                // re-listen to handle the situation when the underlying listener is removed: 添加监听器
                consistencyService
                        .listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), true),
                                oldDom);
                consistencyService
                        .listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), false),
                                oldDom);
            } else {
                putServiceAndInit(service); // 添加并初始化service
            }
        } catch (Throwable e) {
            Loggers.SRV_LOG.error("[NACOS-SERVICE] error while processing service update", e);
        }
    }

    @Override
    public void onDelete(String key) throws Exception {
        String namespace = KeyBuilder.getNamespace(key);
        String name = KeyBuilder.getServiceName(key);
        Service service = chooseServiceMap(namespace).get(name);
        Loggers.RAFT.info("[RAFT-NOTIFIER] datum is deleted, key: {}", key);

        if (service != null) {
            service.destroy();
            String ephemeralInstanceListKey = KeyBuilder.buildInstanceListKey(namespace, name, true);
            String persistInstanceListKey = KeyBuilder.buildInstanceListKey(namespace, name, false);
            consistencyService.remove(ephemeralInstanceListKey);
            consistencyService.remove(persistInstanceListKey);

            // remove listeners of key to avoid mem leak
            consistencyService.unListen(ephemeralInstanceListKey, service);
            consistencyService.unListen(persistInstanceListKey, service);
            consistencyService.unListen(KeyBuilder.buildServiceMetaKey(namespace, name), service);
            Loggers.SRV_LOG.info("[DEAD-SERVICE] {}", service.toJson());
        }

        chooseServiceMap(namespace).remove(name);
    }

    private class UpdatedServiceProcessor implements Runnable {

        //get changed service from other server asynchronously
        @Override
        public void run() { // 获取其他服务实例变更，应用到本地服务
            ServiceKey serviceKey = null;

            try {
                while (true) {
                    try {
                        serviceKey = toBeUpdatedServicesQueue.take(); // 队列中获取其他server变更的服务
                    } catch (Exception e) {
                        Loggers.EVT_LOG.error("[UPDATE-DOMAIN] Exception while taking item from LinkedBlockingDeque.");
                    }

                    if (serviceKey == null) {
                        continue;
                    }
                    GlobalExecutor.submitServiceUpdate(new ServiceUpdater(serviceKey)); // 异步处理变更
                }
            } catch (Exception e) {
                Loggers.EVT_LOG.error("[UPDATE-DOMAIN] Exception while update service: {}", serviceKey, e);
            }
        }
    }

    private class ServiceUpdater implements Runnable {

        String namespaceId;

        String serviceName;

        String serverIP;

        public ServiceUpdater(ServiceKey serviceKey) {
            this.namespaceId = serviceKey.getNamespaceId();
            this.serviceName = serviceKey.getServiceName();
            this.serverIP = serviceKey.getServerIP();
        }

        @Override
        public void run() {
            try {
                updatedHealthStatus(namespaceId, serviceName, serverIP); // 更新健康状态
            } catch (Exception e) {
                Loggers.SRV_LOG
                        .warn("[DOMAIN-UPDATER] Exception while update service: {} from {}, error: {}", serviceName,
                                serverIP, e);
            }
        }
    }

    public RaftPeer getMySelfClusterState() {
        return raftPeerSet.local();
    }

    /**
     * Update health status of instance in service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param serverIP    source server Ip
     */
    public void updatedHealthStatus(String namespaceId, String serviceName, String serverIP) {
        Message msg = synchronizer.get(serverIP, UtilsAndCommons.assembleFullServiceName(namespaceId, serviceName));// 从其他nacos服务获取指定服务的数据
        JsonNode serviceJson = JacksonUtils.toObj(msg.getData()); // json解析

        ArrayNode ipList = (ArrayNode) serviceJson.get("ips"); // 获取其他服务的所有实例信息
        Map<String, String> ipsMap = new HashMap<>(ipList.size()); // key:ip:port value:健康状态
        for (int i = 0; i < ipList.size(); i++) {

            String ip = ipList.get(i).asText(); // ip:port_healthy
            String[] strings = ip.split("_");
            ipsMap.put(strings[0], strings[1]); // map处理
        }

        Service service = getService(namespaceId, serviceName); // 本地注册表获取当前服务

        if (service == null) { // 服务为空直接返回
            return;
        }

        boolean changed = false;

        List<Instance> instances = service.allIPs(); // 获取本地服务的所有实例
        for (Instance instance : instances) { // 遍历处理实例

            boolean valid = Boolean.parseBoolean(ipsMap.get(instance.toIpAddr())); // 获取其他实例传来的健康状态
            if (valid != instance.isHealthy()) { // 健康状态与本地不一致
                changed = true;  // 记录本地实例已经改变
                instance.setHealthy(valid); // 修改本地实例健康状态
                Loggers.EVT_LOG.info("{} {SYNC} IP-{} : {}:{}@{}", serviceName,
                        (instance.isHealthy() ? "ENABLED" : "DISABLED"), instance.getIp(), instance.getPort(),
                        instance.getClusterName());
            }
        }

        if (changed) {
            pushService.serviceChanged(service); // 发布事件变更
            if (Loggers.EVT_LOG.isDebugEnabled()) {
                StringBuilder stringBuilder = new StringBuilder();
                List<Instance> allIps = service.allIPs();
                for (Instance instance : allIps) {
                    stringBuilder.append(instance.toIpAddr()).append("_").append(instance.isHealthy()).append(",");
                }
                Loggers.EVT_LOG
                        .debug("[HEALTH-STATUS-UPDATED] namespace: {}, service: {}, ips: {}", service.getNamespaceId(),
                                service.getName(), stringBuilder.toString());
            }
        }

    }

    public Set<String> getAllServiceNames(String namespaceId) {
        return serviceMap.get(namespaceId).keySet();
    }

    public Map<String, Set<String>> getAllServiceNames() {

        Map<String, Set<String>> namesMap = new HashMap<>(16);
        for (String namespaceId : serviceMap.keySet()) {
            namesMap.put(namespaceId, serviceMap.get(namespaceId).keySet());
        }
        return namesMap;
    }

    public Set<String> getAllNamespaces() {
        return serviceMap.keySet();
    }

    public List<String> getAllServiceNameList(String namespaceId) {
        if (chooseServiceMap(namespaceId) == null) {
            return new ArrayList<>();
        }
        return new ArrayList<>(chooseServiceMap(namespaceId).keySet());
    }

    public Map<String, Set<Service>> getResponsibleServices() {
        Map<String, Set<Service>> result = new HashMap<>(16);
        for (String namespaceId : serviceMap.keySet()) {
            result.put(namespaceId, new HashSet<>());
            for (Map.Entry<String, Service> entry : serviceMap.get(namespaceId).entrySet()) {
                Service service = entry.getValue();
                if (distroMapper.responsible(entry.getKey())) {
                    result.get(namespaceId).add(service);
                }
            }
        }
        return result;
    }

    public int getResponsibleServiceCount() {
        int serviceCount = 0;
        for (String namespaceId : serviceMap.keySet()) {
            for (Map.Entry<String, Service> entry : serviceMap.get(namespaceId).entrySet()) {
                if (distroMapper.responsible(entry.getKey())) {
                    serviceCount++;
                }
            }
        }
        return serviceCount;
    }

    public int getResponsibleInstanceCount() {
        Map<String, Set<Service>> responsibleServices = getResponsibleServices();
        int count = 0;
        for (String namespaceId : responsibleServices.keySet()) {
            for (Service service : responsibleServices.get(namespaceId)) {
                count += service.allIPs().size();
            }
        }

        return count;
    }

    /**
     * Fast remove service.
     *
     * <p>Remove service bu async.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @throws Exception exception
     */
    public void easyRemoveService(String namespaceId, String serviceName) throws Exception {

        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            throw new IllegalArgumentException("specified service not exist, serviceName : " + serviceName);
        }

        consistencyService.remove(KeyBuilder.buildServiceMetaKey(namespaceId, serviceName)); // 对nacos中所有服务执行删除serviceName
    }

    public void addOrReplaceService(Service service) throws NacosException {
        consistencyService.put(KeyBuilder.buildServiceMetaKey(service.getNamespaceId(), service.getName()), service);
    }

    public void createEmptyService(String namespaceId, String serviceName, boolean local) throws NacosException {
        createServiceIfAbsent(namespaceId, serviceName, local, null); // 服务不存在则创建
    }

    /**
     * Create service if not exist.
     *
     * @param namespaceId namespace
     * @param serviceName service name groupName@@serviceName
     * @param local       whether create service by local
     * @param cluster     cluster
     * @throws NacosException nacos exception
     */
    public void createServiceIfAbsent(String namespaceId, String serviceName, boolean local, Cluster cluster)
            throws NacosException {
        Service service = getService(namespaceId, serviceName); // 根据命名空间和服务名称获取服务
        if (service == null) { // 服务为空处理

            Loggers.SRV_LOG.info("creating empty service {}:{}", namespaceId, serviceName);
            service = new Service(); // 创建服务
            service.setName(serviceName);
            service.setNamespaceId(namespaceId);
            service.setGroupName(NamingUtils.getGroupName(serviceName));
            // now validate the service. if failed, exception will be thrown
            service.setLastModifiedMillis(System.currentTimeMillis());
            service.recalculateChecksum();
            if (cluster != null) { // 设置cluster
                cluster.setService(service); // 设置集群和服务之间的关系 一个服务中可能包含多个集群，一个集群有多个服务实例
//                服务-》集群-》实例 实例隶属于某一个集群
                service.getClusterMap().put(cluster.getName(), cluster);
            }
            service.validate();

            putServiceAndInit(service); // 添加服务到map并初始化，注册到serviceMap
            if (!local) {
                addOrReplaceService(service);
            }
        }
    }

    /**
     * Register an instance to a service in AP mode.
     *
     * <p>This method creates service or cluster silently if they don't exist.
     *
     * @param namespaceId id of namespace
     * @param serviceName service name
     * @param instance    instance to register
     * @throws Exception any error occurred in the process
     */
    public void registerInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {

        createEmptyService(namespaceId, serviceName, instance.isEphemeral()); // 创建空服务

        Service service = getService(namespaceId, serviceName); // serviceMap中获取service

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                    "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }

        addInstance(namespaceId, serviceName, instance.isEphemeral(), instance); // 实例注册到service
    }

    /**
     * Update instance to service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param instance    instance
     * @throws NacosException nacos exception
     */
    public void updateInstance(String namespaceId, String serviceName, Instance instance) throws NacosException {

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                    "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }

        if (!service.allIPs().contains(instance)) {
            throw new NacosException(NacosException.INVALID_PARAM, "instance not exist: " + instance);
        }

        addInstance(namespaceId, serviceName, instance.isEphemeral(), instance);
    }

    /**
     * Update instance's metadata.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param action      update or remove
     * @param ips         need update instances
     * @param metadata    target metadata
     * @return update succeed instances
     * @throws NacosException nacos exception
     */
    public List<Instance> updateMetadata(String namespaceId, String serviceName, boolean isEphemeral, String action,
            boolean all, List<Instance> ips, Map<String, String> metadata) throws NacosException {

        Service service = getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.INVALID_PARAM,
                    "service not found, namespace: " + namespaceId + ", service: " + serviceName);
        }

        List<Instance> locatedInstance = getLocatedInstance(namespaceId, serviceName, isEphemeral, all, ips);

        if (CollectionUtils.isEmpty(locatedInstance)) {
            throw new NacosException(NacosException.INVALID_PARAM, "not locate instances, input instances: " + ips);
        }

        if (UPDATE_INSTANCE_METADATA_ACTION_UPDATE.equals(action)) {
            locatedInstance.forEach(ele -> ele.getMetadata().putAll(metadata));
        } else if (UPDATE_INSTANCE_METADATA_ACTION_REMOVE.equals(action)) {
            Set<String> removeKeys = metadata.keySet();
            for (String removeKey : removeKeys) {
                locatedInstance.forEach(ele -> ele.getMetadata().remove(removeKey));
            }
        }
        Instance[] instances = new Instance[locatedInstance.size()];
        locatedInstance.toArray(instances);

        addInstance(namespaceId, serviceName, isEphemeral, instances);

        return locatedInstance;
    }

    /**
     * locate consistency's datum by all or instances provided.
     *
     * @param namespaceId        namespace
     * @param serviceName        serviceName
     * @param isEphemeral        isEphemeral
     * @param all                get from consistencyService directly
     * @param waitLocateInstance instances provided
     * @return located instances
     * @throws NacosException nacos exception
     */
    public List<Instance> getLocatedInstance(String namespaceId, String serviceName, boolean isEphemeral, boolean all,
            List<Instance> waitLocateInstance) throws NacosException {
        List<Instance> locatedInstance;

        //need the newest data from consistencyService
        Datum datum = consistencyService.get(KeyBuilder.buildInstanceListKey(namespaceId, serviceName, isEphemeral));
        if (datum == null) {
            throw new NacosException(NacosException.NOT_FOUND,
                    "instances from consistencyService not exist, namespace: " + namespaceId + ", service: "
                            + serviceName + ", ephemeral: " + isEphemeral);
        }

        if (all) {
            locatedInstance = ((Instances) datum.value).getInstanceList();
        } else {
            locatedInstance = new ArrayList<>();
            for (Instance instance : waitLocateInstance) {
                Instance located = locateInstance(((Instances) datum.value).getInstanceList(), instance);
                if (located == null) {
                    continue;
                }
                locatedInstance.add(located);
            }
        }

        return locatedInstance;
    }

    private Instance locateInstance(List<Instance> instances, Instance instance) {
        int target = 0;
        while (target >= 0) {
            target = instances.indexOf(instance);
            if (target >= 0) {
                Instance result = instances.get(target);
                if (result.getClusterName().equals(instance.getClusterName())) {
                    return result;
                }
                instances.remove(target);
            }
        }
        return null;
    }

    /**
     * Add instance to service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param ephemeral   whether instance is ephemeral
     * @param ips         instances
     * @throws NacosException nacos exception
     */
    public void addInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips)
            throws NacosException {

        String key = KeyBuilder.buildInstanceListKey(namespaceId, serviceName, ephemeral);  // 构造key ephemeral是否临时实例

        Service service = getService(namespaceId, serviceName); // 从serviceMap中获取服务

        synchronized (service) {
//          处理更新、新增、删除、返回服务下应该有的实例列表
            List<Instance> instanceList = addIpAddresses(service, ephemeral, ips);  // service对象的实例列表在这一步并没有更新，后续线程从Notifier的task队列中获取任务，然后执行Handle进行事件（change,delete）事件通知，执行onChange/onDelete方法，才会更新
            Instances instances = new Instances();
            instances.setInstanceList(instanceList);

            consistencyService.put(key, instances); // 本次变更同步更新到本地注册表，同时同步更新到集群中其他nacos节点
        }
    }

    /**
     * Remove instance from service.
     *
     * @param namespaceId namespace
     * @param serviceName service name
     * @param ephemeral   whether instance is ephemeral
     * @param ips         instances
     * @throws NacosException nacos exception
     */
    public void removeInstance(String namespaceId, String serviceName, boolean ephemeral, Instance... ips)
            throws NacosException {
        Service service = getService(namespaceId, serviceName);

        synchronized (service) {
            removeInstance(namespaceId, serviceName, ephemeral, service, ips);
        }
    }
    // 实例下线
    private void removeInstance(String namespaceId, String serviceName, boolean ephemeral, Service service,
            Instance... ips) throws NacosException {

        String key = KeyBuilder.buildInstanceListKey(namespaceId, serviceName, ephemeral); // 构建key

        List<Instance> instanceList = substractIpAddresses(service, ephemeral, ips); // 删除注册表中的实例，并返回剩下的实例集合

        Instances instances = new Instances();
        instances.setInstanceList(instanceList);

        consistencyService.put(key, instances); // 变更同步到其他nacos
    }

    public Instance getInstance(String namespaceId, String serviceName, String cluster, String ip, int port) {
        Service service = getService(namespaceId, serviceName);
        if (service == null) {
            return null;
        }

        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);

        List<Instance> ips = service.allIPs(clusters);
        if (ips == null || ips.isEmpty()) {
            return null;
        }

        for (Instance instance : ips) {
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                return instance;
            }
        }

        return null;
    }

    /**
     * batch operate kinds of resources.
     *
     * @param namespace       namespace.
     * @param operationInfo   operation resources description.
     * @param operateFunction some operation defined by kinds of situation.
     */
    public List<Instance> batchOperate(String namespace, InstanceOperationInfo operationInfo,
            Function<InstanceOperationContext, List<Instance>> operateFunction) {
        List<Instance> operatedInstances = new ArrayList<>();
        try {
            String serviceName = operationInfo.getServiceName();
            NamingUtils.checkServiceNameFormat(serviceName);
            // type: ephemeral/persist
            InstanceOperationContext operationContext;
            String type = operationInfo.getConsistencyType();
            if (!StringUtils.isEmpty(type)) {
                switch (type) {
                    case UtilsAndCommons.EPHEMERAL:
                        operationContext = new InstanceOperationContext(namespace, serviceName, true, true);
                        operatedInstances.addAll(operateFunction.apply(operationContext));
                        break;
                    case UtilsAndCommons.PERSIST:
                        operationContext = new InstanceOperationContext(namespace, serviceName, false, true);
                        operatedInstances.addAll(operateFunction.apply(operationContext));
                        break;
                    default:
                        Loggers.SRV_LOG
                                .warn("UPDATE-METADATA: services.all value is illegal, it should be ephemeral/persist. ignore the service '"
                                        + serviceName + "'");
                        break;
                }
            } else {
                List<Instance> instances = operationInfo.getInstances();
                if (!CollectionUtils.isEmpty(instances)) {
                    //ephemeral:instances or persist:instances
                    Map<Boolean, List<Instance>> instanceMap = instances.stream()
                            .collect(Collectors.groupingBy(ele -> ele.isEphemeral()));

                    for (Map.Entry<Boolean, List<Instance>> entry : instanceMap.entrySet()) {
                        operationContext = new InstanceOperationContext(namespace, serviceName, entry.getKey(), false,
                                entry.getValue());
                        operatedInstances.addAll(operateFunction.apply(operationContext));
                    }
                }
            }
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("UPDATE-METADATA: update metadata failed, ignore the service '" + operationInfo
                    .getServiceName() + "'", e);
        }
        return operatedInstances;
    }

    /**
     * Compare and get new instance list.
     *
     * @param service   service
     * @param action    {@link UtilsAndCommons#UPDATE_INSTANCE_ACTION_REMOVE} or {@link UtilsAndCommons#UPDATE_INSTANCE_ACTION_ADD}
     * @param ephemeral whether instance is ephemeral
     * @param ips       instances
     * @return instance list after operation
     * @throws NacosException nacos exception
     */
    public List<Instance> updateIpAddresses(Service service, String action, boolean ephemeral, Instance... ips)
            throws NacosException {
//    service.getName=group@@serviceName
        Datum datum = consistencyService
                .get(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), ephemeral)); // 通过dataStore.get(key)获取当前服务节点(临时实例)，Datum节点中包含服务所有实例(老的服务实例)

        List<Instance> currentIPs = service.allIPs(ephemeral); // 本地service所有的实例
        Map<String, Instance> currentInstances = new HashMap<>(currentIPs.size());
        Set<String> currentInstanceIds = Sets.newHashSet();

        for (Instance instance : currentIPs) { // 遍历本地的实例
            currentInstances.put(instance.toIpAddr(), instance); // 记录当前遍历的实例 key ip port value instnce
            currentInstanceIds.add(instance.getInstanceId()); // 实例id记录
        }

        Map<String, Instance> instanceMap;
        if (datum != null && null != datum.value) { // 如果Datum节点不为空，且节点的value(旧实例列表)不为空
//            已经有旧的数据，覆盖旧的实例列表
            instanceMap = setValid(((Instances) datum.value).getInstanceList(), currentInstances);
        } else {
            instanceMap = new HashMap<>(ips.length); // 没有旧数据直接创建新map
        }

        for (Instance instance : ips) { // 获取要注册的实例
            if (!service.getClusterMap().containsKey(instance.getClusterName())) { // 当前service的clusterMap中不包含要注册实例的集群名称
                Cluster cluster = new Cluster(instance.getClusterName(), service); // 创建新集群
                cluster.init(); // 初始化集群健康检测
                service.getClusterMap().put(instance.getClusterName(), cluster);
                Loggers.SRV_LOG
                        .warn("cluster: {} not found, ip: {}, will create new cluster with default configuration.",
                                instance.getClusterName(), instance.toJson());
            }

            if (UtilsAndCommons.UPDATE_INSTANCE_ACTION_REMOVE.equals(action)) { // 清除操作处理
                instanceMap.remove(instance.getDatumKey()); // 从instanceMap中删除该实例
            } else { // 新增实例
                Instance oldInstance = instanceMap.get(instance.getDatumKey());
                if (oldInstance != null) { // 旧实例不为空，还使用旧实例id
                    instance.setInstanceId(oldInstance.getInstanceId());
                } else {
                    instance.setInstanceId(instance.generateInstanceId(currentInstanceIds)); // 创建新实例id
                }
                instanceMap.put(instance.getDatumKey(), instance);
            }

        }

        if (instanceMap.size() <= 0 && UtilsAndCommons.UPDATE_INSTANCE_ACTION_ADD.equals(action)) {
            throw new IllegalArgumentException(
                    "ip list can not be empty, service: " + service.getName() + ", ip list: " + JacksonUtils
                            .toJson(instanceMap.values()));
        }

        return new ArrayList<>(instanceMap.values());
    }

    private List<Instance> substractIpAddresses(Service service, boolean ephemeral, Instance... ips)
            throws NacosException {
        return updateIpAddresses(service, UtilsAndCommons.UPDATE_INSTANCE_ACTION_REMOVE, ephemeral, ips);
    }

    private List<Instance> addIpAddresses(Service service, boolean ephemeral, Instance... ips) throws NacosException {
        return updateIpAddresses(service, UtilsAndCommons.UPDATE_INSTANCE_ACTION_ADD, ephemeral, ips); // 修改服务的实例列表
    }

    private Map<String, Instance> setValid(List<Instance> oldInstances, Map<String, Instance> map) {

        Map<String, Instance> instanceMap = new HashMap<>(oldInstances.size());
        for (Instance instance : oldInstances) { // 遍历旧实例列表
            Instance instance1 = map.get(instance.toIpAddr()); // 本地包含旧实例列表数据
            if (instance1 != null) { // 替换
                instance.setHealthy(instance1.isHealthy());
                instance.setLastBeat(instance1.getLastBeat());
            }
            instanceMap.put(instance.getDatumKey(), instance);
        }
        return instanceMap;
    }

    public Service getService(String namespaceId, String serviceName) {
        if (serviceMap.get(namespaceId) == null) {
            return null;
        }
        return chooseServiceMap(namespaceId).get(serviceName);
    }

    public boolean containService(String namespaceId, String serviceName) {
        return getService(namespaceId, serviceName) != null;
    }

    /**
     * Put service into manager.
     *
     * @param service service
     */
    public void putService(Service service) {
        if (!serviceMap.containsKey(service.getNamespaceId())) {
            synchronized (putServiceLock) {
                if (!serviceMap.containsKey(service.getNamespaceId())) {
                    serviceMap.put(service.getNamespaceId(), new ConcurrentSkipListMap<>());
                }
            }
        }
        serviceMap.get(service.getNamespaceId()).put(service.getName(), service); // 添加服务到map中
    }

    private void putServiceAndInit(Service service) throws NacosException {
        putService(service); // 添加到serviceMap 双层map
        service.init(); // 初始化service健康检测服务
        consistencyService
                .listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), true), service); // 持久实例添加监听
        consistencyService
                .listen(KeyBuilder.buildInstanceListKey(service.getNamespaceId(), service.getName(), false), service); // 临时实例添加监听
        Loggers.SRV_LOG.info("[NEW-SERVICE] {}", service.toJson());
    }

    /**
     * Search services.
     *
     * @param namespaceId namespace
     * @param regex       search regex
     * @return list of service which searched
     */
    public List<Service> searchServices(String namespaceId, String regex) {
        List<Service> result = new ArrayList<>();
        for (Map.Entry<String, Service> entry : chooseServiceMap(namespaceId).entrySet()) {
            Service service = entry.getValue();
            String key = service.getName() + ":" + ArrayUtils.toString(service.getOwners());
            if (key.matches(regex)) {
                result.add(service);
            }
        }

        return result;
    }

    public int getServiceCount() {
        int serviceCount = 0;
        for (String namespaceId : serviceMap.keySet()) {
            serviceCount += serviceMap.get(namespaceId).size();
        }
        return serviceCount;
    }

    public int getInstanceCount() {
        int total = 0;
        for (String namespaceId : serviceMap.keySet()) {
            for (Service service : serviceMap.get(namespaceId).values()) {
                total += service.allIPs().size();
            }
        }
        return total;
    }

    public int getPagedService(String namespaceId, int startPage, int pageSize, String param, String containedInstance,
            List<Service> serviceList, boolean hasIpCount) {

        List<Service> matchList;

        if (chooseServiceMap(namespaceId) == null) {
            return 0;
        }

        if (StringUtils.isNotBlank(param)) {
            StringJoiner regex = new StringJoiner(Constants.SERVICE_INFO_SPLITER);
            for (String s : param.split(Constants.SERVICE_INFO_SPLITER)) {
                regex.add(StringUtils.isBlank(s) ? Constants.ANY_PATTERN
                        : Constants.ANY_PATTERN + s + Constants.ANY_PATTERN);
            }
            matchList = searchServices(namespaceId, regex.toString());
        } else {
            matchList = new ArrayList<>(chooseServiceMap(namespaceId).values());
        }

        if (!CollectionUtils.isEmpty(matchList) && hasIpCount) {
            matchList = matchList.stream().filter(s -> !CollectionUtils.isEmpty(s.allIPs()))
                    .collect(Collectors.toList());
        }

        if (StringUtils.isNotBlank(containedInstance)) {

            boolean contained;
            for (int i = 0; i < matchList.size(); i++) {
                Service service = matchList.get(i);
                contained = false;
                List<Instance> instances = service.allIPs();
                for (Instance instance : instances) {
                    if (IPUtil.containsPort(containedInstance)) {
                        if (StringUtils.equals(instance.getIp() + IPUtil.IP_PORT_SPLITER + instance.getPort(), containedInstance)) {
                            contained = true;
                            break;
                        }
                    } else {
                        if (StringUtils.equals(instance.getIp(), containedInstance)) {
                            contained = true;
                            break;
                        }
                    }
                }
                if (!contained) {
                    matchList.remove(i);
                    i--;
                }
            }
        }

        if (pageSize >= matchList.size()) {
            serviceList.addAll(matchList);
            return matchList.size();
        }

        for (int i = 0; i < matchList.size(); i++) {
            if (i < startPage * pageSize) {
                continue;
            }

            serviceList.add(matchList.get(i));

            if (serviceList.size() >= pageSize) {
                break;
            }
        }

        return matchList.size();
    }

    public static class ServiceChecksum {

        public String namespaceId;

        public Map<String, String> serviceName2Checksum = new HashMap<String, String>();

        public ServiceChecksum() {
            this.namespaceId = Constants.DEFAULT_NAMESPACE_ID;
        }

        public ServiceChecksum(String namespaceId) {
            this.namespaceId = namespaceId;
        }

        /**
         * Add service checksum.
         *
         * @param serviceName service name
         * @param checksum    checksum of service
         */
        public void addItem(String serviceName, String checksum) {
            if (StringUtils.isEmpty(serviceName) || StringUtils.isEmpty(checksum)) {
                Loggers.SRV_LOG.warn("[DOMAIN-CHECKSUM] serviceName or checksum is empty,serviceName: {}, checksum: {}",
                        serviceName, checksum);
                return;
            }
            serviceName2Checksum.put(serviceName, checksum);
        }
    }

    private class EmptyServiceAutoClean implements Runnable {

        @Override
        public void run() { // 每20s清理一次注册表中空实例的service

            // Parallel flow opening threshold

            int parallelSize = 100; // 并行流开启阈值，当命名空间中的服务数量超过100会注册创建一个并行流

            serviceMap.forEach((namespace, stringServiceMap) -> {
                Stream<Map.Entry<String, Service>> stream = null;
                if (stringServiceMap.size() > parallelSize) {
                    stream = stringServiceMap.entrySet().parallelStream(); // 开启并行流
                } else {
                    stream = stringServiceMap.entrySet().stream(); // 开启串行流
                }
                stream.filter(entry -> {
                    final String serviceName = entry.getKey();
                    return distroMapper.responsible(serviceName); // 过滤需要当前server负责的服务
                }).forEach(entry -> stringServiceMap.computeIfPresent(entry.getKey(), (serviceName, service) -> {
                    if (service.isEmpty()) {

                        // To avoid violent Service removal, the number of times the Service
                        // experiences Empty is determined by finalizeCnt, and if the specified
                        // value is reached, it is removed

                        if (service.getFinalizeCount() > maxFinalizeCount) { // 服务为空的次数超过最大允许值，删除服务
                            Loggers.SRV_LOG.warn("namespace : {}, [{}] services are automatically cleaned", namespace,
                                    serviceName);
                            try {
                                easyRemoveService(namespace, serviceName);
                            } catch (Exception e) {
                                Loggers.SRV_LOG.error("namespace : {}, [{}] services are automatically clean has "
                                        + "error : {}", namespace, serviceName, e);
                            }
                        }

                        service.setFinalizeCount(service.getFinalizeCount() + 1); // 计数器加1

                        Loggers.SRV_LOG
                                .debug("namespace : {}, [{}] The number of times the current service experiences "
                                                + "an empty instance is : {}", namespace, serviceName,
                                        service.getFinalizeCount());
                    } else {
                        service.setFinalizeCount(0); //计数器清0
                    }
                    return service;
                }));
            });
        }
    }

    private class ServiceReporter implements Runnable {

        @Override
        public void run() {
            try {

                Map<String, Set<String>> allServiceNames = getAllServiceNames(); // key namespaceId，value 当前命名空间中所有服务名称

                if (allServiceNames.size() <= 0) {
                    //ignore
                    return;
                }

                for (String namespaceId : allServiceNames.keySet()) { //遍历命名空间

                    ServiceChecksum checksum = new ServiceChecksum(namespaceId);

                    for (String serviceName : allServiceNames.get(namespaceId)) { // 遍历命名空间下的所有服务
                        if (!distroMapper.responsible(serviceName)) { // 当前服务不属于当前server负责
                            continue;
                        }

                        Service service = getService(namespaceId, serviceName); // serviceMap中获取当前服务

                        if (service == null || service.isEmpty()) {
                            continue;
                        }

                        service.recalculateChecksum(); // 计算当前服务的checksum

                        checksum.addItem(serviceName, service.getChecksum()); //  serviceName2Checksum.put(serviceName, checksum); 记录服务的checkSum
                    }

                    Message msg = new Message();

                    msg.setData(JacksonUtils.toJson(checksum));

                    Collection<Member> sameSiteServers = memberManager.allMembers();

                    if (sameSiteServers == null || sameSiteServers.size() <= 0) {
                        return;
                    }

                    for (Member server : sameSiteServers) { // 遍历所有其他nacos服务
                        if (server.getAddress().equals(NetUtils.localServer())) { // 是当前server
                            continue;
                        }
                        synchronizer.send(server.getAddress(), msg); // 同步当前机器实例状态到其他服务
                    }
                }
            } catch (Exception e) {
                Loggers.SRV_LOG.error("[DOMAIN-STATUS] Exception while sending service status", e);
            } finally {
                GlobalExecutor.scheduleServiceReporter(this, switchDomain.getServiceStatusSynchronizationPeriodMillis(),
                        TimeUnit.MILLISECONDS); // 下一次定时任务执行
            }
        }
    }

    private static class ServiceKey {

        private String namespaceId;

        private String serviceName;

        private String serverIP;

        private String checksum;

        public String getChecksum() {
            return checksum;
        }

        public String getServerIP() {
            return serverIP;
        }

        public String getServiceName() {
            return serviceName;
        }

        public String getNamespaceId() {
            return namespaceId;
        }

        public ServiceKey(String namespaceId, String serviceName, String serverIP, String checksum) {
            this.namespaceId = namespaceId;
            this.serviceName = serviceName;
            this.serverIP = serverIP;
            this.checksum = checksum;
        }

        @Override
        public String toString() {
            return JacksonUtils.toJson(this);
        }
    }
}
