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

package com.alibaba.nacos.naming.controllers;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.CommonParams;
import com.alibaba.nacos.api.naming.NamingResponseCode;
import com.alibaba.nacos.api.naming.PreservedMetadataKeys;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.common.ActionTypes;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.core.utils.WebUtils;
import com.alibaba.nacos.naming.core.Instance;
import com.alibaba.nacos.naming.core.Service;
import com.alibaba.nacos.naming.core.ServiceManager;
import com.alibaba.nacos.naming.healthcheck.RsInfo;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import com.alibaba.nacos.naming.misc.SwitchEntry;
import com.alibaba.nacos.naming.misc.UtilsAndCommons;
import com.alibaba.nacos.naming.pojo.InstanceOperationContext;
import com.alibaba.nacos.naming.pojo.InstanceOperationInfo;
import com.alibaba.nacos.naming.push.ClientInfo;
import com.alibaba.nacos.naming.push.DataSource;
import com.alibaba.nacos.naming.push.PushService;
import com.alibaba.nacos.naming.web.CanDistro;
import com.alibaba.nacos.naming.web.NamingResourceParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.BooleanUtils;
import org.apache.commons.lang3.StringUtils;
import org.codehaus.jackson.util.VersionUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PatchMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.alibaba.nacos.naming.misc.UtilsAndCommons.DEFAULT_CLUSTER_NAME;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.EPHEMERAL;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.PERSIST;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_REMOVE;
import static com.alibaba.nacos.naming.misc.UtilsAndCommons.UPDATE_INSTANCE_METADATA_ACTION_UPDATE;

/**
 * Instance operation controller.
 * 服务实例请求处理器
 * @author nkorange
 */
@RestController
@RequestMapping(UtilsAndCommons.NACOS_NAMING_CONTEXT + "/instance")
public class InstanceController {

    @Autowired
    private SwitchDomain switchDomain;

    @Autowired
    private PushService pushService;

    @Autowired
    private ServiceManager serviceManager;

    private DataSource pushDataSource = new DataSource() {

        @Override
        public String getData(PushService.PushClient client) {

            ObjectNode result = JacksonUtils.createEmptyJsonNode();
            try {
                result = doSrvIpxt(client.getNamespaceId(), client.getServiceName(), client.getAgent(),
                        client.getClusters(), client.getSocketAddr().getAddress().getHostAddress(), 0,
                        StringUtils.EMPTY, false, StringUtils.EMPTY, StringUtils.EMPTY, false);
            } catch (Exception e) {
                Loggers.SRV_LOG.warn("PUSH-SERVICE: service is not modified", e);
            }

            // overdrive the cache millis to push mode
            result.put("cacheMillis", switchDomain.getPushCacheMillis(client.getServiceName()));

            return result.toString();
        }
    };

    /**
     * Register new instance.
     * 注册实例
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during register
     */
    @CanDistro
    @PostMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public String register(HttpServletRequest request) throws Exception {

        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID); // 获取nameSpace
        final String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME); // 获取服务名称 group@serviceName
        NamingUtils.checkServiceNameFormat(serviceName);

        final Instance instance = parseInstance(request); // 解析请求获取实例

        serviceManager.registerInstance(namespaceId, serviceName, instance); // 注册实例到注册表
        return "ok";
    }

    /**
     * Deregister instances.
     *  服务下线逻辑处理
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during deregister
     */
    @CanDistro
    @DeleteMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public String deregister(HttpServletRequest request) throws Exception {
        Instance instance = getIpAddress(request); // 获取请求的实例
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);

        Service service = serviceManager.getService(namespaceId, serviceName); // 注册表中获取service
        if (service == null) {
            Loggers.SRV_LOG.warn("remove instance from non-exist service: {}", serviceName);
            return "ok";
        }

        serviceManager.removeInstance(namespaceId, serviceName, instance.isEphemeral(), instance); // 删除实例
        return "ok";
    }

    /**
     * Update instance.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during update
     */
    @CanDistro
    @PutMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public String update(HttpServletRequest request) throws Exception {
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        final String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        final Instance instance = parseInstance(request);

        String agent = WebUtils.getUserAgent(request);

        ClientInfo clientInfo = new ClientInfo(agent);

        if (clientInfo.type == ClientInfo.ClientType.JAVA
                && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
            serviceManager.updateInstance(namespaceId, serviceName, instance);
        } else {
            serviceManager.registerInstance(namespaceId, serviceName, instance);
        }
        return "ok";
    }

    /**
     * Batch update instance's metadata. old key exist = update, old key not exist = add.
     *
     * @param request http request
     * @return success updated instances. such as '{"updated":["2.2.2.2:8080:unknown:xxxx-cluster:ephemeral"}'.
     * @throws Exception any error during update
     * @since 1.4.0
     */
    @CanDistro
    @PutMapping(value = "/metadata/batch")
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public ObjectNode batchUpdateInstanceMatadata(HttpServletRequest request) throws Exception {
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);

        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);

        String consistencyType = WebUtils.optional(request, "consistencyType", StringUtils.EMPTY);

        String instances = WebUtils.optional(request, "instances", StringUtils.EMPTY);

        List<Instance> targetInstances = parseBatchInstances(instances);

        String metadata = WebUtils.required(request, "metadata");
        Map<String, String> targetMetadata = UtilsAndCommons.parseMetadata(metadata);

        List<Instance> operatedInstances = batchOperateMetadata(namespaceId,
                buildOperationInfo(serviceName, consistencyType, targetInstances), targetMetadata,
                UPDATE_INSTANCE_METADATA_ACTION_UPDATE);

        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();

        for (Instance ip : operatedInstances) {
            ipArray.add(ip.getDatumKey() + ":" + (ip.isEphemeral() ? EPHEMERAL : PERSIST));
        }

        result.replace("updated", ipArray);
        return result;
    }

    /**
     * Batch delete instance's metadata. old key exist = delete, old key not exist = not operate
     *
     * @param request http request
     * @return success updated instances. such as '{"updated":["2.2.2.2:8080:unknown:xxxx-cluster:ephemeral"}'.
     * @throws Exception any error during update
     * @since 1.4.0
     */
    @CanDistro
    @DeleteMapping("/metadata/batch")
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public ObjectNode batchDeleteInstanceMatadata(HttpServletRequest request) throws Exception {
        final String namespaceId = WebUtils
                .optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);

        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);

        String consistencyType = WebUtils.optional(request, "consistencyType", StringUtils.EMPTY);

        String instances = WebUtils.optional(request, "instances", StringUtils.EMPTY);

        List<Instance> targetInstances = parseBatchInstances(instances);

        String metadata = WebUtils.required(request, "metadata");
        Map<String, String> targetMetadata = UtilsAndCommons.parseMetadata(metadata);

        List<Instance> operatedInstances = batchOperateMetadata(namespaceId,
                buildOperationInfo(serviceName, consistencyType, targetInstances), targetMetadata,
                UPDATE_INSTANCE_METADATA_ACTION_REMOVE);

        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();

        for (Instance ip : operatedInstances) {
            ipArray.add(ip.getDatumKey() + ":" + (ip.isEphemeral() ? EPHEMERAL : PERSIST));
        }

        result.replace("updated", ipArray);
        return result;
    }

    private InstanceOperationInfo buildOperationInfo(String serviceName, String consistencyType,
            List<Instance> instances) {
        if (!CollectionUtils.isEmpty(instances)) {
            for (Instance instance : instances) {
                if (StringUtils.isBlank(instance.getClusterName())) {
                    instance.setClusterName(DEFAULT_CLUSTER_NAME);
                }
            }
        }
        return new InstanceOperationInfo(serviceName, consistencyType, instances);
    }

    private List<Instance> parseBatchInstances(String instances) {
        try {
            return JacksonUtils.toObj(instances, new TypeReference<List<Instance>>() {
            });
        } catch (Exception e) {
            Loggers.SRV_LOG.warn("UPDATE-METADATA: Param 'instances' is illegal, ignore this operation", e);
        }
        return null;
    }

    private List<Instance> batchOperateMetadata(String namespace, InstanceOperationInfo instanceOperationInfo,
            Map<String, String> metadata, String action) {
        Function<InstanceOperationContext, List<Instance>> operateFunction = instanceOperationContext -> {
            try {
                return serviceManager.updateMetadata(instanceOperationContext.getNamespace(),
                        instanceOperationContext.getServiceName(), instanceOperationContext.getEphemeral(), action,
                        instanceOperationContext.getAll(), instanceOperationContext.getInstances(), metadata);
            } catch (NacosException e) {
                Loggers.SRV_LOG.warn("UPDATE-METADATA: updateMetadata failed", e);
            }
            return new ArrayList<>();
        };
        return serviceManager.batchOperate(namespace, instanceOperationInfo, operateFunction);
    }

    /**
     * Patch instance.
     *
     * @param request http request
     * @return 'ok' if success
     * @throws Exception any error during patch
     */
    @CanDistro
    @PatchMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public String patch(HttpServletRequest request) throws Exception {
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        String ip = WebUtils.required(request, "ip");
        String port = WebUtils.required(request, "port");
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, StringUtils.EMPTY);
        if (StringUtils.isBlank(cluster)) {
            cluster = WebUtils.optional(request, "cluster", UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        }

        Instance instance = serviceManager.getInstance(namespaceId, serviceName, cluster, ip, Integer.parseInt(port));
        if (instance == null) {
            throw new IllegalArgumentException("instance not found");
        }

        String metadata = WebUtils.optional(request, "metadata", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(metadata)) {
            instance.setMetadata(UtilsAndCommons.parseMetadata(metadata));
        }
        String app = WebUtils.optional(request, "app", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(app)) {
            instance.setApp(app);
        }
        String weight = WebUtils.optional(request, "weight", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(weight)) {
            instance.setWeight(Double.parseDouble(weight));
        }
        String healthy = WebUtils.optional(request, "healthy", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(healthy)) {
            instance.setHealthy(BooleanUtils.toBoolean(healthy));
        }
        String enabledString = WebUtils.optional(request, "enabled", StringUtils.EMPTY);
        if (StringUtils.isNotBlank(enabledString)) {
            instance.setEnabled(BooleanUtils.toBoolean(enabledString));
        }
        instance.setLastBeat(System.currentTimeMillis());
        instance.validate();
        serviceManager.updateInstance(namespaceId, serviceName, instance);
        return "ok";
    }

    /**
     * Get all instance of input service.
     *  根据服务名获取所有实例
     * @param request http request
     * @return list of instance
     * @throws Exception any error during list
     */
    @GetMapping("/list")
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.READ)
    public ObjectNode list(HttpServletRequest request) throws Exception {
//        获取请求中的属性信息
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);

        String agent = WebUtils.getUserAgent(request); // 客户端类型
        String clusters = WebUtils.optional(request, "clusters", StringUtils.EMPTY);
        String clientIP = WebUtils.optional(request, "clientIP", StringUtils.EMPTY);
        int udpPort = Integer.parseInt(WebUtils.optional(request, "udpPort", "0")); // 获取client的udp端口号
        String env = WebUtils.optional(request, "env", StringUtils.EMPTY);
        boolean isCheck = Boolean.parseBoolean(WebUtils.optional(request, "isCheck", "false"));

        String app = WebUtils.optional(request, "app", StringUtils.EMPTY);

        String tenant = WebUtils.optional(request, "tid", StringUtils.EMPTY);

        boolean healthyOnly = Boolean.parseBoolean(WebUtils.optional(request, "healthyOnly", "false"));

        return doSrvIpxt(namespaceId, serviceName, agent, clusters, clientIP, udpPort, env, isCheck, app, tenant,
                healthyOnly); // 处理请求
    }

    /**
     * Get detail information of specified instance.
     *
     * @param request http request
     * @return detail information of instance
     * @throws Exception any error during get
     */
    @GetMapping
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.READ)
    public ObjectNode detail(HttpServletRequest request) throws Exception {

        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        String ip = WebUtils.required(request, "ip");
        int port = Integer.parseInt(WebUtils.required(request, "port"));

        Service service = serviceManager.getService(namespaceId, serviceName);
        if (service == null) {
            throw new NacosException(NacosException.NOT_FOUND, "no service " + serviceName + " found!");
        }

        List<String> clusters = new ArrayList<>();
        clusters.add(cluster);

        List<Instance> ips = service.allIPs(clusters);
        if (ips == null || ips.isEmpty()) {
            throw new NacosException(NacosException.NOT_FOUND,
                    "no ips found for cluster " + cluster + " in service " + serviceName);
        }

        for (Instance instance : ips) {
            if (instance.getIp().equals(ip) && instance.getPort() == port) {
                ObjectNode result = JacksonUtils.createEmptyJsonNode();
                result.put("service", serviceName);
                result.put("ip", ip);
                result.put("port", port);
                result.put("clusterName", cluster);
                result.put("weight", instance.getWeight());
                result.put("healthy", instance.isHealthy());
                result.put("instanceId", instance.getInstanceId());
                result.set("metadata", JacksonUtils.transferToJsonNode(instance.getMetadata()));
                return result;
            }
        }

        throw new NacosException(NacosException.NOT_FOUND, "no matched ip found!");
    }

    /**
     * Create a beat for instance.
     * 心跳处理
     * @param request http request
     * @return detail information of instance
     * @throws Exception any error during handle
     */
    @CanDistro
    @PutMapping("/beat")
    @Secured(parser = NamingResourceParser.class, action = ActionTypes.WRITE)
    public ObjectNode beat(HttpServletRequest request) throws Exception {

        ObjectNode result = JacksonUtils.createEmptyJsonNode(); // 创建json node对象
        result.put(SwitchEntry.CLIENT_BEAT_INTERVAL, switchDomain.getClientBeatInterval()); // 客户端心跳时间间隔

        String beat = WebUtils.optional(request, "beat", StringUtils.EMPTY); // 获取client的beat信息
        RsInfo clientBeat = null;
        if (StringUtils.isNotBlank(beat)) {
            clientBeat = JacksonUtils.toObj(beat, RsInfo.class); // 反序列化解析
        }
        String clusterName = WebUtils
                .optional(request, CommonParams.CLUSTER_NAME, UtilsAndCommons.DEFAULT_CLUSTER_NAME); // 获取cluster名称
        String ip = WebUtils.optional(request, "ip", StringUtils.EMPTY); // 获取ip
        int port = Integer.parseInt(WebUtils.optional(request, "port", "0")); // 获取端口号
        if (clientBeat != null) { // 集群名称 IP 端口赋值
            if (StringUtils.isNotBlank(clientBeat.getCluster())) {
                clusterName = clientBeat.getCluster();
            } else {
                // fix #2533
                clientBeat.setCluster(clusterName); // 设置默认集群
            }
            ip = clientBeat.getIp();
            port = clientBeat.getPort();
        }
        String namespaceId = WebUtils.optional(request, CommonParams.NAMESPACE_ID, Constants.DEFAULT_NAMESPACE_ID);
        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        NamingUtils.checkServiceNameFormat(serviceName);
        Loggers.SRV_LOG.debug("[CLIENT-BEAT] full arguments: beat: {}, serviceName: {}", clientBeat, serviceName);
        Instance instance = serviceManager.getInstance(namespaceId, serviceName, clusterName, ip, port); // 获取当前client对应的实例

        if (instance == null) { // 注册表中不存在该client对应的实例
            if (clientBeat == null) { // 请求中没有心跳数据直接返回
                result.put(CommonParams.CODE, NamingResponseCode.RESOURCE_NOT_FOUND);
                return result;
            }

            Loggers.SRV_LOG.warn("[CLIENT-BEAT] The instance has been removed for health mechanism, "
                    + "perform data compensation operations, beat: {}, serviceName: {}", clientBeat, serviceName);
//        注册表中没有心跳数据，但是请求的心跳数据不为空；注册时心跳请求先于注册请求到达，所以先构建一个实例，并注册到serviceMap
            instance = new Instance();
            instance.setPort(clientBeat.getPort());
            instance.setIp(clientBeat.getIp());
            instance.setWeight(clientBeat.getWeight());
            instance.setMetadata(clientBeat.getMetadata());
            instance.setClusterName(clusterName);
            instance.setServiceName(serviceName);
            instance.setInstanceId(instance.getInstanceId());
            instance.setEphemeral(clientBeat.isEphemeral());

            serviceManager.registerInstance(namespaceId, serviceName, instance);
        }

        Service service = serviceManager.getService(namespaceId, serviceName); // 获取服务

        if (service == null) {
            throw new NacosException(NacosException.SERVER_ERROR,
                    "service not found: " + serviceName + "@" + namespaceId);
        }
        if (clientBeat == null) {
            clientBeat = new RsInfo();
            clientBeat.setIp(ip);
            clientBeat.setPort(port);
            clientBeat.setCluster(clusterName);
        }
        service.processClientBeat(clientBeat); // 处理心跳

        result.put(CommonParams.CODE, NamingResponseCode.OK);
        if (instance.containsMetadata(PreservedMetadataKeys.HEART_BEAT_INTERVAL)) {    // 如果instance中有 preserved.heart.beat.interval 这个参数 动态配置
            result.put(SwitchEntry.CLIENT_BEAT_INTERVAL, instance.getInstanceHeartBeatInterval());
        }
        result.put(SwitchEntry.LIGHT_BEAT_ENABLED, switchDomain.isLightBeatEnabled()); // 是否携带beatInfo，控制台可以修改
        return result; // 返回客户端
    }

    /**
     * List all instance with health status.
     *
     * @param key (namespace##)?serviceName
     * @return list of instance
     * @throws NacosException any error during handle
     */
    @RequestMapping("/statuses")
    public ObjectNode listWithHealthStatus(@RequestParam String key) throws NacosException {

        String serviceName;
        String namespaceId;

        if (key.contains(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)) {
            namespaceId = key.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[0];
            serviceName = key.split(UtilsAndCommons.NAMESPACE_SERVICE_CONNECTOR)[1];
        } else {
            namespaceId = Constants.DEFAULT_NAMESPACE_ID;
            serviceName = key;
        }
        NamingUtils.checkServiceNameFormat(serviceName);
        Service service = serviceManager.getService(namespaceId, serviceName);

        if (service == null) {
            throw new NacosException(NacosException.NOT_FOUND, "service: " + serviceName + " not found.");
        }

        List<Instance> ips = service.allIPs();

        ObjectNode result = JacksonUtils.createEmptyJsonNode();
        ArrayNode ipArray = JacksonUtils.createEmptyArrayNode();

        for (Instance ip : ips) {
            ipArray.add(ip.toIpAddr() + "_" + ip.isHealthy());
        }

        result.replace("ips", ipArray);
        return result;
    }

    private Instance parseInstance(HttpServletRequest request) throws Exception {

        String serviceName = WebUtils.required(request, CommonParams.SERVICE_NAME);
        String app = WebUtils.optional(request, "app", "DEFAULT");
        Instance instance = getIpAddress(request);
        instance.setApp(app);
        instance.setServiceName(serviceName);
        // Generate simple instance id first. This value would be updated according to
        // INSTANCE_ID_GENERATOR.
        instance.setInstanceId(instance.generateInstanceId()); // 设置实例id
        instance.setLastBeat(System.currentTimeMillis());
        String metadata = WebUtils.optional(request, "metadata", StringUtils.EMPTY);
        if (StringUtils.isNotEmpty(metadata)) {
            instance.setMetadata(UtilsAndCommons.parseMetadata(metadata));
        }

        instance.validate();

        return instance;
    }

    private Instance getIpAddress(HttpServletRequest request) {

        String enabledString = WebUtils.optional(request, "enabled", StringUtils.EMPTY);
        boolean enabled;
        if (StringUtils.isBlank(enabledString)) {
            enabled = BooleanUtils.toBoolean(WebUtils.optional(request, "enable", "true"));
        } else {
            enabled = BooleanUtils.toBoolean(enabledString);
        }

        String weight = WebUtils.optional(request, "weight", "1");
        boolean healthy = BooleanUtils.toBoolean(WebUtils.optional(request, "healthy", "true"));

        Instance instance = getBasicIpAddress(request);
        instance.setWeight(Double.parseDouble(weight));
        instance.setHealthy(healthy);
        instance.setEnabled(enabled);

        return instance;
    }

    private Instance getBasicIpAddress(HttpServletRequest request) {

        final String ip = WebUtils.required(request, "ip");
        final String port = WebUtils.required(request, "port");
        String cluster = WebUtils.optional(request, CommonParams.CLUSTER_NAME, StringUtils.EMPTY);
        if (StringUtils.isBlank(cluster)) {
            cluster = WebUtils.optional(request, "cluster", UtilsAndCommons.DEFAULT_CLUSTER_NAME);
        }
        boolean ephemeral = BooleanUtils.toBoolean(
                WebUtils.optional(request, "ephemeral", String.valueOf(switchDomain.isDefaultInstanceEphemeral())));

        Instance instance = new Instance();
        instance.setPort(Integer.parseInt(port));
        instance.setIp(ip);
        instance.setEphemeral(ephemeral);
        instance.setClusterName(cluster);

        return instance;
    }

    private void checkIfDisabled(Service service) throws Exception {
        if (!service.getEnabled()) {
            throw new Exception("service is disabled now.");
        }
    }

    /**
     * Get service full information with instances.
     *
     * @param namespaceId namespace id
     * @param serviceName service name
     * @param agent       agent infor string
     * @param clusters    cluster names
     * @param clientIP    client ip
     * @param udpPort     push udp port
     * @param env         env
     * @param isCheck     is check request
     * @param app         app name
     * @param tid         tenant
     * @param healthyOnly whether only for healthy check
     * @return service full information with instances
     * @throws Exception any error during handle
     */
    public ObjectNode doSrvIpxt(String namespaceId, String serviceName, String agent, String clusters, String clientIP,
            int udpPort, String env, boolean isCheck, String app, String tid, boolean healthyOnly) throws Exception {

        ClientInfo clientInfo = new ClientInfo(agent); // 针对不同的agent生成不同的ClientInfo
        ObjectNode result = JacksonUtils.createEmptyJsonNode(); // 返回结果node
        Service service = serviceManager.getService(namespaceId, serviceName); // serviceMap中获取服务
        long cacheMillis = switchDomain.getDefaultCacheMillis(); // SwitchDomain是整个naming的核心开关配置类，几乎所有关键的开关配置信息都在该类中定义。

        // now try to enable the push udpPort > 0表示订阅了，
        try {
            if (udpPort > 0 && pushService.canEnablePush(agent)) { // 判断客户端版本是否支持推送

                pushService
                        .addClient(namespaceId, serviceName, clusters, agent, new InetSocketAddress(clientIP, udpPort),
                                pushDataSource, tid, app); // 创建发布订阅的udp client
                cacheMillis = switchDomain.getPushCacheMillis(serviceName);
            }
        } catch (Exception e) {
            Loggers.SRV_LOG
                    .error("[NACOS-API] failed to added push client {}, {}:{}", clientInfo, clientIP, udpPort, e);
            cacheMillis = switchDomain.getDefaultCacheMillis();
        }

        if (service == null) { //获取不到服务，直接返回空
            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("no instance to serve for service: {}", serviceName);
            }
            result.put("name", serviceName);
            result.put("clusters", clusters);
            result.put("cacheMillis", cacheMillis);
            result.replace("hosts", JacksonUtils.createEmptyArrayNode());
            return result;
        }

        checkIfDisabled(service); // 检查服务是否被禁用，服务被禁用，直接返回异常

        List<Instance> srvedIPs;

        srvedIPs = service.srvIPs(Arrays.asList(StringUtils.split(clusters, ","))); // 获取该服务特定集群下的的所有实例，持久/临时

        // filter ips using selector: 选择器不为空，使用选择器过滤实例
        if (service.getSelector() != null && StringUtils.isNotBlank(clientIP)) {
            srvedIPs = service.getSelector().select(clientIP, srvedIPs);
        }

        if (CollectionUtils.isEmpty(srvedIPs)) { // 筛选出结果为空，返回客户端空实例

            if (Loggers.SRV_LOG.isDebugEnabled()) {
                Loggers.SRV_LOG.debug("no instance to serve for service: {}", serviceName);
            }

            if (clientInfo.type == ClientInfo.ClientType.JAVA
                    && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
                result.put("dom", serviceName);
            } else {
                result.put("dom", NamingUtils.getServiceName(serviceName));
            }

            result.put("name", serviceName);
            result.put("cacheMillis", cacheMillis);
            result.put("lastRefTime", System.currentTimeMillis());
            result.put("checksum", service.getChecksum());
            result.put("useSpecifiedURL", false);
            result.put("clusters", clusters);
            result.put("env", env);
            result.set("hosts", JacksonUtils.createEmptyArrayNode()); // 空Host
            result.set("metadata", JacksonUtils.transferToJsonNode(service.getMetadata()));
            return result;
        }

        Map<Boolean, List<Instance>> ipMap = new HashMap<>(2);
        ipMap.put(Boolean.TRUE, new ArrayList<>()); // 健康实例
        ipMap.put(Boolean.FALSE, new ArrayList<>()); // 不健康实例

        for (Instance ip : srvedIPs) {
            ipMap.get(ip.isHealthy()).add(ip); // map处理
        }

        if (isCheck) { // 是否达到保护阈值检查
            result.put("reachProtectThreshold", false);
        }

        double threshold = service.getProtectThreshold(); // 获取服务的保护阈值，默认0

        if ((float) ipMap.get(Boolean.TRUE).size() / srvedIPs.size() <= threshold) { // 健康实例/总实例 小于保护阈值

            Loggers.SRV_LOG.warn("protect threshold reached, return all ips, service: {}", serviceName);
            if (isCheck) { // 启动保护
                result.put("reachProtectThreshold", true);
            }

            ipMap.get(Boolean.TRUE).addAll(ipMap.get(Boolean.FALSE)); // 不健康实例添加到map中，避免健康实例负载过高，让不健康实例也能响应请求
            ipMap.get(Boolean.FALSE).clear(); // 清空不健康实例列表
        }

        if (isCheck) {
            result.put("protectThreshold", service.getProtectThreshold());
            result.put("reachLocalSiteCallThreshold", false);

            return JacksonUtils.createEmptyJsonNode();
        }

        ArrayNode hosts = JacksonUtils.createEmptyArrayNode();

        for (Map.Entry<Boolean, List<Instance>> entry : ipMap.entrySet()) { //遍历实例列表，健康、不健康
            List<Instance> ips = entry.getValue();

            if (healthyOnly && !entry.getKey()) { // 客户端值需要健康实例，过滤不健康实例
                continue;
            }

            for (Instance instance : ips) { // 遍历健康/不健康实例

                // remove disabled instance:
                if (!instance.isEnabled()) {
                    continue;
                }

                ObjectNode ipObj = JacksonUtils.createEmptyJsonNode();
                // 存储遍历结果
                ipObj.put("ip", instance.getIp());
                ipObj.put("port", instance.getPort());
                // deprecated since nacos 1.0.0:
                ipObj.put("valid", entry.getKey());
                ipObj.put("healthy", entry.getKey());
                ipObj.put("marked", instance.isMarked());
                ipObj.put("instanceId", instance.getInstanceId());
                ipObj.set("metadata", JacksonUtils.transferToJsonNode(instance.getMetadata()));
                ipObj.put("enabled", instance.isEnabled());
                ipObj.put("weight", instance.getWeight());
                ipObj.put("clusterName", instance.getClusterName());
                if (clientInfo.type == ClientInfo.ClientType.JAVA
                        && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
                    ipObj.put("serviceName", instance.getServiceName());
                } else {
                    ipObj.put("serviceName", NamingUtils.getServiceName(instance.getServiceName()));
                }

                ipObj.put("ephemeral", instance.isEphemeral());
                hosts.add(ipObj);

            }
        }

        result.replace("hosts", hosts);
        if (clientInfo.type == ClientInfo.ClientType.JAVA
                && clientInfo.version.compareTo(VersionUtil.parseVersion("1.0.0")) >= 0) {
            result.put("dom", serviceName);
        } else {
            result.put("dom", NamingUtils.getServiceName(serviceName));
        }
        result.put("name", serviceName);
        result.put("cacheMillis", cacheMillis);
        result.put("lastRefTime", System.currentTimeMillis());
        result.put("checksum", service.getChecksum());
        result.put("useSpecifiedURL", false);
        result.put("clusters", clusters);
        result.put("env", env);
        result.replace("metadata", JacksonUtils.transferToJsonNode(service.getMetadata()));
        return result; //返回到客户端
    }
}
