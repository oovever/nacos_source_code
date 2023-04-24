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

package com.alibaba.nacos.naming.consistency.persistent.raft;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.naming.misc.HttpClient;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NetUtils;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.apache.commons.collections.SortedBag;
import org.apache.commons.collections.bag.TreeBag;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Sets of raft peers.
 *
 * @author nacos
 * @deprecated will remove in 1.4.x
 */
@Deprecated
@Component
@DependsOn("ProtocolManager")
public class RaftPeerSet extends MemberChangeListener implements Closeable {

    private final ServerMemberManager memberManager;

    private AtomicLong localTerm = new AtomicLong(0L);

    private RaftPeer leader = null;

    private volatile Map<String, RaftPeer> peers = new HashMap<>(8);

    private Set<String> sites = new HashSet<>();

    private volatile boolean ready = false;

    private Set<Member> oldMembers = new HashSet<>();

    public RaftPeerSet(ServerMemberManager memberManager) {
        this.memberManager = memberManager;
    }

    @PostConstruct
    public void init() {
        NotifyCenter.registerSubscriber(this);
        changePeers(memberManager.allMembers());
    }

    @Override
    public void shutdown() throws NacosException {
        this.localTerm.set(-1);
        this.leader = null;
        this.peers.clear();
        this.sites.clear();
        this.ready = false;
        this.oldMembers.clear();
    }

    public RaftPeer getLeader() {
        if (EnvUtil.getStandaloneMode()) {
            return local();
        }
        return leader;
    }

    public Set<String> allSites() {
        return sites;
    }

    public boolean isReady() {
        return ready;
    }

    /**
     * Remove raft node.
     *
     * @param servers node address need to be removed
     */
    public void remove(List<String> servers) {
        for (String server : servers) {
            peers.remove(server);
        }
    }

    /**
     * Update raft peer.
     *
     * @param peer new peer.
     * @return new peer
     */
    public RaftPeer update(RaftPeer peer) {
        peers.put(peer.ip, peer);
        return peer;
    }

    /**
     * Judge whether input address is leader.
     *
     * @param ip peer address
     * @return true if is leader or stand alone, otherwise false
     */
    public boolean isLeader(String ip) {
        if (EnvUtil.getStandaloneMode()) {
            return true;
        }

        if (leader == null) {
            Loggers.RAFT.warn("[IS LEADER] no leader is available now!");
            return false;
        }

        return StringUtils.equals(leader.ip, ip);
    }

    public Set<String> allServersIncludeMyself() {
        return peers.keySet();
    }

    /**
     * Get all servers excludes current peer.
     *
     * @return all servers excludes current peer
     */
    public Set<String> allServersWithoutMySelf() {
        Set<String> servers = new HashSet<String>(peers.keySet());

        // exclude myself
        servers.remove(local().ip);

        return servers;
    }

    public Collection<RaftPeer> allPeers() {
        return peers.values();
    }

    public int size() {
        return peers.size();
    }

    /**
     * Calculate and decide which peer is leader. If has new peer has more than half vote, change leader to new peer.
     *
     * @param candidate new candidate
     * @return new leader if new candidate has more than half vote, otherwise old leader
     */
    public RaftPeer decideLeader(RaftPeer candidate) {
        peers.put(candidate.ip, candidate); // 记录收到选票的ip和RaftPeer

        SortedBag ips = new TreeBag();
        int maxApproveCount = 0;
        String maxApprovePeer = null; // 最多票的节点
        for (RaftPeer peer : peers.values()) { // 遍历peers map
            if (StringUtils.isEmpty(peer.voteFor)) { // 没有选举leader跳过
                continue;
            }

            ips.add(peer.voteFor); // 记录次数
            if (ips.getCount(peer.voteFor) > maxApproveCount) { // 是否大于最多次数
                maxApproveCount = ips.getCount(peer.voteFor); // 记录最多票的次数
                maxApprovePeer = peer.voteFor; // 记录最多票的节点
            }
        }

        if (maxApproveCount >= majorityCount()) { //投某节点的票大于半数+1
            RaftPeer peer = peers.get(maxApprovePeer); // 获取最大票的节点
            peer.state = RaftPeer.State.LEADER; // 设置当前节点状态为leader

            if (!Objects.equals(leader, peer)) { // leader与选出的节点不同
                leader = peer; // 设置leader为选出的节点
                ApplicationUtils.publishEvent(new LeaderElectFinishedEvent(this, leader, local())); // 发布leader选举完成的事件
                Loggers.RAFT.info("{} has become the LEADER", leader.ip);
            }
        }

        return leader;
    }

    /**
     * Set leader as new candidate.
     *
     * @param candidate new candidate
     * @return new leader
     */
    public RaftPeer makeLeader(RaftPeer candidate) {
        if (!Objects.equals(leader, candidate)) { // leader非远端leader，设置当前leader为远端leader
            leader = candidate;
            ApplicationUtils.publishEvent(new MakeLeaderEvent(this, leader, local())); // 触发MakeLeaderEvent监听事件
            Loggers.RAFT
                    .info("{} has become the LEADER, local: {}, leader: {}", leader.ip, JacksonUtils.toJson(local()),
                            JacksonUtils.toJson(leader));
        }

        for (final RaftPeer peer : peers.values()) {
            Map<String, String> params = new HashMap<>(1);
            if (!Objects.equals(peer, candidate) && peer.state == RaftPeer.State.LEADER) { // 找到之前的leader节点
                try {
                    String url = RaftCore.buildUrl(peer.ip, RaftCore.API_GET_PEER); // ns/raft/peer
                    HttpClient.asyncHttpGet(url, null, params, new Callback<String>() { //获取leader节点的peer
                        @Override
                        public void onReceive(RestResult<String> result) {
                            if (!result.ok()) {
                                Loggers.RAFT
                                        .error("[NACOS-RAFT] get peer failed: {}, peer: {}", result.getCode(), peer.ip);
                                peer.state = RaftPeer.State.FOLLOWER;
                                return;
                            }

                            update(JacksonUtils.toObj(result.getData(), RaftPeer.class)); // 更新原来是leader的节点状态
                        }

                        @Override
                        public void onError(Throwable throwable) {

                        }

                        @Override
                        public void onCancel() {

                        }
                    });
                } catch (Exception e) {
                    peer.state = RaftPeer.State.FOLLOWER;
                    Loggers.RAFT.error("[NACOS-RAFT] error while getting peer from peer: {}", peer.ip);
                }
            }
        }

        return update(candidate);
    }

    /**
     * Get local raft peer.
     *
     * @return local raft peer
     */
    public RaftPeer local() {
        RaftPeer peer = peers.get(EnvUtil.getLocalAddress());
        if (peer == null && EnvUtil.getStandaloneMode()) {
            RaftPeer localPeer = new RaftPeer();
            localPeer.ip = NetUtils.localServer();
            localPeer.term.set(localTerm.get());
            peers.put(localPeer.ip, localPeer);
            return localPeer;
        }
        if (peer == null) {
            throw new IllegalStateException(
                    "unable to find local peer: " + NetUtils.localServer() + ", all peers: " + Arrays
                            .toString(peers.keySet().toArray()));
        }

        return peer;
    }

    public RaftPeer get(String server) {
        return peers.get(server);
    }

    public int majorityCount() {
        return peers.size() / 2 + 1;
    }

    /**
     * Reset set.
     */
    public void reset() {

        leader = null;

        for (RaftPeer peer : peers.values()) {
            peer.voteFor = null;
        }
    }

    public void setTerm(long term) {
        localTerm.set(term);
    }

    public long getTerm() {
        return localTerm.get();
    }

    public boolean contains(RaftPeer remote) {
        return peers.containsKey(remote.ip);
    }

    @Override
    public void onEvent(MembersChangeEvent event) {
        Collection<Member> members = event.getMembers();
        Collection<Member> newMembers = new HashSet<>(members);
        newMembers.removeAll(oldMembers);

        // If an IP change occurs, the change starts
        if (!newMembers.isEmpty()) {
            changePeers(members);
        }

        oldMembers.clear();
        oldMembers.addAll(members);
    }

    protected void changePeers(Collection<Member> members) {
        Map<String, RaftPeer> tmpPeers = new HashMap<>(members.size());

        for (Member member : members) {

            final String address = member.getAddress();
            if (peers.containsKey(address)) {
                tmpPeers.put(address, peers.get(address));
                continue;
            }

            RaftPeer raftPeer = new RaftPeer();
            raftPeer.ip = address;

            // first time meet the local server:
            if (EnvUtil.getLocalAddress().equals(address)) {
                raftPeer.term.set(localTerm.get());
            }

            tmpPeers.put(address, raftPeer);
        }

        // replace raft peer set:
        peers = tmpPeers;

        ready = true;
        Loggers.RAFT.info("raft peers changed: " + members);
    }

    @Override
    public String toString() {
        return "RaftPeerSet{" + "localTerm=" + localTerm + ", leader=" + leader + ", peers=" + peers + ", sites="
                + sites + '}';
    }
}
