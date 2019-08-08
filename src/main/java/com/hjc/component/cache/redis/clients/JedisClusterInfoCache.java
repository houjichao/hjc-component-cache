package com.hjc.component.cache.redis.clients;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Client;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.SafeEncoder;

public class JedisClusterInfoCache {
    private Map<String, JedisClusterPool> nodes             = new ConcurrentHashMap<String, JedisClusterPool>();
    private Map<Integer, JedisSlotInfo>   slots             = new ConcurrentHashMap<Integer, JedisSlotInfo>();
    
    private final ReentrantReadWriteLock  rwl               = new ReentrantReadWriteLock();
    private final Lock                    r                 = rwl.readLock();
    private final Lock                    w                 = rwl.writeLock();
    private volatile boolean              rediscovering;
    private final GenericObjectPoolConfig poolConfig;
    
    private int                           connectionTimeout;
    private int                           soTimeout;
    private String                        password;
    private static final int              MASTER_NODE_INDEX = 2;
    
    public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig, int timeout) {
        this(poolConfig, timeout, timeout, null);
    }
    
    public JedisClusterInfoCache(final GenericObjectPoolConfig poolConfig,
        final int connectionTimeout, final int soTimeout, final String password) {
        this.poolConfig = poolConfig;
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
    }
    
    public void discoverClusterNodesAndSlots(Jedis jedis) {
        w.lock();
        try {
            _reset();
            this.slots = discoverClusterSlots(jedis);
        } finally {
            w.unlock();
        }
    }
    
    public void renewClusterSlots(Jedis jedis) {
        //If rediscovering is already in process - no need to start one more same rediscovering, just return
        if (!rediscovering) {
            try {
                w.lock();
                rediscovering = true;
                
                if (jedis != null) {
                    try {
                        Map<Integer, JedisSlotInfo> mapSlots = this.slots;
                        this.slots = discoverClusterSlots(jedis);
                        mapSlots.clear();
                        mapSlots = null;
                        return;
                    } catch (JedisException e) {
                        //try nodes from all pools
                    }
                }
                
                for (JedisPool jp : getShuffledNodesPool()) {
                    try {
                        jedis = jp.getResource();
                        Map<Integer, JedisSlotInfo> mapSlots = this.slots;
                        this.slots = discoverClusterSlots(jedis);
                        mapSlots.clear();
                        mapSlots = null;
                        return;
                    } catch (JedisConnectionException e) {
                        // try next nodes
                    } finally {
                        if (jedis != null) {
                            jedis.close();
                        }
                    }
                }
            } finally {
                rediscovering = false;
                w.unlock();
            }
        }
    }
    
    @SuppressWarnings("unchecked")
    private Map<Integer, JedisSlotInfo> discoverClusterSlots(Jedis jedis) {
        Map<Integer, JedisSlotInfo> mapSlots = new HashMap<Integer, JedisSlotInfo>();
        List<Object> slots = jedis.clusterSlots();
        for (Object slotInfoObj : slots) {
            List<Object> slotInfo = (List<Object>) slotInfoObj;
            if (slotInfo.size() <= MASTER_NODE_INDEX) {
                continue;
            }
            
            List<Integer> slotNums = getAssignedSlotArray(slotInfo);
            
            //master
            List<Object> masterHostInfos = (List<Object>) slotInfo.get(MASTER_NODE_INDEX);
            HostAndPort masterNode = generateHostAndPort(masterHostInfos);
            JedisPool master = setupNodeIfNotExist(masterNode, true);
            
            // slaves
            int size = slotInfo.size();
            List<JedisPool> slaves = new ArrayList<>();
            for (int i = MASTER_NODE_INDEX + 1; i < size; ++i) {
                List<Object> hostInfos = (List<Object>) slotInfo.get(i);
                if (hostInfos.size() <= 0) {
                    continue;
                }
                
                HostAndPort slaveNode = generateHostAndPort(hostInfos);
                JedisPool slavePool = setupNodeIfNotExist(slaveNode, false);
                slaves.add(slavePool);
            }
            
            //cache slotInfo
            JedisSlotInfo jedisSlotInfo = new JedisSlotInfo(master, slaves);
            for (Integer slot : slotNums) {
                mapSlots.put(slot, jedisSlotInfo);
            }
        }
        return mapSlots;
    }
    
    private HostAndPort generateHostAndPort(List<Object> hostInfos) {
        return new HostAndPort(SafeEncoder.encode((byte[]) hostInfos.get(0)),
            ((Long) hostInfos.get(1)).intValue());
    }
    
    public JedisPool setupNodeIfNotExist(HostAndPort node, boolean isMaster) {
        w.lock();
        try {
            boolean readonly = true;
            if (isMaster) {
                readonly = false;
            }
            
            String nodeKey = getNodeKey(node);
            JedisClusterPool existingPool = nodes.get(nodeKey);
            if (existingPool != null) {
                if (readonly) {
                    existingPool.setReadonly(readonly);
                }
                return existingPool;
            }
            
            JedisClusterPool nodePool = new JedisClusterPool(poolConfig, node.getHost(),
                node.getPort(), connectionTimeout, soTimeout, password, 0, null, false, null, null,
                null, readonly);
            nodes.put(nodeKey, nodePool);
            return nodePool;
        } finally {
            w.unlock();
        }
    }
    
    public JedisPool getNode(String nodeKey) {
        r.lock();
        try {
            return nodes.get(nodeKey);
        } finally {
            r.unlock();
        }
    }
    
    public JedisSlotInfo getSlotPool(int slot) {
        r.lock();
        try {
            return slots.get(slot);
        } finally {
            r.unlock();
        }
    }
    
    public Map<String, JedisPool> getNodes() {
        r.lock();
        try {
            return new HashMap<String, JedisPool>(nodes);
        } finally {
            r.unlock();
        }
    }
    
    public List<JedisPool> getShuffledNodesPool() {
        r.lock();
        try {
            List<JedisPool> pools = new ArrayList<JedisPool>(nodes.values());
            Collections.shuffle(pools);
            return pools;
        } finally {
            r.unlock();
        }
    }
    
    public void reset() {
        w.lock();
        try {
            _reset();
        } finally {
            w.unlock();
        }
    }
    
    /**
     * Clear discovered nodes collections and gently release allocated resources
     */
    private void _reset() {
        slots.clear();
        for (JedisPool pool : nodes.values()) {
            if (pool != null) {
                try {
                    pool.destroy();
                } catch (Throwable ignore) {
                }
            }
        }
        nodes.clear();
    }
    
    public static String getNodeKey(HostAndPort hnp) {
        return hnp.getHost() + ":" + hnp.getPort();
    }
    
    public static String getNodeKey(Client client) {
        return client.getHost() + ":" + client.getPort();
    }
    
    public static String getNodeKey(Jedis jedis) {
        return getNodeKey(jedis.getClient());
    }
    
    private List<Integer> getAssignedSlotArray(List<Object> slotInfo) {
        List<Integer> slotNums = new ArrayList<Integer>();
        for (int slot = ((Long) slotInfo.get(0)).intValue(); slot <= ((Long) slotInfo.get(1))
            .intValue(); slot++) {
            slotNums.add(slot);
        }
        return slotNums;
    }
}
