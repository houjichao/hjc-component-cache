package com.hjc.component.cache.redis.clients;

import java.io.Closeable;
import java.util.Map;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

public abstract class JedisClusterConnectionHandler implements Closeable {
    protected final JedisClusterInfoCache cache;
    private final int                     slowtime;
    private final boolean                 quietly;
    
    public JedisClusterConnectionHandler(Set<HostAndPort> nodes,
        final GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout,
        String password, int slowtime, boolean quietly) {
        this.cache = new JedisClusterInfoCache(poolConfig, connectionTimeout, soTimeout, password);
        this.slowtime = slowtime;
        this.quietly = quietly;
        initializeSlotsCache(nodes, poolConfig, password);
    }
    
    public int getSlowtime() {
        return slowtime;
    }
    
    public boolean isQuietly() {
        return quietly;
    }
    
    abstract Jedis getJedis();
    
    abstract Jedis getMasterJedisFromSlot(int slot);
    
    abstract Jedis getReadJedisFromSlot(int slot);
    
    public Jedis getJedisFromNode(HostAndPort node) {
        return cache.setupNodeIfNotExist(node, true).getResource();
    }
    
    public Map<String, JedisPool> getNodes() {
        return cache.getNodes();
    }
    
    private void initializeSlotsCache(Set<HostAndPort> startNodes,
        GenericObjectPoolConfig poolConfig, String password) {
        for (HostAndPort hostAndPort : startNodes) {
            Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
            if (password != null) {
                jedis.auth(password);
            }
            try {
                cache.discoverClusterNodesAndSlots(jedis);
                break;
            } catch (JedisConnectionException e) {
                // try next nodes
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
    }
    
    public void renewSlotCache() {
        cache.renewClusterSlots(null);
    }
    
    public void renewSlotCache(Jedis jedis) {
        cache.renewClusterSlots(jedis);
    }
    
    @Override
    public void close() {
        cache.reset();
    }
}
