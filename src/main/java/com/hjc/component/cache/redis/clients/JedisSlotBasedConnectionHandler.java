package com.hjc.component.cache.redis.clients;

import java.util.List;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;

public class JedisSlotBasedConnectionHandler extends JedisClusterConnectionHandler {
    private static final Logger logger = LoggerFactory
        .getLogger(JedisSlotBasedConnectionHandler.class);
    
    public JedisSlotBasedConnectionHandler(Set<HostAndPort> nodes,
        GenericObjectPoolConfig poolConfig, int connectionTimeout, int soTimeout, String password,
        int slowtime, boolean quietly) {
        super(nodes, poolConfig, connectionTimeout, soTimeout, password, slowtime, quietly);
    }
    
    @Override
    Jedis getJedis() {
        // In antirez's redis-rb-cluster implementation,
        // getRandomConnection always return valid connection (able to
        // ping-pong)
        // or exception if all connections are invalid
        List<JedisPool> pools = cache.getShuffledNodesPool();
        for (JedisPool pool : pools) {
            Jedis jedis = null;
            try {
                jedis = pool.getResource();
                if (jedis == null) {
                    continue;
                }
                String result = jedis.ping();
                if (result.equalsIgnoreCase("pong")) {
                    return jedis;
                } else {
                    logger.error(JedisClusterInfoCache.getNodeKey(jedis) + " ping failed!");
                }
                jedis.close();
            } catch (JedisException ex) {
                if (jedis != null) {
                    jedis.close();
                }
            }
        }
        throw new JedisNoReachableClusterNodeException("No reachable node in cluster");
    }
    
    @Override
    Jedis getMasterJedisFromSlot(int slot) {
        JedisSlotInfo jedisSlotInfo = cache.getSlotPool(slot);
        if (jedisSlotInfo != null) {
            // It can't guaranteed to get valid connection because of node
            // assignment
            return getMasterJedis(jedisSlotInfo);
        } else {
            renewSlotCache(); //It's abnormal situation for cluster mode, that we have just nothing for slot, try to rediscover state
            jedisSlotInfo = cache.getSlotPool(slot);
            if (jedisSlotInfo != null) {
                return getMasterJedis(jedisSlotInfo);
            } else {
                //no choice, fallback to new connection to random node
                return getJedis();
            }
        }
    }
    
    private Jedis getMasterJedis(JedisSlotInfo jedisSlotInfo) {
        JedisPool master = jedisSlotInfo.getMasterPool();
        return master.getResource();
    }
    
    private Jedis getSlaveJedis(JedisSlotInfo jedisSlotInfo) {
        JedisPool slave = jedisSlotInfo.getReadPool();
        return slave.getResource();
    }
    
    @Override
    Jedis getReadJedisFromSlot(int slot) {
        JedisSlotInfo jedisSlotInfo = cache.getSlotPool(slot);
        if (jedisSlotInfo != null) {
            // It can't guaranteed to get valid connection because of node
            // assignment
            return getSlaveJedis(jedisSlotInfo);
        } else {
            renewSlotCache(); //It's abnormal situation for cluster mode, that we have just nothing for slot, try to rediscover state
            jedisSlotInfo = cache.getSlotPool(slot);
            if (jedisSlotInfo != null) {
                return getSlaveJedis(jedisSlotInfo);
            } else {
                //no choice, fallback to new connection to random node
                return getJedis();
            }
        }
    }
    
}
