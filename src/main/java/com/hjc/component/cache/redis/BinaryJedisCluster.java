package com.hjc.component.cache.redis;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

/**
 * 基于Redis Cluster集群的实现
 * 
 * @author hjc
 *
 */
public class BinaryJedisCluster extends BaseBinaryJedis {
    private redis.clients.jedis.BinaryJedisCluster jedisCluster;
    
    public BinaryJedisCluster(Properties props) {
        JedisPoolConfig jpc = getConfig(props);
        
        Set<HostAndPort> nodes = new HashSet<HostAndPort>();
        int nodeSize = getNodeSize(props);// 节点数
        for (int i = 1; i <= nodeSize; ++i) {// 从1开始配置
            String host = getProperty(props, "node-host-" + i, Protocol.DEFAULT_HOST);
            int port = getProperty(props, "node-port-" + i, Protocol.DEFAULT_PORT);
            nodes.add(new HostAndPort(host, port));
        }
        
        int connectionTimeout = getProperty(props, "timeout", Protocol.DEFAULT_TIMEOUT);
        int soTimeout = getProperty(props, "sotimeout", Protocol.DEFAULT_TIMEOUT);
        int maxRedirections = getProperty(props, "max-redirections", 5);
        maxRedirections = Math.min(nodeSize, maxRedirections);
        String password = getProperty(props, "password", null);
        
        this.jedisCluster = new redis.clients.jedis.BinaryJedisCluster(nodes, connectionTimeout,
            soTimeout, maxRedirections, password, jpc);
    }
    
    @Override
    public Long del(byte[] key) {
        return jedisCluster.del(key);
    }
    
    @Override
    public Boolean exists(byte[] key) {
        return jedisCluster.exists(key);
    }
    
    @Override
    public Long expire(byte[] key, int seconds) {
        return jedisCluster.expire(key, seconds);
    }
    
    @Override
    public Long expireAt(byte[] key, long unixTime) {
        return jedisCluster.expireAt(key, unixTime);
    }
    
    @Override
    public Long pexpire(byte[] key, long milliseconds) {
        return jedisCluster.pexpire(key, milliseconds);
    }
    
    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        return jedisCluster.pexpireAt(key, millisecondsTimestamp);
    }
    
    @Override
    public Long persist(byte[] key) {
        return jedisCluster.persist(key);
    }
    
    @Override
    public Long ttl(byte[] key) {
        return jedisCluster.ttl(key);
    }
    
    @Override
    public String type(byte[] key) {
        return jedisCluster.type(key);
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public String set(byte[] key, byte[] value) {
        return jedisCluster.set(key, value);
    }
    
    @Override
    public byte[] get(byte[] key) {
        return jedisCluster.get(key);
    }
    
    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        return jedisCluster.getrange(key, startOffset, endOffset);
    }
    
    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        return jedisCluster.getSet(key, value);
    }
    
    @Override
    public Boolean getbit(byte[] key, long offset) {
        return jedisCluster.getbit(key, offset);
    }
    
    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        return jedisCluster.setbit(key, offset, value);
    }
    
    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        return jedisCluster.setex(key, seconds, value);
    }
    
    @Override
    public Long setnx(byte[] key, byte[] value) {
        return jedisCluster.setnx(key, value);
    }
    
    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        return jedisCluster.setrange(key, offset, value);
    }
    
    @Override
    public Long strlen(byte[] key) {
        return jedisCluster.strlen(key);
    }
    
    @Override
    public Long incr(byte[] key) {
        return jedisCluster.incr(key);
    }
    
    @Override
    public Long incrBy(byte[] key, long integer) {
        return jedisCluster.incrBy(key, integer);
    }
    
    @Override
    public Double incrByFloat(byte[] key, double value) {
        return jedisCluster.incrByFloat(key, value);
    }
    
    @Override
    public Long decr(byte[] key) {
        return jedisCluster.decr(key);
    }
    
    @Override
    public Long decrBy(byte[] key, long integer) {
        return jedisCluster.decrBy(key, integer);
    }
    
    @Override
    public Long append(byte[] key, byte[] value) {
        return jedisCluster.append(key, value);
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public Long hdel(byte[] key, byte[]... field) {
        return jedisCluster.hdel(key, field);
    }
    
    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        return jedisCluster.hexists(key, field);
    }
    
    @Override
    public byte[] hget(byte[] key, byte[] field) {
        return jedisCluster.hget(key, field);
    }
    
    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        return jedisCluster.hgetAll(key);
    }
    
    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        return jedisCluster.hincrBy(key, field, value);
    }
    
    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        return jedisCluster.hincrByFloat(key, field, value);
    }
    
    @Override
    public Set<byte[]> hkeys(byte[] key) {
        return jedisCluster.hkeys(key);
    }
    
    @Override
    public Long hlen(byte[] key) {
        return jedisCluster.hlen(key);
    }
    
    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        return jedisCluster.hmget(key, fields);
    }
    
    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        return jedisCluster.hmset(key, hash);
    }
    
    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        return jedisCluster.hset(key, field, value);
    }
    
    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        return jedisCluster.hsetnx(key, field, value);
    }
    
    @Override
    public Collection<byte[]> hvals(byte[] key) {
        return jedisCluster.hvals(key);
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public byte[] lindex(byte[] key, long index) {
        return jedisCluster.lindex(key, index);
    }
    
    @Override
    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        return jedisCluster.linsert(key, where, pivot, value);
    }
    
    @Override
    public Long llen(byte[] key) {
        return jedisCluster.llen(key);
    }
    
    @Override
    public byte[] lpop(byte[] key) {
        return jedisCluster.lpop(key);
    }
    
    @Override
    public Long lpush(byte[] key, byte[]... args) {
        return jedisCluster.lpush(key, args);
    }
    
    @Override
    public Long lpushx(byte[] key, byte[]... args) {
        return jedisCluster.lpushx(key, args);
    }
    
    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {
        return jedisCluster.lrange(key, start, end);
    }
    
    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        return jedisCluster.lrem(key, count, value);
    }
    
    @Override
    public String lset(byte[] key, long index, byte[] value) {
        return jedisCluster.lset(key, index, value);
    }
    
    @Override
    public String ltrim(byte[] key, long start, long end) {
        return jedisCluster.ltrim(key, start, end);
    }
    
    @Override
    public byte[] rpop(byte[] key) {
        return jedisCluster.rpop(key);
    }
    
    @Override
    public Long rpush(byte[] key, byte[]... args) {
        return jedisCluster.rpush(key, args);
    }
    
    @Override
    public Long rpushx(byte[] key, byte[]... args) {
        return jedisCluster.rpushx(key, args);
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public Long sadd(byte[] key, byte[]... member) {
        return jedisCluster.sadd(key, member);
    }
    
    @Override
    public Long scard(byte[] key) {
        return jedisCluster.scard(key);
    }
    
    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        return jedisCluster.sismember(key, member);
    }
    
    @Override
    public Set<byte[]> smembers(byte[] key) {
        return jedisCluster.smembers(key);
    }
    
    @Override
    public byte[] spop(byte[] key) {
        return jedisCluster.spop(key);
    }
    
    @Override
    public byte[] srandmember(byte[] key) {
        return jedisCluster.srandmember(key);
    }
    
    @Override
    public Long srem(byte[] key, byte[]... member) {
        return jedisCluster.srem(key, member);
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        return jedisCluster.zadd(key, score, member);
    }
    
    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        return jedisCluster.zadd(key, scoreMembers);
    }
    
    @Override
    public Long zcard(byte[] key) {
        return jedisCluster.zcard(key);
    }
    
    @Override
    public Long zcount(byte[] key, double min, double max) {
        return jedisCluster.zcount(key, min, max);
    }
    
    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        return jedisCluster.zincrby(key, score, member);
    }
    
    @Override
    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        return jedisCluster.zlexcount(key, min, max);
    }
    
    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        return jedisCluster.zrange(key, start, end);
    }
    
    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        return jedisCluster.zrangeByLex(key, min, max);
    }
    
    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        return jedisCluster.zrangeByLex(key, min, max, offset, count);
    }
    
    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        return jedisCluster.zrangeByScore(key, min, max);
    }
    
    @Override
    public Long zrank(byte[] key, byte[] member) {
        return jedisCluster.zrank(key, member);
    }
    
    @Override
    public Long zrem(byte[] key, byte[]... member) {
        return jedisCluster.zrem(key, member);
    }
    
    @Override
    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        return jedisCluster.zremrangeByLex(key, min, max);
    }
    
    @Override
    public Long zremrangeByRank(byte[] key, long start, long end) {
        return jedisCluster.zremrangeByRank(key, start, end);
    }
    
    @Override
    public Long zremrangeByScore(byte[] key, double min, double max) {
        return jedisCluster.zremrangeByScore(key, min, max);
    }
    
    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        return jedisCluster.zrevrange(key, start, end);
    }
    
    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        return jedisCluster.zrevrangeByScore(key, min, max);
    }
    
    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        return jedisCluster.zrevrank(key, member);
    }
    
    @Override
    public Double zscore(byte[] key, byte[] member) {
        return jedisCluster.zscore(key, member);
    }
    
    @Override
    public void close() throws IOException {
        jedisCluster.close();
    }
    
    // -----------------------------------------------------------------------
    
}
