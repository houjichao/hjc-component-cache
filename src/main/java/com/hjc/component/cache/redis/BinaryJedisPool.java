package com.hjc.component.cache.redis;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;
import redis.clients.util.Pool;

/**
 * 单点部署的实现
 * 
 * @author hjc
 *
 */
public class BinaryJedisPool extends BaseBinaryJedis {
    private Pool<Jedis> jedisPool;
    
    public BinaryJedisPool(Properties props) {
        this.jedisPool = initJedisPool(props);
    }
    
    protected Pool<Jedis> initJedisPool(Properties props) {
        JedisPoolConfig jpc = getConfig(props);
        String host = getProperty(props, "host", Protocol.DEFAULT_HOST);
        int port = getProperty(props, "port", Protocol.DEFAULT_PORT);
        int timeout = getProperty(props, "timeout", Protocol.DEFAULT_TIMEOUT);
        String password = getProperty(props, "password", null);
        int database = getProperty(props, "database", Protocol.DEFAULT_DATABASE);
        String clientName = getProperty(props, "client-name", null);
        return new JedisPool(jpc, host, port, timeout, password, database, clientName);
    }
    
    private Jedis getJedis() {
        return jedisPool.getResource();
    }
    
    private void close(Jedis jedis) {
        if (jedis != null) {
            jedis.close();
        }
    }
    
    @Override
    public Long del(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.del(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Boolean exists(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.exists(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long expire(byte[] key, int seconds) {
        Jedis jedis = getJedis();
        try {
            return jedis.expire(key, seconds);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long expireAt(byte[] key, long unixTime) {
        Jedis jedis = getJedis();
        try {
            return jedis.expireAt(key, unixTime);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long pexpire(byte[] key, long milliseconds) {
        Jedis jedis = getJedis();
        try {
            return jedis.pexpire(key, milliseconds);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long pexpireAt(byte[] key, long millisecondsTimestamp) {
        Jedis jedis = getJedis();
        try {
            return jedis.pexpireAt(key, millisecondsTimestamp);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long persist(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.persist(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long ttl(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.ttl(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public String type(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.type(key);
        } finally {
            close(jedis);
        }
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public String set(byte[] key, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.set(key, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public byte[] get(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.get(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public byte[] getrange(byte[] key, long startOffset, long endOffset) {
        Jedis jedis = getJedis();
        try {
            return jedis.getrange(key, startOffset, endOffset);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public byte[] getSet(byte[] key, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.getSet(key, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Boolean getbit(byte[] key, long offset) {
        Jedis jedis = getJedis();
        try {
            return jedis.getbit(key, offset);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Boolean setbit(byte[] key, long offset, boolean value) {
        Jedis jedis = getJedis();
        try {
            return jedis.setbit(key, offset, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public String setex(byte[] key, int seconds, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.setex(key, seconds, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long setnx(byte[] key, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.setnx(key, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long setrange(byte[] key, long offset, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.setrange(key, offset, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long strlen(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.strlen(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long incr(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.incr(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long incrBy(byte[] key, long integer) {
        Jedis jedis = getJedis();
        try {
            return jedis.incrBy(key, integer);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Double incrByFloat(byte[] key, double value) {
        Jedis jedis = getJedis();
        try {
            return jedis.incrByFloat(key, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long decr(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.decr(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long decrBy(byte[] key, long integer) {
        Jedis jedis = getJedis();
        try {
            return jedis.decrBy(key, integer);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long append(byte[] key, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.append(key, value);
        } finally {
            close(jedis);
        }
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public Long hdel(byte[] key, byte[]... field) {
        Jedis jedis = getJedis();
        try {
            return jedis.hdel(key, field);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Boolean hexists(byte[] key, byte[] field) {
        Jedis jedis = getJedis();
        try {
            return jedis.hexists(key, field);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public byte[] hget(byte[] key, byte[] field) {
        Jedis jedis = getJedis();
        try {
            return jedis.hget(key, field);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Map<byte[], byte[]> hgetAll(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.hgetAll(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long hincrBy(byte[] key, byte[] field, long value) {
        Jedis jedis = getJedis();
        try {
            return jedis.hincrBy(key, field, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Double hincrByFloat(byte[] key, byte[] field, double value) {
        Jedis jedis = getJedis();
        try {
            return jedis.hincrByFloat(key, field, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Set<byte[]> hkeys(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.hkeys(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long hlen(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.hlen(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public List<byte[]> hmget(byte[] key, byte[]... fields) {
        Jedis jedis = getJedis();
        try {
            return jedis.hmget(key, fields);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public String hmset(byte[] key, Map<byte[], byte[]> hash) {
        Jedis jedis = getJedis();
        try {
            return jedis.hmset(key, hash);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long hset(byte[] key, byte[] field, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.hset(key, field, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long hsetnx(byte[] key, byte[] field, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.hsetnx(key, field, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Collection<byte[]> hvals(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.hvals(key);
        } finally {
            close(jedis);
        }
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public byte[] lindex(byte[] key, long index) {
        Jedis jedis = getJedis();
        try {
            return jedis.lindex(key, index);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long linsert(byte[] key, LIST_POSITION where, byte[] pivot, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.linsert(key, where, pivot, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long llen(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.llen(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public byte[] lpop(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.lpop(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long lpush(byte[] key, byte[]... args) {
        Jedis jedis = getJedis();
        try {
            return jedis.lpush(key, args);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long lpushx(byte[] key, byte[]... args) {
        Jedis jedis = getJedis();
        try {
            return jedis.lpushx(key, args);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public List<byte[]> lrange(byte[] key, long start, long end) {
        Jedis jedis = getJedis();
        try {
            return jedis.lrange(key, start, end);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long lrem(byte[] key, long count, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.lrem(key, count, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public String lset(byte[] key, long index, byte[] value) {
        Jedis jedis = getJedis();
        try {
            return jedis.lset(key, index, value);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public String ltrim(byte[] key, long start, long end) {
        Jedis jedis = getJedis();
        try {
            return jedis.ltrim(key, start, end);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public byte[] rpop(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.rpop(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long rpush(byte[] key, byte[]... args) {
        Jedis jedis = getJedis();
        try {
            return jedis.rpush(key, args);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long rpushx(byte[] key, byte[]... args) {
        Jedis jedis = getJedis();
        try {
            return jedis.rpushx(key, args);
        } finally {
            close(jedis);
        }
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public Long sadd(byte[] key, byte[]... member) {
        Jedis jedis = getJedis();
        try {
            return jedis.sadd(key, member);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long scard(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.scard(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Boolean sismember(byte[] key, byte[] member) {
        Jedis jedis = getJedis();
        try {
            return jedis.sismember(key, member);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Set<byte[]> smembers(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.smembers(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public byte[] spop(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.spop(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public byte[] srandmember(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.srandmember(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long srem(byte[] key, byte[]... member) {
        Jedis jedis = getJedis();
        try {
            return jedis.srem(key, member);
        } finally {
            close(jedis);
        }
    }
    
    // -----------------------------------------------------------------------
    
    @Override
    public Long zadd(byte[] key, double score, byte[] member) {
        Jedis jedis = getJedis();
        try {
            return jedis.zadd(key, score, member);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zadd(byte[] key, Map<byte[], Double> scoreMembers) {
        Jedis jedis = getJedis();
        try {
            return jedis.zadd(key, scoreMembers);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zcard(byte[] key) {
        Jedis jedis = getJedis();
        try {
            return jedis.zcard(key);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zcount(byte[] key, double min, double max) {
        Jedis jedis = getJedis();
        try {
            return jedis.zcount(key, min, max);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Double zincrby(byte[] key, double score, byte[] member) {
        Jedis jedis = getJedis();
        try {
            return jedis.zincrby(key, score, member);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zlexcount(byte[] key, byte[] min, byte[] max) {
        Jedis jedis = getJedis();
        try {
            return jedis.zlexcount(key, min, max);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Set<byte[]> zrange(byte[] key, long start, long end) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrange(key, start, end);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrangeByLex(key, min, max);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Set<byte[]> zrangeByLex(byte[] key, byte[] min, byte[] max, int offset, int count) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrangeByLex(key, min, max, offset, count);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Set<byte[]> zrangeByScore(byte[] key, double min, double max) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrangeByScore(key, min, max);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zrank(byte[] key, byte[] member) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrank(key, member);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zrem(byte[] key, byte[]... member) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrem(key, member);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zremrangeByLex(byte[] key, byte[] min, byte[] max) {
        Jedis jedis = getJedis();
        try {
            return jedis.zremrangeByLex(key, min, max);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zremrangeByRank(byte[] key, long start, long end) {
        Jedis jedis = getJedis();
        try {
            return jedis.zremrangeByRank(key, start, end);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zremrangeByScore(byte[] key, double min, double max) {
        Jedis jedis = getJedis();
        try {
            return jedis.zremrangeByScore(key, min, max);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Set<byte[]> zrevrange(byte[] key, long start, long end) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrevrange(key, start, end);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Set<byte[]> zrevrangeByScore(byte[] key, double max, double min) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrevrangeByScore(key, min, max);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Long zrevrank(byte[] key, byte[] member) {
        Jedis jedis = getJedis();
        try {
            return jedis.zrevrank(key, member);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public Double zscore(byte[] key, byte[] member) {
        Jedis jedis = getJedis();
        try {
            return jedis.zscore(key, member);
        } finally {
            close(jedis);
        }
    }
    
    @Override
    public void close() throws IOException {
        jedisPool.close();
    }
    
    // -----------------------------------------------------------------------
    
}
