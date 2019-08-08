package com.hjc.component.cache.redis.clients;

import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;


import com.hjc.component.cache.redis.BaseBinaryJedis;
import redis.clients.jedis.BinaryClient.LIST_POSITION;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Protocol;

public class BinaryJedisCluster extends BaseBinaryJedis {
    protected static final short            HASHSLOTS                = 16384;
    protected static final int              DEFAULT_TIMEOUT          = 2000;
    protected static final int              DEFAULT_MAX_REDIRECTIONS = 5;
    protected int                           maxAttempts;
    protected JedisClusterConnectionHandler connectionHandler;
    
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
        int maxRedirections = getProperty(props, "max-redirections", DEFAULT_MAX_REDIRECTIONS);
        maxRedirections = Math.min(nodeSize, maxRedirections);
        
        String password = getProperty(props, "password", null);
        int slowtime = getProperty(props, "slowtime", 500);
        boolean quietly = getProperty(props, "quietly", true);
        
        this.connectionHandler = new JedisSlotBasedConnectionHandler(nodes, jpc, connectionTimeout,
            soTimeout, password, slowtime, quietly);
        this.maxAttempts = maxRedirections;
    }
    
    @Override
    public void close() throws IOException {
        if (connectionHandler != null) {
            connectionHandler.close();
        }
    }
    
    @Override
    public Long del(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.del(key);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Boolean exists(final byte[] key) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.exists(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long expire(final byte[] key, final int seconds) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.expire(key, seconds);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long expireAt(final byte[] key, final long unixTime) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.expireAt(key, unixTime);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long pexpire(final byte[] key, final long milliseconds) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.pexpire(key, milliseconds);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long pexpireAt(final byte[] key, final long millisecondsTimestamp) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.pexpireAt(key, millisecondsTimestamp);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long persist(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.persist(key);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long ttl(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.ttl(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public String type(final byte[] key) {
        return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
            @Override
            public String execute(Jedis connection) {
                return connection.type(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public String set(final byte[] key, final byte[] value) {
        return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
            @Override
            public String execute(Jedis connection) {
                return connection.set(key, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public byte[] get(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.get(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public byte[] getrange(final byte[] key, final long startOffset, final long endOffset) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.getrange(key, startOffset, endOffset);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public byte[] getSet(final byte[] key, final byte[] value) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.getSet(key, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Boolean getbit(final byte[] key, final long offset) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.getbit(key, offset);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Boolean setbit(final byte[] key, final long offset, final boolean value) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.setbit(key, offset, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public String setex(final byte[] key, final int seconds, final byte[] value) {
        return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
            @Override
            public String execute(Jedis connection) {
                return connection.setex(key, seconds, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long setnx(final byte[] key, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.setnx(key, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long setrange(final byte[] key, final long offset, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.setrange(key, offset, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long strlen(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.strlen(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long incr(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.incr(key);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long incrBy(final byte[] key, final long integer) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.incrBy(key, integer);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Double incrByFloat(final byte[] key, final double value) {
        return new JedisClusterCommand<Double>(connectionHandler, maxAttempts) {
            @Override
            public Double execute(Jedis connection) {
                return connection.incrByFloat(key, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long decr(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.decr(key);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long decrBy(final byte[] key, final long integer) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.decrBy(key, integer);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long append(final byte[] key, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.append(key, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long hdel(final byte[] key, final byte[]... field) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hdel(key, field);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Boolean hexists(final byte[] key, final byte[] field) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.hexists(key, field);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public byte[] hget(final byte[] key, final byte[] field) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.hget(key, field);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Map<byte[], byte[]> hgetAll(final byte[] key) {
        return new JedisClusterCommand<Map<byte[], byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Map<byte[], byte[]> execute(Jedis connection) {
                return connection.hgetAll(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long hincrBy(final byte[] key, final byte[] field, final long value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hincrBy(key, field, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Double hincrByFloat(final byte[] key, final byte[] field, final double value) {
        return new JedisClusterCommand<Double>(connectionHandler, maxAttempts) {
            @Override
            public Double execute(Jedis connection) {
                return connection.hincrByFloat(key, field, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Set<byte[]> hkeys(final byte[] key) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.hkeys(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long hlen(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hlen(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public List<byte[]> hmget(final byte[] key, final byte[]... fields) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.hmget(key, fields);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public String hmset(final byte[] key, final Map<byte[], byte[]> hash) {
        return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
            @Override
            public String execute(Jedis connection) {
                return connection.hmset(key, hash);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long hset(final byte[] key, final byte[] field, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hset(key, field, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long hsetnx(final byte[] key, final byte[] field, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.hsetnx(key, field, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Collection<byte[]> hvals(final byte[] key) {
        return new JedisClusterCommand<Collection<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Collection<byte[]> execute(Jedis connection) {
                return connection.hvals(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public byte[] lindex(final byte[] key, final long index) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.lindex(key, index);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long linsert(final byte[] key, final LIST_POSITION where, final byte[] pivot,
        final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.linsert(key, where, pivot, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long llen(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.llen(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public byte[] lpop(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.lpop(key);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long lpush(final byte[] key, final byte[]... args) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.lpush(key, args);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long lpushx(final byte[] key, final byte[]... args) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.lpushx(key, args);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public List<byte[]> lrange(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<List<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public List<byte[]> execute(Jedis connection) {
                return connection.lrange(key, start, end);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long lrem(final byte[] key, final long count, final byte[] value) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.lrem(key, count, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public String lset(final byte[] key, final long index, final byte[] value) {
        return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
            @Override
            public String execute(Jedis connection) {
                return connection.lset(key, index, value);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public String ltrim(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<String>(connectionHandler, maxAttempts) {
            @Override
            public String execute(Jedis connection) {
                return connection.ltrim(key, start, end);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public byte[] rpop(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.rpop(key);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long rpush(final byte[] key, final byte[]... args) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.rpush(key, args);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long rpushx(final byte[] key, final byte[]... args) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.rpushx(key, args);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long sadd(final byte[] key, final byte[]... member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.sadd(key, member);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long scard(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.sadd(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Boolean sismember(final byte[] key, final byte[] member) {
        return new JedisClusterCommand<Boolean>(connectionHandler, maxAttempts) {
            @Override
            public Boolean execute(Jedis connection) {
                return connection.sismember(key, member);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Set<byte[]> smembers(final byte[] key) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.smembers(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public byte[] spop(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.spop(key);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public byte[] srandmember(final byte[] key) {
        return new JedisClusterCommand<byte[]>(connectionHandler, maxAttempts) {
            @Override
            public byte[] execute(Jedis connection) {
                return connection.srandmember(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long srem(final byte[] key, final byte[]... member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.srem(key, member);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long zadd(final byte[] key, final double score, final byte[] member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zadd(key, score, member);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long zadd(final byte[] key, final Map<byte[], Double> scoreMembers) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zadd(key, scoreMembers);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long zcard(final byte[] key) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zcard(key);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long zcount(final byte[] key, final double min, final double max) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zcount(key, min, max);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Double zincrby(final byte[] key, final double score, final byte[] member) {
        return new JedisClusterCommand<Double>(connectionHandler, maxAttempts) {
            @Override
            public Double execute(Jedis connection) {
                return connection.zincrby(key, score, member);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long zlexcount(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zlexcount(key, min, max);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Set<byte[]> zrange(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrange(key, start, end);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByLex(key, min, max);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Set<byte[]> zrangeByLex(final byte[] key, final byte[] min, final byte[] max,
        final int offset, final int count) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByLex(key, min, max, offset, count);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Set<byte[]> zrangeByScore(final byte[] key, final double min, final double max) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrangeByScore(key, min, max);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long zrank(final byte[] key, final byte[] member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zrank(key, member);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Long zrem(final byte[] key, final byte[]... member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zrem(key, member);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long zremrangeByLex(final byte[] key, final byte[] min, final byte[] max) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zremrangeByLex(key, min, max);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long zremrangeByRank(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zremrangeByRank(key, start, end);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long zremrangeByScore(final byte[] key, final double min, final double max) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zremrangeByScore(key, min, max);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Set<byte[]> zrevrange(final byte[] key, final long start, final long end) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrange(key, start, end);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Set<byte[]> zrevrangeByScore(final byte[] key, final double max, final double min) {
        return new JedisClusterCommand<Set<byte[]>>(connectionHandler, maxAttempts) {
            @Override
            public Set<byte[]> execute(Jedis connection) {
                return connection.zrevrangeByScore(key, max, min);
            }
        }.runBinary(false, key);
    }
    
    @Override
    public Long zrevrank(final byte[] key, final byte[] member) {
        return new JedisClusterCommand<Long>(connectionHandler, maxAttempts) {
            @Override
            public Long execute(Jedis connection) {
                return connection.zrevrank(key, member);
            }
        }.runBinary(true, key);
    }
    
    @Override
    public Double zscore(final byte[] key, final byte[] member) {
        return new JedisClusterCommand<Double>(connectionHandler, maxAttempts) {
            @Override
            public Double execute(Jedis connection) {
                return connection.zscore(key, member);
            }
        }.runBinary(true, key);
    }
    
}
