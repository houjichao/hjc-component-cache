package com.hjc.component.cache.redis.clients;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisAskDataException;
import redis.clients.jedis.exceptions.JedisClusterException;
import redis.clients.jedis.exceptions.JedisClusterMaxRedirectionsException;
import redis.clients.jedis.exceptions.JedisConnectionException;
import redis.clients.jedis.exceptions.JedisMovedDataException;
import redis.clients.jedis.exceptions.JedisNoReachableClusterNodeException;
import redis.clients.jedis.exceptions.JedisRedirectionException;
import redis.clients.util.JedisClusterCRC16;
import redis.clients.util.SafeEncoder;

public abstract class JedisClusterCommand<T> {
    private static final Logger logger = LoggerFactory
        .getLogger(JedisClusterCommand.class);
    private JedisClusterConnectionHandler connectionHandler;
    private int                           maxAttempts;
    
    public JedisClusterCommand(JedisClusterConnectionHandler connectionHandler, int maxAttempts) {
        this.connectionHandler = connectionHandler;
        this.maxAttempts = maxAttempts;
    }
    
    public abstract T execute(Jedis connection);
    
    private T doExecute(Jedis connection, byte[] key, boolean readonly) {
        long st = System.currentTimeMillis();
        T out = null;
        try {
            out = execute(connection);
        } finally {
            long cst = System.currentTimeMillis() - st;
            if (cst > connectionHandler.getSlowtime()) {
                StringBuilder msg = new StringBuilder(JedisClusterInfoCache.getNodeKey(connection))
                    .append(" -> ").append(cst).append(" ms,key: " + SafeEncoder.encode(key));
                if (out != null && out.getClass() == byte[].class) {
                    byte[] buf = (byte[]) out;
                    msg.append(" value's length: " + buf.length);
                }
                logger.warn(msg.toString());
            }
        }
        return out;
    }
    
    public T runBinary(boolean readonly, byte[] key) {
        if (key == null) {
            String msg = "No way to dispatch this command to Redis Cluster.";
            if (connectionHandler.isQuietly()) {
                logger.error(msg);
                return null;
            }
            throw new JedisClusterException(msg);
        }
        return runWithRetries(key, this.maxAttempts, false, null, readonly);
    }
    
    public T runBinary(boolean readonly, int keyCount, byte[]... keys) {
        if (keys == null || keys.length == 0) {
            String msg = "No way to dispatch this command to Redis Cluster.";
            if (connectionHandler.isQuietly()) {
                logger.error(msg);
                return null;
            }
            throw new JedisClusterException(msg);
        }
        
        // For multiple keys, only execute if they all share the
        // same connection slot.
        if (keys.length > 1) {
            int slot = JedisClusterCRC16.getSlot(keys[0]);
            for (int i = 1; i < keyCount; i++) {
                int nextSlot = JedisClusterCRC16.getSlot(keys[i]);
                if (slot != nextSlot) {
                    String msg = "No way to dispatch this command to Redis Cluster  because keys have different slots.";
                    logger.error(msg);
                    throw new JedisClusterException(msg);
                }
            }
        }
        
        return runWithRetries(keys[0], this.maxAttempts, false, null, readonly);
    }
    
    private T runWithRetries(byte[] key, int attempts, boolean tryRandomNode, Jedis askJedis,
        boolean readonly) {
        if (attempts <= 0) {
            String msg = "Too many Cluster redirections?";
            if (connectionHandler.isQuietly()) {
                logger.error(msg);
                return null;
            }
            throw new JedisClusterMaxRedirectionsException(msg);
        }
        
        Jedis jedis = null;
        try {
            if (askJedis != null) {//指定节点
                jedis = askJedis;
                jedis.asking();
            } else {
                if (tryRandomNode) {//随机
                    jedis = connectionHandler.getJedis();
                } else {
                    int slot = JedisClusterCRC16.getSlot(key);
                    if (readonly) {//只读
                        jedis = connectionHandler.getReadJedisFromSlot(slot);
                    } else {//读写
                        jedis = connectionHandler.getMasterJedisFromSlot(slot);
                    }
                }
            }
            
            return doExecute(jedis, key, readonly);
        } catch (JedisNoReachableClusterNodeException jnrcne) {
            if (connectionHandler.isQuietly()) {
                logger.error(jnrcne.getMessage(), jnrcne);
                return null;
            }
            throw jnrcne;
        } catch (JedisConnectionException jce) {
            // release current connection before recursion
            releaseConnection(jedis);
            jedis = null;
            
            if (attempts <= 1) {
                //We need this because if node is not reachable anymore - we need to finally initiate slots renewing,
                //or we can stuck with cluster state without one node in opposite case.
                //But now if maxAttempts = 1 or 2 we will do it too often. For each time-outed request.
                // make tracking of successful/unsuccessful operations for node - do renewing only
                //if there were no successful responses from this node last few seconds
                this.connectionHandler.renewSlotCache();
                //no more redirections left, throw original exception, not JedisClusterMaxRedirectionsException, because it's not MOVED situation
                logger.error("JedisConnectionException->renewSlotCache:" + jce.getMessage());
                if (connectionHandler.isQuietly()) {
                    return null;
                }
                throw jce;
            }
            
            if (readonly && attempts == 2) {
                readonly = false;//最后一次试试master
                logger.warn("JedisConnectionException->readonly set false", jce);
            }
            
            return runWithRetries(key, attempts - 1, tryRandomNode, null, readonly);
        } catch (JedisRedirectionException jre) {
            // if MOVED redirection occurred,
            if (jre instanceof JedisMovedDataException) {
                // it rebuilds cluster's slot cache
                // recommended by Redis cluster specification
                this.connectionHandler.renewSlotCache(jedis);
                logger.warn("JedisMovedDataException->renewSlotCache:" + jre.getMessage());
            }
            
            // release current connection before recursion or renewing
            releaseConnection(jedis);
            jedis = null;
            
            askJedis = null;
            if (jre instanceof JedisAskDataException) {
                HostAndPort targetNode = jre.getTargetNode();
                askJedis = this.connectionHandler.getJedisFromNode(targetNode);
                logger.warn("JedisAskDataException->TargetNode:" + targetNode.getHost() + ":"
                    + targetNode.getPort());
            } else if (jre instanceof JedisMovedDataException) {
            } else {
                if (connectionHandler.isQuietly()) {
                    logger.error("JedisRedirectionException->" + jre.getMessage());
                    return null;
                }
                throw new JedisClusterException(jre);
            }
            
            if (attempts == 1) {
                readonly = false;//最后一次试试master
                logger.warn("JedisRedirectionException->readonly set false", jre);
            }
            
            return runWithRetries(key, attempts - 1, false, askJedis, readonly);
        } finally {
            releaseConnection(jedis);
        }
    }
    
    private void releaseConnection(Jedis connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Throwable t) {
                logger.warn("releaseConnection error!", t);
            }
        }
    }
    
}
