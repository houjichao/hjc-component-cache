package com.hjc.component.cache.redis.clients;

import java.net.URI;
import java.util.concurrent.atomic.AtomicReference;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.BinaryJedis;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.InvalidURIException;
import redis.clients.jedis.exceptions.JedisException;
import redis.clients.util.JedisURIHelper;

public class JedisClusterFactory implements PooledObjectFactory<Jedis> {
    private static final Logger logger      = LoggerFactory
        .getLogger(JedisClusterFactory.class);
    private final AtomicReference<HostAndPort> hostAndPort = new AtomicReference<HostAndPort>();
    private final int                          connectionTimeout;
    private final int                          soTimeout;
    private final String                       password;
    private final int                          database;
    private final String                       clientName;
    private final boolean                      ssl;
    private final SSLSocketFactory             sslSocketFactory;
    private SSLParameters                      sslParameters;
    private HostnameVerifier                   hostnameVerifier;
    private boolean                            readonly;
    
    JedisClusterFactory(final String host, final int port, final int connectionTimeout,
        final int soTimeout, final String password, final int database, final String clientName,
        final boolean ssl, final SSLSocketFactory sslSocketFactory,
        final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
        final boolean readonly) {
        this.hostAndPort.set(new HostAndPort(host, port));
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = password;
        this.database = database;
        this.clientName = clientName;
        this.ssl = ssl;
        this.sslSocketFactory = sslSocketFactory;
        this.sslParameters = sslParameters;
        this.hostnameVerifier = hostnameVerifier;
        this.readonly = readonly;
    }
    
    public JedisClusterFactory(final URI uri, final int connectionTimeout, final int soTimeout,
        final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
        final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
        final boolean readonly) {
        if (!JedisURIHelper.isValid(uri)) {
            throw new InvalidURIException(
                String.format("Cannot open Redis connection due invalid URI. %s", uri.toString()));
        }
        
        this.hostAndPort.set(new HostAndPort(uri.getHost(), uri.getPort()));
        this.connectionTimeout = connectionTimeout;
        this.soTimeout = soTimeout;
        this.password = JedisURIHelper.getPassword(uri);
        this.database = JedisURIHelper.getDBIndex(uri);
        this.clientName = clientName;
        this.ssl = ssl;
        this.sslSocketFactory = sslSocketFactory;
        this.sslParameters = sslParameters;
        this.hostnameVerifier = hostnameVerifier;
        this.readonly = readonly;
    }
    
    public boolean isReadonly() {
        return readonly;
    }
    
    public void setReadonly(boolean readonly) {
        this.readonly = readonly;
    }
    
    public void setHostAndPort(final HostAndPort hostAndPort) {
        this.hostAndPort.set(hostAndPort);
    }
    
    @Override
    public void activateObject(PooledObject<Jedis> pooledJedis) throws Exception {
        final BinaryJedis jedis = pooledJedis.getObject();
        if (jedis.getDB() != database) {
            jedis.select(database);
        }
    }
    
    @Override
    public void destroyObject(PooledObject<Jedis> pooledJedis) throws Exception {
        final BinaryJedis jedis = pooledJedis.getObject();
        if (jedis.isConnected()) {
            try {
                try {
                    jedis.quit();
                } catch (Exception e) {
                }
                jedis.disconnect();
            } catch (Exception e) {
            }
        }
    }
    
    @Override
    public PooledObject<Jedis> makeObject() throws Exception {
        final HostAndPort hostAndPort = this.hostAndPort.get();
        final Jedis jedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort(),
            connectionTimeout, soTimeout, ssl, sslSocketFactory, sslParameters, hostnameVerifier);
        
        try {
            jedis.connect();
            if (null != this.password) {
                jedis.auth(this.password);
            }
            if (database != 0) {
                jedis.select(database);
            }
            if (clientName != null) {
                jedis.clientSetname(clientName);
            }
            if (readonly) {
                jedis.readonly();
            }
        } catch (JedisException je) {
            jedis.close();
            throw je;
        }
        
        return new DefaultPooledObject<Jedis>(jedis);
        
    }
    
    @Override
    public void passivateObject(PooledObject<Jedis> pooledJedis) throws Exception {
    }
    
    @Override
    public boolean validateObject(PooledObject<Jedis> pooledJedis) {
        final BinaryJedis jedis = pooledJedis.getObject();
        try {
            HostAndPort hostAndPort = this.hostAndPort.get();
            String connectionHost = jedis.getClient().getHost();
            int connectionPort = jedis.getClient().getPort();
            
            if (hostAndPort.getHost().equals(connectionHost)
                && hostAndPort.getPort() == connectionPort) {
                if (jedis.isConnected()) {
                    String result = jedis.ping();
                    if (result.equalsIgnoreCase("PONG")) {
                        return true;
                    } else {
                        logger.warn("validate jedis ping failed!"
                            + JedisClusterInfoCache.getNodeKey(jedis.getClient()));
                        return false;
                    }
                }
            }
            logger.warn(
                "validate jedis false!" + JedisClusterInfoCache.getNodeKey(jedis.getClient()));
            return false;
        } catch (final Exception e) {
            logger.warn(
                "validate jedis false!" + JedisClusterInfoCache.getNodeKey(jedis.getClient()), e);
            return false;
        }
    }
}
