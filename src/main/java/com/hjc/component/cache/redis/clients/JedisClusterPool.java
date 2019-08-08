package com.hjc.component.cache.redis.clients;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocketFactory;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.JedisPool;

public class JedisClusterPool extends JedisPool {
    private final JedisClusterFactory factory;
    
    public JedisClusterPool(final GenericObjectPoolConfig poolConfig, final String host, int port,
        final int connectionTimeout, final int soTimeout, final String password, final int database,
        final String clientName, final boolean ssl, final SSLSocketFactory sslSocketFactory,
        final SSLParameters sslParameters, final HostnameVerifier hostnameVerifier,
        final boolean isMaster) {
        this.factory = new JedisClusterFactory(host, port, connectionTimeout, soTimeout, password,
            database, clientName, ssl, sslSocketFactory, sslParameters, hostnameVerifier, isMaster);
        this.initPool(poolConfig, factory);
    }
    
    public void setReadonly(boolean readonly) {
        this.factory.setReadonly(readonly);
    }
    
    public boolean isReadonly() {
        return this.factory.isReadonly();
    }
    
}
