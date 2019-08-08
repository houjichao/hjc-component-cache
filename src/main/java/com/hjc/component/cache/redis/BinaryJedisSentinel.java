package com.hjc.component.cache.redis;

import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisSentinelPool;
import redis.clients.jedis.Protocol;
import redis.clients.util.Pool;

public class BinaryJedisSentinel extends BinaryJedisPool {

	public BinaryJedisSentinel(Properties props) {
		super(props);
	}

	@Override
	protected Pool<Jedis> initJedisPool(Properties props) {
		GenericObjectPoolConfig poolConfig = getConfig(props);
		String masterName = getProperty(props, "master-mame", "master");
		int timeout = getProperty(props, "timeout", Protocol.DEFAULT_TIMEOUT);
		String password = getProperty(props, "password", null);
		int database = getProperty(props, "database", Protocol.DEFAULT_DATABASE);
		String clientName = getProperty(props, "client-name", null);

		Set<String> sentinels = new HashSet<String>();
		int nodeSize = getNodeSize(props);// 节点数
		String defaultAddress = Protocol.DEFAULT_HOST + ":" + Protocol.DEFAULT_PORT;
		for (int i = 1; i <= nodeSize; ++i) {// 从1开始配置
			String address = getProperty(props, "node-address-" + i, defaultAddress);
			sentinels.add(address);
		}
		return new JedisSentinelPool(masterName, sentinels, poolConfig, timeout, timeout, password, database,
				clientName);
	}
}
