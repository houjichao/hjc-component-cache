package com.hjc.component.cache.redis;

import java.util.Properties;

import org.apache.commons.lang.math.NumberUtils;

import redis.clients.jedis.JedisPoolConfig;

/**
 * IdclogRedis存储涉及的redis二进制命令的基类
 * 
 * @author hjc
 *
 */
public abstract class BaseBinaryJedis implements IBinaryJedis {
    /**
     * 获取key的值
     * 
     * @param key
     *            键名
     * @param defaultValue
     *            默认值
     * @return 如果键存在返回键值，否则返回默认值(string)
     */
    protected String getProperty(Properties props, String key, String defaultValue) {
        String value = props.getProperty(key, defaultValue);
        return value != null ? value.trim()
            : null;
    }
    
    /**
     * 获取key的值
     * 
     * @param key
     *            键名
     * @param defaultValue
     *            默认值
     * @return 如果键存在返回键值，否则返回默认值(int)
     */
    protected int getProperty(Properties props, String key, int defaultValue) {
        return NumberUtils.toInt(props.getProperty(key), defaultValue);
    }
    
    /**
     * 获取key的值
     * 
     * @param key
     *            键名
     * @param defaultValue
     *            默认值
     * @return 如果键存在返回键值，否则返回默认值(boolean)
     */
    protected boolean getProperty(Properties props, String key, boolean defaultValue) {
        return "true".equalsIgnoreCase(props.getProperty(key, String.valueOf(defaultValue)).trim());
    }
    
    /**
     * 获取Jedis连接池配置
     * 
     * @param props
     *            配置文件属性
     * @return Jedis连接池配置
     */
    protected JedisPoolConfig getConfig(Properties props) {
        JedisPoolConfig jpc = new JedisPoolConfig();
        
        //最小空闲时间
        jpc.setMinEvictableIdleTimeMillis(
            getProperty(props, "min-evictable-idle-time-millis", 10000));
        
        //回收资源线程的执行周期
        jpc.setTimeBetweenEvictionRunsMillis(
            getProperty(props, "time-between-eviction-runs-millis", 10));
        
        //回收资源的数量
        jpc.setNumTestsPerEvictionRun(getProperty(props, "num-tests-per-eviction-run", -1));
        
        //LIFO or FIFO
        jpc.setLifo(getProperty(props, "lifo", true));
        
        // 最大连接数
        jpc.setMaxTotal(getProperty(props, "max-total", 500));
        
        //最小空闲连接数
        jpc.setMinIdle(getProperty(props, "min-idle", 2));
        
        //最大空闲连接
        jpc.setMaxIdle(getProperty(props, "max-idle", 20));
        
        //获取连接超时等待
        jpc.setMaxWaitMillis(getProperty(props, "max-wait-millis", 50));
        
        jpc.setTestWhileIdle(getProperty(props, "test-while-idle", false));
        jpc.setTestOnBorrow(getProperty(props, "test-on-borrow", true));
        jpc.setTestOnReturn(getProperty(props, "test-on-return", false));
        jpc.setTestOnCreate(getProperty(props, "test-on-create", false));
        return jpc;
    }
    
    protected int getNodeSize(Properties props) {
        return getProperty(props, "redis-nodes", 0);
    }
    
}
