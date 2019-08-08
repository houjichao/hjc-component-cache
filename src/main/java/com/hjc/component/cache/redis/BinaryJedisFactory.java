package com.hjc.component.cache.redis;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import com.hjc.component.cache.util.StringHelper;
import com.hjc.component.cache.util.config.SysConfig;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;


public class BinaryJedisFactory implements InitializingBean, DisposableBean {
    private static final String                    APPID      = "hjc.application.id";
    private static final Map<String, IBinaryJedis> jedisCache = new ConcurrentHashMap<String, IBinaryJedis>();
    protected static final Logger logger     = LoggerFactory
        .getLogger(BinaryJedisFactory.class);
    private static String  appId;   //默认的APPID
    
    static {
        appId = loadAppId();
    }
    
    public static String getAppId() {
        return appId;
    }
    
    /**
     * 获取部署的应用ID
     * 
     * @return 应用ID
     */
    private static String loadAppId() {
        String appId = SysConfig.getProperty(APPID);
        if (StringHelper.isEmpty(appId)) {
            appId = System.getProperty(APPID);
        }
        if (StringHelper.isEmpty(appId)) {
            appId = System.getenv(APPID);
        }
        return appId;
    }
    
    public static IBinaryJedis jedis(String redisCfg) {
        Properties props = getProperties(redisCfg);
        return jedis(redisCfg, props);
    }
    
    public static IBinaryJedis jedis(String configKey, Properties props) {
        String jedisClass = props.getProperty("jedis-class", "pool");
        String key = new StringBuilder(configKey).append("@").append(jedisClass).toString();
        IBinaryJedis jedis = jedisCache.get(key);
        if (jedis != null) {
            return jedis;
        }
        
        synchronized (jedisCache) {
            jedis = jedisCache.get(key);
            if (jedis != null) {
                return jedis;
            }

            if ("hjc-cluster".equals(jedisClass)) {//改造后的客户端
                jedis = new BinaryJedisCluster(
                        getProperties(props, "jedis-cluster."));
            } else if ("cluster".equals(jedisClass)) {
                jedis = new BinaryJedisCluster(getProperties(props, "jedis-cluster."));
            } else if ("sentinel".equals(jedisClass)) {
                jedis = new BinaryJedisSentinel(getProperties(props, "jedis-sentinel"));
            } else {// pool
                jedis = new BinaryJedisPool(getProperties(props, "jedis-pool."));
            }
            jedisCache.put(key, jedis);
        }
        
        return jedis;
        
    }
    
    protected static Properties getProperties(String redisCfg) {
        InputStream is = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(redisCfg);
            if (is != null) {
                Properties progs = new Properties();
                progs.load(is);
                return progs;
            }
        } catch (IOException ignore) {
        } finally {
            IOUtils.closeQuietly(is);
        }
        
        return SysConfig.getProperties("redis.");
    }
    
    /**
     * 获取指定前缀的属性
     * 
     * @param prefix
     *            前缀
     * @return 指定前缀的属性
     */
    protected static Properties getProperties(Properties props, String prefix) {
        Properties new_props = new Properties();
        Enumeration<Object> keys = props.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            if (key.startsWith(prefix)) {
                new_props.setProperty(key.substring(prefix.length()), props.getProperty(key));
            }
        }
        return new_props;
    }
    
    @Override
    public void destroy() throws Exception {
        Map<String, IBinaryJedis> map = new ConcurrentHashMap<String, IBinaryJedis>();
        
        synchronized (jedisCache) {
            map.putAll(jedisCache);
            jedisCache.clear();
        }
        
        for (IBinaryJedis jedis : map.values()) {
            try {
                jedis.close();
            } catch (Throwable t) {
                logger.error("关闭jedis池失败", t);
            }
        }
    }
    
    @Override
    public void afterPropertiesSet() throws Exception {
        jedis(RedisCache.DEFAULT_REDIS);
    }
    
}
