package com.hjc.component.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.hjc.component.cache.jcs.JcsCache;
import com.hjc.component.cache.redis.BinaryJedisFactory;
import com.hjc.component.cache.redis.RedisCache;
import com.hjc.component.cache.util.StringHelper;
import org.apache.jcs.access.exception.CacheException;


/**
 * 缓存工厂类
 * 
 * @author hjc
 *
 */
public class CacheFactory {
    private static Map<String, ICache> jcsCache             = new ConcurrentHashMap<String, ICache>();
    private static Map<String, ICache> redisCache           = new ConcurrentHashMap<String, ICache>();
    private static String              DEFAULT_CACHE_REGION = "hjcpub";
    private static String              REDIS_DNY_PROPS      = "redis_dny.props";
    
    public static ICache getRedisCache(String redisCfg) {
        ICache cache = redisCache.get(redisCfg);
        if (cache == null) {
            cache = new RedisCache(redisCfg);
            redisCache.put(redisCfg, cache);
        }
        return cache;
    }
    
    private static ICache getRedisCache() {
        String appId = BinaryJedisFactory.getAppId();
        if (StringHelper.isNotEmpty(appId)) {
            String key = "appId=" + appId;
            ICache cache = redisCache.get(key);
            if (cache != null) {
                return cache;
            }
        }
        return getRedisCache(RedisCache.DEFAULT_REDIS);
    }
    
    public static ICache getCache(CacheType type, String region) {
        if (type == CacheType.REDIS) {
            return getRedisCache();//后台配置
        } else if (type == CacheType.REDIS_DNY) {
            return getRedisCache(REDIS_DNY_PROPS);//前台配置
        }
        
        if (type == CacheType.JCS) {
            if (StringHelper.isEmpty(region)) {
                region = DEFAULT_CACHE_REGION;
            }
            
            ICache cache = jcsCache.get(region);
            if (cache == null) {
                try {
                    cache = new JcsCache(region);
                    jcsCache.put(region, cache);
                } catch (CacheException e) {
                    throw new RuntimeException("获取JCS缓存失败", e);
                }
            }
            return cache;
        }
        
        throw new RuntimeException("不支持的缓存类型：" + type);
    }
}
