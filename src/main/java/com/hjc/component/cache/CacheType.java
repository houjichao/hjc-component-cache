package com.hjc.component.cache;

/**
 * 缓存类型
 * 
 * @author hjc
 *
 */
public enum CacheType {
    /**
     * JCS 缓存
     */
    JCS
    /**
     * 静态配置缓存（配置文件redis.props）
     */
    , REDIS
    /**
     * 实例数据缓存（配置文件redis_dny.props）
     */
    , REDIS_DNY
}
