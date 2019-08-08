package com.hjc.component.cache.threadcache;

/**
 * 缓存数据加载
 * 
 * @author hjc
 */
public interface ICacheLoader {
	/**
	 * 根据key装载缓存
	 * 
	 * @param key
	 *            缓存对象Key
	 * @return 缓存对象
	 */
	Object loadData(Object key);
}
