package com.hjc.component.cache;

/**
 * 缓存接口
 * 
 * @author hjc
 *
 */
public interface ICache {
	/**
	 * 获取缓存
	 * 
	 * @param key
	 *            缓存KEY
	 * @return 缓存对象
	 */
	public CacheWrapper get(CacheKey key);

	/**
	 * 储存缓存，key存在直接覆盖
	 * 
	 * @param key
	 *            缓存KEY
	 * @param wrapper
	 *            缓存对象
	 */
	public void put(CacheKey key, CacheWrapper wrapper);

	/**
	 * 删除缓存
	 * 
	 * @param key
	 *            缓存KEY
	 */
	public void del(CacheKey key);

	/**
	 * 自增
	 * 
	 * @param key 缓存KEY
	 * @return
	 */
	public Long incr(CacheKey key);
	/**
	 * 自减
	 * 
	 * @param key 缓存KEY
	 * @return
	 */
	public Long decr(CacheKey key);
	
	/**
	 * 设置过期时间
	 * 
	 * @param key 缓存KEY
	 * @param expire 过期时间
	 * @return
	 */
	public Long expire(CacheKey key, int expire);
	
}
