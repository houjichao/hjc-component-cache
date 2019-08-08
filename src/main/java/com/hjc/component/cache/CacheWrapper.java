package com.hjc.component.cache;

import java.io.Serializable;

/**
 * 对缓存数据进行封装
 * 
 * @author hjc
 *
 */
public class CacheWrapper implements Serializable {
	private static final long serialVersionUID = 7607808187157549319L;
	/**
	 * 缓存数据
	 */
	private Object cacheObject;
	/**
	 * 加载时间
	 */
	private long lastLoadTime;
	/**
	 * 缓存时长
	 */
	private int expire;

	public CacheWrapper() {
	}

	public CacheWrapper(Object cacheObject, int expire) {
		this.cacheObject = cacheObject;
		this.lastLoadTime = System.currentTimeMillis();
		this.expire = expire;
	}

	public long getLastLoadTime() {
		return lastLoadTime;
	}

	public void setLastLoadTime(long lastLoadTime) {
		this.lastLoadTime = lastLoadTime;
	}

	public Object getCacheObject() {
		return cacheObject;
	}

	public void setCacheObject(Object cacheObject) {
		this.cacheObject = cacheObject;
	}

	public int getExpire() {
		return expire;
	}

	public void setExpire(int expire) {
		this.expire = expire;
	}

	/**
	 * 判断缓存是否已经过期
	 * 
	 * @return boolean
	 */
	public boolean isExpired() {
		if (expire > 0) {
			long t = System.currentTimeMillis() - lastLoadTime - expire * 1000;
			if (t > 0) {
				return true;
			}
		}
		return false;
	}
}
