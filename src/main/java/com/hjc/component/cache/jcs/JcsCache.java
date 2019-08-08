package com.hjc.component.cache.jcs;

import com.hjc.component.cache.CacheKey;
import com.hjc.component.cache.CacheWrapper;
import com.hjc.component.cache.ICache;
import org.apache.jcs.JCS;
import org.apache.jcs.access.exception.CacheException;
import org.apache.jcs.engine.ElementAttributes;


public class JcsCache implements ICache {
	private JCS cache;

	public JcsCache(String cacheRegion) throws CacheException {
		cache = JCS.getInstance(cacheRegion);
	}

	@Override
	public CacheWrapper get(CacheKey key) {
		String cacheObjKey = key.getFullKey();
		CacheWrapper wrapper = (CacheWrapper) cache.get(cacheObjKey);
		if (wrapper != null && wrapper.isExpired()) {
			try {
				// 删除缓存数据
				cache.remove(cacheObjKey);
			} catch (CacheException ignore) {
			}
			wrapper = null;
		}
		return wrapper;
	}

	@Override
	public void put(CacheKey key, CacheWrapper wrapper) {
		String cacheObjKey = key.getFullKey();
		try {
			if (wrapper.getExpire() > 0) {
				ElementAttributes eleAttr = new ElementAttributes();
				eleAttr.setMaxLifeSeconds(wrapper.getExpire());
				eleAttr.setIsEternal(false);
				eleAttr.setIdleTime(wrapper.getExpire());
				cache.put(cacheObjKey, wrapper, eleAttr);
			} else {
				cache.put(cacheObjKey, wrapper);
			}
		} catch (CacheException e) {
			throw new RuntimeException("缓存对象保存异常" + "cacheKey" + cacheObjKey, e);
		}
	}

	@Override
	public void del(CacheKey key) {
		String cacheObjKey = key.getFullKey();
		try {
			cache.remove(cacheObjKey);
		} catch (CacheException e) {
			throw new RuntimeException("缓存对象保存异常" + "cacheKey" + cacheObjKey, e);
		}
	}

	@Override
    public Long incr(CacheKey key) {
        return null;
    }

    @Override
    public Long expire(CacheKey key, int expire) {
        return null;
    }

	@Override
	public Long decr(CacheKey key) {
		return null;
	}

}
