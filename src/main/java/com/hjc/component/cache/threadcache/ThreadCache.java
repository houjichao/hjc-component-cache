package com.hjc.component.cache.threadcache;

import com.hjc.component.cache.util.AsyncInvoker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;


/**
 * 提交过程使用的缓存
 * 
 * @author hjc
 */
public class ThreadCache {
	private static Logger logger = LoggerFactory.getLogger(ThreadCache.class);
	private static ThreadLocal<Map<Class<?>, Object>> cacheData = new ThreadLocal<Map<Class<?>, Object>>();// 缓存数据
	private static ThreadLocal<List<AsyncInvoker>> asyncList = new ThreadLocal<List<AsyncInvoker>>();// 用于数据库commit后的异步调用

	/**
	 * 初始化缓存
	 */
	public static void init() {
		clear(false);
		Map<Class<?>, Object> mapData = new LinkedHashMap<Class<?>, Object>();
		cacheData.set(mapData);
	}

	/**
	 * 清空
	 * 
	 * @param isSuccess
	 *            是否调用成功
	 */
	public static void clear(boolean isSuccess) {
		cacheData.set(null);
		cacheData.remove();

		if (isSuccess) {
			// 执行异步调用
			List<AsyncInvoker> list = asyncList.get();
			if (list != null && !list.isEmpty()) {
				for (AsyncInvoker async : list) {
					try {
						async.getResult();
					} catch (Throwable t) {
						// 执行异常也不允许影响返回
						logger.error("执行异步调用异常", t);
					}
				}
			}
		}

		asyncList.set(null);
		asyncList.remove();
	}

	/**
	 * 获取缓存值
	 * 
	 * @param loader
	 * @param cacheKey
	 * @return
	 */
	public static Object getCacheData(ICacheLoader loader, Object cacheKey) {
		Class<?> cls = loader.getClass();
		Map<Class<?>, Object> mapData = cacheData.get();
		if (mapData == null) {
			init();
			mapData = cacheData.get();
		}

		@SuppressWarnings("unchecked")
		Map<Object, Object> cacheObj = (Map<Object, Object>) mapData.get(cls);
		if (cacheObj == null) {
			// 初始化缓存对象
			cacheObj = new LinkedHashMap<Object, Object>();
			mapData.put(cls, cacheObj);
		} else {
			if (cacheObj.containsKey(cacheKey)) {
				return cacheObj.get(cacheKey);
			}
		}

		// 加载数据并放入缓存
		Object cacheData = loader.loadData(cacheKey);
		cacheObj.put(cacheKey, cacheData);
		return cacheData;
	}

	/**
	 * 增加异步调用
	 * 
	 * @param instance
	 * @param methodName
	 * @param params
	 */
	public static void addAsyncInvoker(Object instance, String methodName,
			Object... params) {
		List<AsyncInvoker> list = asyncList.get();
		if (list == null) {
			list = new ArrayList<AsyncInvoker>();
			asyncList.set(list);
		}
		AsyncInvoker.addAsyncInvoker(list, instance, methodName, params);
	}

}
