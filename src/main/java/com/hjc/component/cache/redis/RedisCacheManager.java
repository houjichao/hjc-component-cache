package com.hjc.component.cache.redis;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.hjc.component.cache.CacheKey;
import com.hjc.component.cache.CacheResult;
import com.hjc.component.cache.CacheWrapper;
import com.hjc.component.cache.RegionInfo;
import com.hjc.component.cache.annotation.Cache;
import com.hjc.component.cache.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.util.ClassUtils;


public class RedisCacheManager extends RedisCache {
	private static final Map<String, Map<String, RefreshInvoker>> invokerCahce = new ConcurrentHashMap<String, Map<String, RefreshInvoker>>();
	private static final Logger logger = LoggerFactory.getLogger(RedisCacheManager.class);
	private static long MIN = DateUtil.str2Date("20160501000000").getTime();
	private String namespace = StringHelper.EMPTY;// 命名空间
	private int thresholdTime = 12;// 过期阀值，单位秒
	private int waitTimeout = 60;// 等待超时，单位秒

	public void setNamespace(String namespace) {
		this.namespace = namespace;
	}

	public void setThresholdTime(int thresholdTime) {
		this.thresholdTime = thresholdTime;
	}

	public void setWaitTimeout(int waitTimeout) {
		this.waitTimeout = waitTimeout;
	}

	/**
	 * 获取所有分区信息
	 *
	 * @return 分区信息
	 */
	public List<RegionInfo> listRegions() {
		List<RegionInfo> outList = new ArrayList<RegionInfo>();
		byte[] regionSetKeyBytes = regionsKeyToBytes(namespace);
		Map<byte[], byte[]> bytesMap = jedis.hgetAll(regionSetKeyBytes);
		if (bytesMap != null) {
			for (Entry<byte[], byte[]> e : bytesMap.entrySet()) {
				RegionInfo ri = new RegionInfo();
				ri.setNamespace(namespace);
				readRegionKey(e.getKey(), ri);
				readRegionValue(e.getValue(), ri);
				outList.add(ri);
			}
		}
		return outList;
	}

	/**
	 * 分页查询
	 * 
	 * @param className
	 *            类名
	 * @param methodDesc
	 *            方法签名
	 * @param pageNumber
	 *            页码
	 * @param pageSize
	 *            每页数
	 * @return 缓存KEY
	 */
	public CacheResult<CacheKey> query(String className, String methodDesc, int pageNumber, int pageSize) {
		pageNumber = Math.max(pageNumber, 1);// 页码(缺省默认第一页)
		pageSize = Math.min(pageSize, 5);// 每页数

		CacheResult<CacheKey> result = new CacheResult<CacheKey>();
		result.setTotal(0);

		byte[] regionKeyBytes = joinToBytes(namespace, className, methodDesc);
		Long total = jedis.zcard(regionKeyBytes);// 查询记录总数

		if (total != null && total > 0) {
			result.setTotal(total);// 记录总数

			long start = (pageNumber - 1) * pageSize;
			long end = pageNumber * pageSize - 1;
			final Set<byte[]> list = jedis.zrange(regionKeyBytes, start, end);
			List<CacheKey> infoList = new ArrayList<CacheKey>(list.size());
			for (byte[] fieldBytes : list) {
				final CacheKey key = CacheKey.from(fieldBytes);
				if (key != null) {
					infoList.add(key);
				}
			}
			result.setList(infoList);
		}
		return result;
	}

	public void clear(String className, String methodDesc, ExecutorService executorService, boolean waitFuture) {
		byte[] regionKeyBytes = joinToBytes(namespace, className, methodDesc);
		while (true) {
			final Set<byte[]> list = jedis.zrange(regionKeyBytes, 0, 6000);
			if (list == null || list.isEmpty()) {
				break;
			}

			List<Future<Boolean>> futureList = new LinkedList<Future<Boolean>>();
			for (byte[] fieldBytes : list) {
				final CacheKey cacheKey = CacheKey.from(fieldBytes);

				if (executorService != null) {
					Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
						@Override
						public Boolean call() throws Exception {
							del(cacheKey);
							return true;
						}
					});

					if (waitFuture) {
						futureList.add(future);
					}
				} else {
					del(cacheKey);
				}
			}

			for (final Future<Boolean> future : futureList) {
				boolean isError = false;
				try {
					future.get(waitTimeout, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					isError = true;
					logger.error("执行中断", e);
				} catch (ExecutionException e) {
					Throwable t = e.getCause();
					if (t == null) {
						t = e;
					}
					logger.error("执行异常", t);
				} catch (TimeoutException e) {
					logger.error("执行超时", e);
				} catch (Throwable t) {
					logger.error("未知异常", t);
				} finally {
					if (isError) {
						try {
							if (future != null) {
								future.cancel(true);
							}
						} catch (Throwable ignore) {
						}
					}
				}
			}
		}
	}

	public void refresh(String className, String methodDesc, ExecutorService executorService, boolean waitFuture) {
		final long max = System.currentTimeMillis() + thresholdTime * 1000;
		byte[] regionKeyBytes = joinToBytes(namespace, className, methodDesc);
		final Set<byte[]> list = jedis.zrangeByScore(regionKeyBytes, MIN, max);
		if (list == null) {
			return;
		}

		List<Future<Boolean>> futureList = new LinkedList<Future<Boolean>>();
		for (byte[] fieldBytes : list) {
			final CacheKey cacheKey = CacheKey.from(fieldBytes);

			if (executorService != null) {
				Future<Boolean> future = executorService.submit(new Callable<Boolean>() {
					@Override
					public Boolean call() throws Exception {
						return refresh(cacheKey);
					}
				});

				if (waitFuture) {
					futureList.add(future);
				}
			} else {
				refresh(cacheKey);
			}
		}

		for (final Future<Boolean> future : futureList) {
			boolean isError = false;
			try {
				future.get(waitTimeout, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				isError = true;
				logger.error("执行中断", e);
			} catch (ExecutionException e) {
				Throwable t = e.getCause();
				if (t == null) {
					t = e;
				}
				logger.error("执行异常", t);
			} catch (TimeoutException e) {
				logger.error("执行超时", e);
			} catch (Throwable t) {
				logger.error("未知异常", t);
			} finally {
				if (isError) {
					try {
						if (future != null) {
							future.cancel(true);
						}
					} catch (Throwable ignore) {
					}
				}
			}
		}
	}

	/**
	 * 刷新一个key
	 * 
	 * @param cacheKey
	 *            KEY
	 */
	public boolean refresh(CacheKey cacheKey) {
		byte[] keyArgsBytes = toKeyArgsBytes(cacheKey);

		// 根据KEY获取类方法和请求参数
		byte[] fullValueBytes = jedis.get(keyArgsBytes);
		Object[] args = Serialize.fstdeserialize(fullValueBytes);
        if (args == null || args.length < 6) {
            removeFromRegion(cacheKey);
            return false;
        }

		int expire = (int) args[0];
		boolean refresh = (boolean) args[1];
		String neanName = (String) args[2];
		String className = (String) args[3];
		String methodDesc = (String) args[4];
		Object[] arguments = (Object[]) args[5];

		RefreshInvoker refreshInvoker = getInvoker(neanName, className, methodDesc);
		if (refreshInvoker == null) {
			return false;// 找不到invoker
		}

		System.out.println(cacheKey.getFullKey() + " refreshed!!!");
		Object cacheObject = refreshInvoker.invoke(cacheKey, arguments);
		CacheWrapper cacheWrapper = new CacheWrapper(cacheObject, expire);

		Method method = refreshInvoker.getMethod();
		String methodName = method.getName();
		try {
			// 保存到缓存
			put(cacheKey, cacheWrapper);
		} catch (Throwable t) {
			// 写缓存异常处理
			logger.error("写缓存异常，" + className + "." + methodName + ", cacheKey:" + cacheKey.getFullKey(), t);
			return false;
		}

		try {
			// 获取实际存储的region
			addToRegion(className, method, arguments, expire, refresh, neanName, cacheKey,
					cacheWrapper.getLastLoadTime());
		} catch (Throwable t) {
			// 写缓存异常处理
			logger.error("写缓存异常，" + className + "." + methodName + ", cacheKey:" + cacheKey.getFullKey(), t);
			return false;
		}

		return true;
	}

	private static Class<?> resolveClassName(String clazzName) {
		try {
			return ClassUtils.resolveClassName(clazzName, ClassUtils.getDefaultClassLoader());
		} catch (Throwable ignore) {
		}
		return null;
	}

	protected static RefreshInvoker getInvoker(String beanName, String className, String methodSignature) {
		// 优先根据beanName定位
		if (StringHelper.isNotEmpty(beanName)) {
			Map<String, RefreshInvoker> invokers = invokerCahce.get(beanName);
			if (invokers == null) {
				invokers = new HashMap<String, RefreshInvoker>();// 留个空防止重复加载
				invokerCahce.put(beanName, invokers);
			}
			RefreshInvoker invoker = null;
			if (!invokers.containsKey(methodSignature)) {
				try {
					Object instance = null;
					Class<?> clazz = null;
					if (beanName.startsWith("com.eshore.")) {
						clazz = resolveClassName(beanName);
					}
					if (clazz == null) {
						instance = SpringContextUtil.getApplicationContext().getBean(beanName);
					} else {
						instance = SpringContextUtil.getBean(clazz);
					}
					if (instance != null) {
						if (clazz == null) {
							clazz = instance.getClass();
						}
						invoker = loadInvoker(invokers, clazz, instance, methodSignature);
					}
				} catch (Throwable t) {
					logger.error("[" + beanName + "]找不到", t);
				}
				invokers.put(methodSignature, invoker);
			}

			invoker = invokers.get(methodSignature);
			if (invoker != null) {
				return invoker;// 找到
			}
		}

		// 根据类名定位
		Map<String, RefreshInvoker> invokers = invokerCahce.get(className);
		if (invokers == null) {
			try {
				Class<?> clazz = resolveClassName(className);
				if (clazz != null) {
					Object instance = SpringContextUtil.getBean(clazz);
					if (instance != null) {
						invokers = loadInvokers(clazz, instance);
					}
				}
			} catch (Throwable t) {
				logger.error("[" + className + "]找不到", t);
			}
			if (invokers == null) {
				invokers = new HashMap<String, RefreshInvoker>();
			}
			invokerCahce.put(className, invokers);
		}

		return invokers.get(methodSignature);
	}

	private static RefreshInvoker loadInvoker(Map<String, RefreshInvoker> invokers, Class<?> clazz, Object instance,
			String methodSignature) {
		Method[] methods = clazz.getMethods();
		if (methods != null) {
			for (Method method : methods) {
				if (!Modifier.isPublic(method.getModifiers())) {
					continue;// 不是public方法
				}

				String mSignature = ReflectUtils.getDesc(method);
				if (methodSignature.equals(mSignature)) {// 找到匹配方法
					RefreshInvoker invoker = new RefreshInvoker(instance, method);
					invokers.put(mSignature, invoker);
					return invoker;// 找到就返回
				}

				if (method.isAnnotationPresent(Cache.class)) {// Cacheable注解，也顺便注册
					RefreshInvoker invoker = new RefreshInvoker(instance, method);
					invokers.put(mSignature, invoker);
				}
			}
		}

		return null;
	}

	private static Map<String, RefreshInvoker> loadInvokers(Class<?> clazz, Object instance) {
		Map<String, RefreshInvoker> invokers = new HashMap<String, RefreshInvoker>();
		Method[] methods = clazz.getDeclaredMethods();
		if (methods != null) {
			for (Method method : methods) {
				if (!Modifier.isPublic(method.getModifiers())) {
					continue;// 不是public方法
				}
				if (!method.isAnnotationPresent(Cache.class)) {
					continue;// 没有Cacheable注解
				}

				String mSignature = ReflectUtils.getDesc(method);
				RefreshInvoker invoker = new RefreshInvoker(instance, method);
				invokers.put(mSignature, invoker);
			}
		}
		return invokers;
	}
}
