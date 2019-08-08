package com.hjc.component.cache.aop;

import java.lang.reflect.Method;

import com.hjc.component.cache.CacheFactory;
import com.hjc.component.cache.CacheKey;
import com.hjc.component.cache.CacheWrapper;
import com.hjc.component.cache.ICache;
import com.hjc.component.cache.annotation.Cache;
import com.hjc.component.cache.annotation.CacheDelete;
import com.hjc.component.cache.redis.RedisCache;
import com.hjc.component.cache.util.CacheUtil;
import com.hjc.component.cache.util.StringHelper;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.Signature;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 使用Aspectj 实现AOP挂载 注意：拦截器不能有相同名字的Method
 * 
 * @author hjc
 *
 */
public class AspectjAopInterceptor {
    private static final Logger logger       = LoggerFactory
        .getLogger(AspectjAopInterceptor.class);
    private int                  timeout      = 10;                 // 加载超时阀值,单位：秒
    private String               namespace    = ""; // 命名空间
    private int                  cacheTimeout = 50;                 // 缓存超时阀值,单位：毫秒
    
    public void setTimeout(int timeout) {
        this.timeout = timeout * 1000;
    }
    
    public void setNamespace(String namespace) {
        this.namespace = namespace;
    }
    
    public void setCacheTimeout(int cacheTimeout) {
        this.cacheTimeout = cacheTimeout;
    }
    
    /**
     * 检查切面含有Cache注解则进行缓存
     * 
     * @param pjp
     *            切面处理
     * @return 缓存对象
     * @throws Throwable
     *             异常
     */
    public Object checkAndCache(ProceedingJoinPoint pjp) throws Throwable {
        Signature signature = pjp.getSignature();
        MethodSignature methodSignature = (MethodSignature) signature;
        Method method = methodSignature.getMethod();
        
        if (!method.isAnnotationPresent(Cache.class)) {
            return loadData(pjp, method);// 没有注解
        }
        
        Cache cacheable = method.getAnnotation(Cache.class);
        if (!CacheUtil.isCacheable(cacheable, pjp.getArgs())) {
            return loadData(pjp, method);// 不启用缓存
        }
        
        // 从缓存中读取
        String className = pjp.getTarget().getClass().getName();
        String methodName = method.getName();
        Object[] arguments = pjp.getArgs();
        String _key = cacheable.key();
        String _hfield = cacheable.hfield();
        CacheKey cacheKey = getCacheKey(className, methodName, arguments, _key, _hfield, null,
            false);
        if (cacheKey == null) {
            return loadData(pjp, method);// 没办法获得key
        }
        
        boolean loadCacheFail = false;
        CacheWrapper cacheWrapper = null;
        ICache cache = CacheFactory.getCache(cacheable.type(), cacheable.region());
        long st = System.currentTimeMillis();
        try {
            cacheWrapper = cache.get(cacheKey);
        } catch (Throwable t) {
            logger.error(
                "读缓存异常，" + className + "." + methodName + ", cacheKey:" + cacheKey.getFullKey(), t);
            logger.warn("读缓存异常 method" + method.toString());
            cacheWrapper = null;
            loadCacheFail = true;
        } finally {
            long cst = System.currentTimeMillis() - st;
            if (cst > cacheTimeout) {
                logger.warn(
                    "读缓存耗时 get cache cst = " + cst + " ms  , cacheKey:" + cacheKey.getFullKey());
                logger.warn("读缓存耗时 method" + method.toString());
            }
        }
        
        if (null != cacheWrapper) {
            return cacheWrapper.getCacheObject();// 从缓存获得
        }
        
        // 调用原方法加载
        Object cacheObject = loadData(pjp, method);
        if (cacheable.isCache() == false) {
            // 不保存缓存直接返回（由method内缓存）
            return cacheObject;
        }
        
        if (loadCacheFail == false) {//从缓存加载没出错才进行存储到缓存
            st = System.currentTimeMillis();
            try {
                // 保存到缓存
                cacheWrapper = new CacheWrapper(cacheObject, cacheable.expire());
                cache.put(cacheKey, cacheWrapper);
            } catch (Throwable t) {
                // 写缓存异常处理
                logger.error(
                    "写缓存异常，" + className + "." + methodName + ", cacheKey:" + cacheKey.getFullKey(),
                    t);
                logger.error("写缓存异常 method" + method.toString());
            } finally {
                long cst = System.currentTimeMillis() - st;
                if (cst > cacheTimeout) {
                    logger.warn("写缓存耗时 put cache cst = " + cst + " ms  , cacheKey:"
                        + cacheKey.getFullKey());
                    logger.warn("写缓存耗时 method" + method.toString());
                }
            }
            
            st = System.currentTimeMillis();
            try {
                if (cacheable.expire() > 0 && cache instanceof RedisCache) {
                    // 保存扩展缓存redis管理信息
                    RedisCache redisCache = (RedisCache) cache;
                    // 获取实际存储的region
                    redisCache.addToRegion(className, method, arguments, cacheable.expire(),
                        cacheable.refresh(), cacheable.refreshBeanName(), cacheKey,
                        cacheWrapper.getLastLoadTime());
                }
            } catch (Throwable t) {
                // 写缓存异常处理
                logger.error(
                    "写缓存异常，" + className + "." + methodName + ", cacheKey:" + cacheKey.getFullKey(),
                    t);
                logger.error("写缓存耗时 method" + method.toString());
            } finally {
                long cst = System.currentTimeMillis() - st;
                if (cst > cacheTimeout) {
                    logger.warn("写缓存耗时 addToRegion cst = " + cst + " ms  , cacheKey:"
                        + cacheKey.getFullKey());
                    logger.warn("写缓存耗时 method" + method.toString());
                }
            }
        }
        return cacheObject;
    }
    
    /**
     * 检查切面含有CacheDelete注解则进行缓存删除
     * 
     * @param jp
     *            切面处理
     * @return 缓存对象
     * @throws Throwable
     *             异常
     */
    public void checkAndDeleteCache(JoinPoint jp, Object retVal) {
        Signature signature = jp.getSignature();
        MethodSignature methodSignature = (MethodSignature) signature;
        Method method = methodSignature.getMethod();
        // 删除单个主键KEY
        if (method.isAnnotationPresent(CacheDelete.class)) {
            Object[] arguments = jp.getArgs();
            CacheDelete cacheDelete = method.getAnnotation(CacheDelete.class);
            deleteCache(jp, method, cacheDelete, arguments, retVal);
        }
    }
    
    public void deleteCache(JoinPoint jp, CacheDelete cacheDelete, Object retVal) {
        Signature signature = jp.getSignature();
        MethodSignature methodSignature = (MethodSignature) signature;
        Method method = methodSignature.getMethod();
        Object[] arguments = jp.getArgs();
        deleteCache(jp, method, cacheDelete, arguments, retVal);
    }
    
    private void deleteCache(JoinPoint jp, Method method, CacheDelete cacheDelete,
        Object[] arguments, Object retVal) {
        if (CacheUtil.isCanDelete(cacheDelete, arguments, retVal)) {
            String className = jp.getTarget().getClass().getName();
            String methodName = method.getName();
            String _key = cacheDelete.key();
            String _hfield = cacheDelete.hfield();
            CacheKey cacheKey = getCacheKey(className, methodName, arguments, _key, _hfield, retVal,
                true);
            if (null != cacheKey) {
                long st = System.currentTimeMillis();
                try {
                    ICache cache = CacheFactory.getCache(cacheDelete.type(), cacheDelete.region());
                    cache.del(cacheKey);
                } catch (Throwable t) {
                    // 删除缓存异常处理
                    logger.error("删除缓存异常，" + className + "." + methodName + ", cacheKey:"
                        + cacheKey.getFullKey(), t);
                    logger.error("删除缓存异常 method" + method.toString());
                } finally {
                    long cst = System.currentTimeMillis() - st;
                    if (cst > cacheTimeout) {
                        logger.warn("删除缓存耗时 addToRegion cst = " + cst + " ms  , cacheKey:"
                            + cacheKey.getFullKey());
                        logger.warn("删除缓存耗时 method" + method.toString());
                    }
                }
            }
        }
    }
    
    /**
     * 直接加载数据（加载后的数据不往缓存放）
     * 
     * @param pjp
     *            切面处理
     * @return 缓存数据
     * @throws Throwable
     *             异常
     */
    private Object loadData(ProceedingJoinPoint pjp, Method method) throws Throwable {
        long startTime = System.currentTimeMillis();
        try {
            return pjp.proceed();
        } finally {
            long useTime = System.currentTimeMillis() - startTime;
            if (useTime > timeout) {
                String className = pjp.getTarget().getClass().getName();
                logger.error(className + "." + method.getName() + ", use time:" + useTime + "ms");
            }
        }
    }
    
    /**
     * 生成缓存Key
     * 
     * @param className
     *            类名
     * @param methodName
     *            方法名
     * @param arguments
     *            参数
     * @param _key
     *            key
     * @param _hfield
     *            field
     * @param result
     *            执行实际方法的返回值
     * @return CacheKey
     */
    private CacheKey getCacheKey(String className, String methodName, Object[] arguments,
        String _key, String _hfield, Object result, boolean hasRetVal) {
        String key = null;
        if (StringHelper.isNotEmpty(_key)) {
            key = CacheUtil.getDefinedCacheKey(_key, arguments, result, hasRetVal);
        } else {
            key = CacheUtil.getDefaultCacheKey(className, methodName, arguments);
        }
        if (StringHelper.isEmpty(key)) {
            return null;
        }
        
        String hfield = StringHelper.EMPTY;
        if (StringHelper.isNotEmpty(_hfield)) {
            hfield = CacheUtil.getDefinedCacheKey(_hfield, arguments, result, hasRetVal);
        }
        
        CacheKey cacheKey = new CacheKey();
        cacheKey.setNamespace(namespace);
        cacheKey.setKey(key);
        cacheKey.setHfield(hfield);
        return cacheKey;
    }
    
}
