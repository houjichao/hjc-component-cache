package com.hjc.component.cache.redis;

import com.hjc.component.cache.CacheKey;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;


public class RefreshInvoker {
    private final Object instance;
    private final Method method;

    public RefreshInvoker(Object instance, Method method) {
        this.instance = instance;
        this.method = method;
        this.method.setAccessible(true);
    }

    public Method getMethod() {
        return method;
    }

    public Object invoke(CacheKey cacheKey, Object[] args) {
        try {
            return method.invoke(instance, args);
        } catch (IllegalAccessException e) {
            throw new RuntimeException("权限异常,cacheKey" + cacheKey.getFullKey(), e);
        } catch (IllegalArgumentException e) {
            throw new RuntimeException("参数错误,cacheKey" + cacheKey.getFullKey(), e);
        } catch (InvocationTargetException e) {
            Throwable t = e.getCause();
            if (t == null) {
                t = e;
            }
            throw new RuntimeException("缓存异常,cacheKey" + cacheKey.getFullKey(), t);
        }
    }

}
