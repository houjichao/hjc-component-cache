package com.hjc.component.cache.util;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.List;

import org.apache.commons.lang.math.NumberUtils;
import org.springframework.util.ReflectionUtils;


/**
 * 接口异步返回的结果调用
 * 
 * @author hjc
 */
public class AsyncInvoker {
    protected Object   instance;// 调用的实例
    protected Method   method;  // java方法
    protected Object[] params;  // 异步调用参数
    
    /**
     * 增加异步处理
     * 
     * @param list
     * @param methodName
     * @param params
     */
    public static void addAsyncInvoker(List<AsyncInvoker> list, Object instance, String methodName,
        Object... params) {
        try {
            Class<?> cls = instance.getClass();
            Method method = ReflectionUtils.findMethod(cls, methodName,
                    new Class<?>[]{Object[].class});
            if (method == null) {
                String errMsg = "异步结果处理方法不存在";
                throw new RuntimeException("errMsg" + errMsg);
            }

            method.setAccessible(true);
            list.add(new AsyncInvoker(instance, method, params));
        } catch (SecurityException e) {
            String errMsg = "异步结果处理方法不可见";
            throw new RuntimeException("errMsg" + errMsg);

        }
    }
    
    public String getMethodName() {
        return method.getName();
    }
    
    /**
     * @param method
     * @param instance
     */
    private AsyncInvoker(Object instance, Method method, Object[] params) {
        this.instance = instance;
        this.method = method;
        this.params = params;
    }
    
    /**
     * 执行方法
     * 
     * @param args
     *            参数值列表
     * @return
     * @throws NoSuchMethodException
     * @throws IllegalAccessException
     * @throws InvocationTargetException
     */
    private Object invoke(Object[] args)
        throws NoSuchMethodException, IllegalAccessException, InvocationTargetException {
        return method.invoke(instance, convertParamType(args));
    }
    
    /**
     * 尝试强制类型转换
     * 
     * @param args
     * @return
     */
    private Object[] convertParamType(Object[] args) {
        Class<?>[] types = method.getParameterTypes();
        int typeLen = types == null ? 0
            : types.length;
        int argLen = args == null ? 0
            : args.length;
        if (typeLen != argLen) {// 抛异常
            String errMsg = "方法\"" + method.getName() + "\"参数个数不匹配";
            throw new RuntimeException("errMsg" + errMsg);

        }
        if (argLen <= 1) {
            return args;
        }
        
        for (int i = 0; i < argLen; ++i) {
            Class<?> needType = types[i];
            args[i] = convertValue(args[i], needType, i);
        }
        
        return args;
    }
    
    /**
     * 获取空值
     * 
     * @param needType
     * @return
     */
    private Object getNullValue(Class<?> needType) {
        // 字符串
        if (String.class == needType) {
            return "";
        }
        
        // long类型
        if (Long.class == needType || long.class == needType) {
            return 0L;
        }
        
        // int类型
        if (Integer.class == needType || int.class == needType) {
            return Integer.valueOf(0);
        }
        
        // boolean类型
        if (Boolean.class == needType || boolean.class == needType) {
            return false;
        }
        
        return null;
    }
    
    /**
     * 根据需要的类型进行类型转换
     * 
     * @param arg
     * @param needType
     * @return
     */
    private Object convertValue(Object arg, Class<?> needType, int pos) {
        if (arg == null) {
            return getNullValue(needType);
        }
        
        Class<?> cls = arg.getClass();
        if (cls == needType || needType.isAssignableFrom(cls)) {// 类型匹配
            return arg;
        }
        
        if (Long.class == needType || long.class == needType) {
            if (Boolean.class == cls || boolean.class == cls) {
                return (Boolean) arg ? 1L
                    : 0L;
            } else {
                return NumberUtils.toLong(arg.toString());
            }
        }
        
        if (Integer.class == needType || int.class == needType) {
            if (Boolean.class == cls || boolean.class == cls) {
                return (Boolean) arg ? 1
                    : 0;
            } else {
                return NumberUtils.toInt(arg.toString());
            }
        }
        
        if (String.class == needType) {
            if (Number.class.isAssignableFrom(cls)) {
                return arg.toString();
            }
        }
        
        String errMsg = "方法\"" + method.getName() + "\"参数" + (pos + 1) + "类型不匹配";
        throw new RuntimeException("errMsg" + errMsg);
    }
    
    /**
     * 获取异步处理结果
     */
    public Object getResult() {
        try {
            return invoke(new Object[] {params });
        } catch (NoSuchMethodException e) {
            String errMsg = "异步结果处理方法不存在！" + method.getName();
            throw new RuntimeException("errMsg" + errMsg);
        } catch (IllegalAccessException e) {
            String errMsg = "异步结果处理方法不可访问！" + method.getName();
            throw new RuntimeException("errMsg" + errMsg);
        } catch (InvocationTargetException e) {
            Throwable t = e.getTargetException();
            if (t != null) {
                if (t instanceof RuntimeException) {
                    throw (RuntimeException) t;
                }
            } else {
                t = e;
            }
            
            String errMsg = "调用异步结果处理方法失败！" + method.getName();
            throw new RuntimeException("errMsg" + errMsg);
        } catch (Throwable t) {// 系统异常
            String errMsg = "无效的异步结果处理方法！" + method.getName();
            throw new RuntimeException("errMsg" + errMsg);
        }
    }
    
}
