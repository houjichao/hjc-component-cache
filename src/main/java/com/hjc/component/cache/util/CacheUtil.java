package com.hjc.component.cache.util;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.zip.Deflater;
import java.util.zip.Inflater;

import com.hjc.component.cache.annotation.Cache;
import com.hjc.component.cache.annotation.CacheDelete;
import org.springframework.expression.Expression;
import org.springframework.expression.ExpressionParser;
import org.springframework.expression.spel.standard.SpelExpressionParser;
import org.springframework.expression.spel.support.StandardEvaluationContext;


/**
 * 缓存工具类
 * 
 * @author hjc
 *
 */
public class CacheUtil {
    private static final String                  SPLIT_STR    = "_";
    private static final String                  ARGS         = "args";
    private static final String                  RET_VAL      = "retVal";
    private static final ExpressionParser        parser       = new SpelExpressionParser();
    private static final Map<String, Expression> expCache     = new ConcurrentHashMap<String, Expression>(
        64);
    private static final Map<String, Method>     funcs        = new ConcurrentHashMap<String, Method>(
        64);
    private static final String                  HASH_FUNC    = "hash";
    private static final String                  EMPTH_FUNC   = "empty";
    private static Method                        HASH_METHOD  = null;
    private static Method                        EMPTY_METHOD = null;
    
    static {
        try {
            HASH_METHOD = CacheUtil.class.getDeclaredMethod("getUniqueHashStr",
                new Class[] {Object.class });
            EMPTY_METHOD = CacheUtil.class.getDeclaredMethod("isEmpty",
                new Class[] {Object.class });
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    /**
     * 是否可以缓存
     * 
     * @param cache
     *            Cache
     * @param arguments
     *            参数
     * @return cacheAble 是否可以进行缓存
     */
    public static boolean isCacheable(Cache cache, Object[] arguments) {
        if (null != cache.condition() && cache.condition().length() > 0) {
            // 根据条件配置进行判断
            return getElValue(cache.condition(), arguments, Boolean.class);
        }
        return true;
    }
    
    /**
     * 注册SpEl方法
     * 
     * @param name
     *            方法名
     * @param method
     *            方法
     */
    public static void addFunction(String name, Method method) {
        funcs.put(name, method);
    }
    
    /**
     * 将Spring EL 表达式转换期望的值
     * 
     * @param keySpEL
     *            生成缓存Key的Spring el表达式
     * @param arguments
     *            参数
     * @param valueType
     *            值类型
     * @return T Value 返回值
     * @param <T>
     *            泛型
     */
    public static <T> T getElValue(String keySpEL, Object[] arguments, Class<T> valueType) {
        return getElValue(keySpEL, arguments, null, false, valueType);
    }
    
    /**
     * 将Spring EL 表达式转换期望的值
     * 
     * @param keySpEL
     *            生成缓存Key的Spring el表达式
     * @param arguments
     *            参数
     * @param retVal
     *            结果值
     * @param hasRetVal
     *            retVal 参数
     * @param valueType
     *            值类型
     * @return T value 返回值
     * @param <T>
     *            泛型
     */
    public static <T> T getElValue(String keySpEL, Object[] arguments, Object retVal,
        boolean hasRetVal, Class<T> valueType) {
        StandardEvaluationContext context = new StandardEvaluationContext();
        
        // 注册方法
        if (HASH_METHOD != null) {
            context.registerFunction(HASH_FUNC, HASH_METHOD);
        }
        if (EMPTY_METHOD != null) {
            context.registerFunction(EMPTH_FUNC, EMPTY_METHOD);
        }
        Iterator<Map.Entry<String, Method>> it = funcs.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry<String, Method> entry = it.next();
            context.registerFunction(entry.getKey(), entry.getValue());
        }
        
        context.setVariable(ARGS, arguments);
        if (hasRetVal) {
            context.setVariable(RET_VAL, retVal);
        }
        
        Expression expression = expCache.get(keySpEL);
        if (null == expression) {
            expression = parser.parseExpression(keySpEL);
            expCache.put(keySpEL, expression);
        }
        
        return expression.getValue(context, valueType);
    }
    
    /**
     * 判断对象是否为空
     * 
     * @param obj
     *            对象
     * @return boolean
     */
    public static boolean isEmpty(Object obj) {
        if (null == obj) {
            return true;
        }
        
        if (obj instanceof String) {
            return ((String) obj).length() <= 0;
        }
        
        Class<?> cl = obj.getClass();
        if (cl.isArray()) {
            int len = Array.getLength(obj);
            return len <= 0;
        }
        
        if (obj instanceof Collection) {
            Collection<?> tempCol = (Collection<?>) obj;
            return tempCol.isEmpty();
        }
        
        if (obj instanceof Map) {
            Map<?, ?> tempMap = (Map<?, ?>) obj;
            return tempMap.isEmpty();
        }
        
        return false;
    }
    
    /**
     * 将Object 对象转换为唯一的Hash字符串
     * 
     * @param obj
     *            Object
     * @return Hash字符串
     */
    public static String getUniqueHashStr(Object obj) {
        return getMiscHashCode(BeanUtil.toString(obj));
    }
    
    /**
     * 通过混合Hash算法，将长字符串转为短字符串（字符串长度小于等于20时，不做处理）
     * 
     * @param str
     *            String
     * @return Hash字符串
     */
    public static String getMiscHashCode(String str) {
        if (null == str || str.length() == 0) {
            return StringHelper.EMPTY;
        }
        
        if (str.length() <= 20) {
            return str;
        }
        
        StringBuilder tmp = new StringBuilder();
        tmp.append(str.hashCode()).append(SPLIT_STR).append(getHashCode(str));
        int mid = str.length() / 2;
        String str1 = str.substring(0, mid);
        String str2 = str.substring(mid);
        tmp.append(SPLIT_STR).append(str1.hashCode());
        tmp.append(SPLIT_STR).append(str2.hashCode());
        return tmp.toString();
    }
    
    /**
     * 生成字符串的HashCode
     * 
     * @param buf
     *            buf
     * @return int hashCode
     */
    private static int getHashCode(String buf) {
        int hash = 5381;
        int len = buf.length();
        
        while (len-- > 0) {
            /* hash * 33 + c */
            hash = ((hash << 5) + hash) + buf.charAt(len);
        }
        return hash;
    }
    
    /**
     * 根据请求参数和执行结果值，进行构造缓存Key
     * 
     * @param keySpEL
     *            生成缓存Key的Spring el表达式
     * @param arguments
     *            参数
     * @param retVal
     *            结果值
     * @param hasRetVal
     *            是否有retVal
     * @return CacheKey 缓存Key
     */
    public static String getDefinedCacheKey(String keySpEL, Object[] arguments, Object retVal,
        boolean hasRetVal) {
        if (keySpEL.indexOf("#") == -1 && keySpEL.indexOf("'") == -1) {
            return keySpEL;
        }
        return getElValue(keySpEL, arguments, retVal, hasRetVal, String.class);
    }
    
    /**
     * 生成缓存Key
     * 
     * @param className
     *            类名称
     * @param method
     *            方法名称
     * @param arguments
     *            参数
     * @return CacheKey 缓存Key
     */
    public static String getDefaultCacheKey(String className, String method, Object[] arguments) {
        StringBuilder sb = new StringBuilder();
        sb.append(getDefaultCacheKeyPrefix(className, method, arguments));
        if (null != arguments && arguments.length > 0) {
            sb.append(getUniqueHashStr(arguments));
        }
        return sb.toString();
    }
    
    /**
     * 生成缓存Key的前缀
     * 
     * @param className
     *            类名称
     * @param method
     *            方法名称
     * @param arguments
     *            参数
     * @return CacheKey 缓存Key
     */
    public static String getDefaultCacheKeyPrefix(String className, String method,
        Object[] arguments) {
        StringBuilder sb = new StringBuilder();
        sb.append(className);
        if (null != method && method.length() > 0) {
            sb.append(".").append(method);
        }
        return sb.toString();
    }
    
    /**
     * 是否可以删除缓存
     * 
     * @param cacheDelete
     *            CacheDelete注解
     * @param arguments
     *            参数
     * @param retVal
     *            结果值
     * @return Can Delete
     */
    public static boolean isCanDelete(CacheDelete cacheDelete, Object[] arguments, Object retVal) {
        boolean rv = true;
        if (null != arguments && arguments.length > 0 && null != cacheDelete.condition()
            && cacheDelete.condition().length() > 0) {
            rv = getElValue(cacheDelete.condition(), arguments, retVal, true, Boolean.class);
        }
        return rv;
    }
    
    /**
     * 将int数值转换为占四个字节的byte数组，本方法适用于(低位在前，高位在后)的顺序。
     * 
     * @param value
     *            要转换的int值
     */
    public static void intToBytes(byte[] ary, int offset, int value) {
        ary[offset + 3] = (byte) ((value & 0xFF000000) >> 24);
        ary[offset + 2] = (byte) ((value & 0x00FF0000) >> 16);
        ary[offset + 1] = (byte) ((value & 0x0000FF00) >> 8);
        ary[offset + 0] = (byte) ((value & 0x000000FF));
    }
    
    /**
     * byte数组中取int数值，本方法适用于(低位在前，高位在后)的顺序。
     * 
     * @param ary
     *            byte数组
     * @param offset
     *            从数组的第offset位开始
     * @return int数值
     */
    public static int bytesToInt(byte[] ary, int offset) {
        int value;
        value = (int) ((ary[offset] & 0xFF) //
            | ((ary[offset + 1] << 8) & 0xFF00) //
            | ((ary[offset + 2] << 16) & 0xFF0000) //
            | ((ary[offset + 3] << 24) & 0xFF000000));
        return value;
    }
    
    /**
    * 
    * @param inputByte
    *            待解压缩的字节数组
    * @return 解压缩后的字节数组
    * @throws IOException
    */
    public static byte[] uncompress(byte[] inputByte, int offset, int length) throws IOException {
        int len = 0;
        Inflater infl = new Inflater();
        infl.setInput(inputByte, offset, length);
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] outByte = new byte[1024];
        try {
            while (!infl.finished()) {
                // 解压缩并将解压缩后的内容输出到字节输出流bos中
                len = infl.inflate(outByte);
                if (len == 0) {
                    break;
                }
                bos.write(outByte, 0, len);
            }
            infl.end();
        } catch (Exception e) {
            //
        } finally {
            bos.close();
        }
        return bos.toByteArray();
    }
    
    /**
     * 压缩.
     * 
     * @param inputByte
     *            待压缩的字节数组
     * @return 压缩后的数据
     * @throws IOException
     */
    public static byte[] compress(byte[] inputByte) throws IOException {
        int len = 0;
        Deflater defl = new Deflater();
        defl.setInput(inputByte);
        defl.finish();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        byte[] outputByte = new byte[1024];
        try {
            while (!defl.finished()) {
                // 压缩并将压缩后的内容输出到字节输出流bos中
                len = defl.deflate(outputByte);
                bos.write(outputByte, 0, len);
            }
            defl.end();
        } finally {
            bos.close();
        }
        return bos.toByteArray();
    }
    
}
