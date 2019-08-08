package com.hjc.component.cache.redis;

import com.hjc.component.cache.CacheKey;
import com.hjc.component.cache.CacheWrapper;
import com.hjc.component.cache.ICache;
import com.hjc.component.cache.RegionInfo;
import com.hjc.component.cache.util.CacheUtil;
import com.hjc.component.cache.util.ReflectUtils;
import com.hjc.component.cache.util.Serialize;
import com.hjc.component.cache.util.StringHelper;
import lombok.extern.slf4j.Slf4j;
import org.nustaq.serialization.FSTObjectInput;
import redis.clients.util.SafeEncoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Method;

@Slf4j
public class RedisCache implements ICache {
    public static final String   DEFAULT_REDIS   = "redis.props";
    private static final String  REGIONS_KEY     = "REGIONS";
    private static final byte[]  REGIONS_BYTES   = stringToBytes(REGIONS_KEY);
    private static final int     COMPRESS_LENGTH = 1024;                      //1K
    protected final IBinaryJedis jedis;
    
    public RedisCache() {
        jedis = BinaryJedisFactory.jedis(DEFAULT_REDIS);
    }
    
    public RedisCache(String redisCfg) {
        jedis = BinaryJedisFactory.jedis(redisCfg);
    }
    
    protected static byte[] stringToBytes(String str) {
        return SafeEncoder.encode(str);
    }
    
    protected static String bytesToString(byte[] data) {
        return SafeEncoder.encode(data);
    }
    
    protected CacheWrapper bytesToWrapper(byte[] data) {
        if (data == null || data.length <= 1) {
            return null;
        }
        
        try {
            byte flag = data[0];
            if (flag == 1) {//先解压，后反序列化
                byte[] uncompressBytes = CacheUtil.uncompress(data, 1, data.length - 1);
                return Serialize.fstdeserialize(uncompressBytes);
            } else {//没有压缩
                InputStream is = null;
                FSTObjectInput in = null;
                try {
                    is = new ByteArrayInputStream(data, 1, data.length);
                    in = new FSTObjectInput(is);
                    return (CacheWrapper) in.readObject();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException e) {
                        }
                    }
                    if (is != null) {
                        try {
                            is.close();
                        } catch (IOException e) {
                        }
                    }
                }
            }
        } catch (Throwable ignore) {
            //很可能缓存对象版本变化导致反序列化失败
            //返回null，使重新装载
            return null;
        }
    }
    
    protected byte[] wrapperToBytes(CacheWrapper wrapper) {
    	if(wrapper == null || wrapper.getCacheObject() == null){
    		//Logger.getRootLogger().error(key.getCacheKey());
    	}
        byte[] wraperBytes = Serialize.fstserialize(wrapper);
        if (wraperBytes == null || wraperBytes.length <= 0) {
        	log.error("redis wraperBytes is null: "+wrapper.toString());
            return wraperBytes;
        }
        
        ByteArrayOutputStream out = null;
        try {
            byte flag = 0;
            byte[] data = wraperBytes;
            if (wraperBytes.length > COMPRESS_LENGTH) {//大于COMPRESS_LENGTH,则进行压缩
                flag = (byte) 1;
                data = CacheUtil.compress(wraperBytes);
            }
            //组包
            out = new ByteArrayOutputStream();
            out.write(flag);
            out.write(data);
            return out.toByteArray();
        } catch (Throwable ignore) {
            return null;
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (IOException ignore) {
                }
            }
        }
    }
    
    protected byte[] regionsKeyToBytes(String namespace) {
        if (StringHelper.isNotEmpty(namespace)) {
            return stringToBytes(namespace + ":" + REGIONS_KEY);
        }
        return REGIONS_BYTES;
    }
    
    protected byte[] joinToBytes(String namespace, String... args) {
        StringBuilder str = new StringBuilder();
        if (StringHelper.isNotEmpty(namespace)) {
            str.append(namespace);
        }
        for (String s : args) {
            str.append(":").append(s);
        }
        return stringToBytes(str.toString());
    }
    
    protected byte[] joinToBytesRedis(String namespace, String... args) {
        StringBuilder str = new StringBuilder();
        if (StringHelper.isNotEmpty(namespace)) {
            str.append(namespace);
        }
        for (String s : args) {
            str.append(":").append(s);
        }
        return stringToBytes(str.toString());
    }
    
    @Override
    public CacheWrapper get(CacheKey key) {
        return _get(key, true);
    }
    
    protected CacheWrapper _get(CacheKey key, boolean delExpired) {
        byte[] keyBytes = stringToBytes(key.getCacheKey());
        byte[] wrapperBytes = null;
        byte[] hfeildBytes = null;
        boolean ishash = false;
        
        if (StringHelper.isNotEmpty(key.getHfield())) {
            ishash = true;
            hfeildBytes = stringToBytes(key.getHfield());
            wrapperBytes = jedis.hget(keyBytes, hfeildBytes);
        } else {
            wrapperBytes = jedis.get(keyBytes);
        }
        
        CacheWrapper wrapper = bytesToWrapper(wrapperBytes);
        if (delExpired && ishash && wrapper != null && wrapper.isExpired() && hfeildBytes != null) {
            // 删除过期的
            jedis.hdel(keyBytes, hfeildBytes);
            wrapper = null;
        }
        
        return wrapper;
    }
    
    @Override
    public void put(CacheKey key, CacheWrapper wrapper) {
    	if(wrapper == null || wrapper.getCacheObject() == null){
    		log.error("redis wrapper is null ,this key is " + key.getCacheKey() + ", wrapper is "+wrapper.toString());
    	}
        byte[] keyBytes = stringToBytes(key.getCacheKey());
        byte[] wrapperBytes = wrapperToBytes(wrapper);
        
        if (StringHelper.isNotEmpty(key.getHfield())) {
            // 不支持设置缓存时间
            jedis.hset(keyBytes, stringToBytes(key.getHfield()), wrapperBytes);
        } else {
            if (wrapper.getExpire() > 0) {
                jedis.setex(keyBytes, wrapper.getExpire(), wrapperBytes);
            } else {
                jedis.set(keyBytes, wrapperBytes);
            }
        }
    }
    
    @Override
    public void del(CacheKey key) {
        byte[] keyBytes = stringToBytes(key.getCacheKey());
        if (StringHelper.isNotEmpty(key.getHfield())) {
            jedis.hdel(keyBytes, stringToBytes(key.getHfield()));
        } else {
            jedis.del(keyBytes);
        }
        removeFromRegion(key);
    }
    
    protected byte[] toKeyArgsBytes(CacheKey cacheKey) {
        StringBuilder b = new StringBuilder();
        if (StringHelper.isNotEmpty(cacheKey.getNamespace())) {
            b.append(cacheKey.getNamespace()).append(":");
        }
        b.append("$KEY").append(":");
        b.append(cacheKey.getKey());
        if (StringHelper.isNotEmpty(cacheKey.getHfield())) {
            b.append(":").append(cacheKey.getHfield());
        }
        return stringToBytes(b.toString());
    }
    
    protected void readRegionKey(byte[] regionKeyBytes, RegionInfo ri) {
        String regionKey = bytesToString(regionKeyBytes);
        String namespace = ri.getNamespace();
        if (StringHelper.isNotEmpty(namespace)) {
            regionKey = regionKey.substring(namespace.length());
        }
        String[] args = StringHelper.splitTokens(regionKey, ":");
        if (args.length == 2) {
            ri.setClassName(args[0]);
            ri.setMethodDesc(args[1]);
        }
    }
    
    protected void readRegionValue(byte[] regionValueBytes, RegionInfo ri) {
        if (regionValueBytes.length != 5) {
            return;
        }
        ri.setRefresh(regionValueBytes[0] == 1);
        ri.setExpire(CacheUtil.bytesToInt(regionValueBytes, 1));
    }
    
    protected void removeFromRegion(CacheKey cacheKey) {
        byte[] keyArgsBytes = toKeyArgsBytes(cacheKey);
        
        // 根据KEY获取类方法和请求参数
        byte[] fullValueBytes = jedis.get(keyArgsBytes);
        Object[] args = Serialize.fstdeserialize(fullValueBytes);
        if (args == null || args.length < 6) {
            return;
        }
        
        // int expire = (int) args[0];
        // boolean refresh = (boolean) args[1];
        // String neanName = (String) args[2];
        String className = (String) args[3];
        String methodDesc = (String) args[4];
        // Object[] arguments = (Object[]) args[5];
        byte[] regionKeyBytes = joinToBytes(cacheKey.getNamespace(), className, methodDesc);
        
        // 删除KEY关联的类、方法和请求参数
        jedis.del(keyArgsBytes);
        
        // 从区域删除KEY
        jedis.zrem(regionKeyBytes, cacheKey.getFullKeyBytes());
        
    }
    
    public void addToRegion(String className, Method method, Object[] arguments, int expire,
        boolean refresh, String refreshBeanName, CacheKey cacheKey, long ct) {
        if (expire <= 0) {
            return;
        }
        
        String methodDesc = ReflectUtils.getDesc(method);
        String namespace = cacheKey.getNamespace();
        byte[] regionKeyBytes = joinToBytes(namespace, className, methodDesc);
        
        // 保存KEY关联的类、方法和请求参数
        byte[] keyArgsBytes = toKeyArgsBytes(cacheKey);
        Object[] args = new Object[] {expire, refresh, refreshBeanName, className, methodDesc,
                arguments };
        byte[] fullValueBytes = Serialize.fstserialize(args);
        //expire+两分钟
        jedis.setex(keyArgsBytes, expire + 120, fullValueBytes);
        
        // 保存KEY到区域
        byte[] fullKeyBytes = cacheKey.getFullKeyBytes();
        long score = Long.MAX_VALUE;
        if (expire > 0) {
            score = ct + expire * 1000;// 失效时间作为score
        }
        jedis.zadd(regionKeyBytes, score, fullKeyBytes);
        
        // 检查region是否在指定容器里,不在就保存
        byte[] regionSetKeyBytes = regionsKeyToBytes(cacheKey.getNamespace());
        byte[] regionValueBytes = new byte[5];
        regionValueBytes[0] = (refresh ? (byte) 1
            : 0);
        CacheUtil.intToBytes(regionValueBytes, 1, expire);
        jedis.hset(regionSetKeyBytes, regionKeyBytes, regionValueBytes);
    }
	
    @Override
    public Long incr(CacheKey key) {
        byte[] keyBytes = stringToBytes(key.getCacheKey());

        Long ret = jedis.incr(keyBytes);
        return ret;
    }
    
    
    @Override
    public Long expire(CacheKey key, int expire) {
        byte[] keyBytes = stringToBytes(key.getCacheKey());
        return jedis.expire(keyBytes, expire);
    }

	@Override
	public Long decr(CacheKey key) {
        byte[] keyBytes = stringToBytes(key.getCacheKey());

        Long ret = jedis.decr(keyBytes);
        return ret;
	}
 
}
