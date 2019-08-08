package com.hjc.component.cache.util.config;


import com.hjc.component.cache.util.StringHelper;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Enumeration;
import java.util.List;
import java.util.Properties;

/**
 * 系统配置信息
 * 
 * @author hjc
 * 
 */
public class SysConfig {
    private final static String     cfgFile        = "sysconfig.props";
    private final static String     keySysId       = "sysconfig.sysId";
    private final static String     keySysOrgid    = "sysconfig.sysOrgid";
    private final static Properties systemProperty = new Properties();
    
    static {
        init();
    }
    
    /**
     * 用配置文件初始化参数。
     */
    private static void init() {
        InputStream is = null;
        try {
            is = Thread.currentThread().getContextClassLoader().getResourceAsStream(cfgFile);
            if (is != null) {
                List<String> lines = IOUtils.readLines(is);
                for (String line : lines) {
                    String str = StringHelper.trimMore(line);
                    if (StringHelper.isEmpty(str) || str.startsWith("#") || str.startsWith("﻿#")) {
                        continue;
                    }
                    int pos = str.indexOf('=');
                    if (pos < 0) {
                        continue;
                    }

                    String key = str.substring(0, pos);
                    String value = str.substring(pos + 1);
                    systemProperty.put(key, value);
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("加载配置文件失败cfgFile" + cfgFile, e);
        } finally {
            if (StringHelper.isEmpty(systemProperty.getProperty(keySysId))) {
                systemProperty.remove(keySysId);
                systemProperty.put(keySysId, "6001010001");
            }
            if (StringHelper.isEmpty(systemProperty.getProperty(keySysOrgid))) {
                systemProperty.remove(keySysOrgid);
                systemProperty.put(keySysOrgid, "600101");
            }
            IOUtils.closeQuietly(is);
        }
    }
    
    /**
     * 获取系统ID
     * 
     * @return 系统ID
     */
    public static String getSysId() {
        return systemProperty.getProperty(keySysId);
    }
    
    /**
     * 获取系统机构ID
     * 
     * @return 系统机构ID
     */
    public static String getSysOrgid() {
        return systemProperty.getProperty(keySysOrgid);
    }
    
    /**
     * 获取key的值
     * 
     * @param key
     *            键名
     * @return 键值
     */
    public static String getProperty(String key) {
        return systemProperty.getProperty(key);
    }
    
    /**
     * 获取指定前缀的属性
     * 
     * @param prefix
     *            前缀
     * @return 指定前缀的属性
     */
    public static Properties getProperties(String prefix) {
        Properties new_props = new Properties();
        Enumeration<Object> keys = systemProperty.keys();
        while (keys.hasMoreElements()) {
            String key = (String) keys.nextElement();
            if (key.startsWith(prefix)) {
                new_props.setProperty(key.substring(prefix.length()),
                    systemProperty.getProperty(key));
            }
        }
        return new_props;
    }
}
