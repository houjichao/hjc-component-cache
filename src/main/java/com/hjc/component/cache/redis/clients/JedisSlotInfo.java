package com.hjc.component.cache.redis.clients;

import java.util.List;


import com.hjc.component.cache.util.AtomicIntSeq;
import redis.clients.jedis.JedisPool;

public class JedisSlotInfo {
    private AtomicIntSeq seq;
    private JedisPool    masterPool;
    private JedisPool[]  readPool;
    
    public JedisSlotInfo(JedisPool masterPool, List<JedisPool> slaves) {
        this.masterPool = masterPool;
        
        int slaveNum = 0;
        if (slaves != null) {
            slaveNum = slaves.size();
        }
        this.readPool = new JedisPool[slaveNum + 1];
        this.readPool = slaves.toArray(this.readPool);
        this.readPool[slaveNum] = masterPool;//主可读
        this.seq = new AtomicIntSeq(this.readPool.length);
    }
    
    public JedisPool getMasterPool() {
        return this.masterPool;
    }
    
    public JedisPool getReadPool() {
        int pos = seq.getSeq();
        if (pos >= 0 && pos < readPool.length) {
            return this.readPool[pos];
        }
        return this.masterPool;
    }
}
