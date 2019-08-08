package com.hjc.component.cache.util;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * 线程安全的原子循环序列
 * 
 * @author hjc
 * 
 */
public class AtomicIntSeq extends AtomicInteger {
    private static final long serialVersionUID = 5411593568581628316L;
    private int               maxSeq;                                 // 循环序列的最大值
    
    /**
     * 构造
     * 
     * @param maxSeq
     */
    public AtomicIntSeq(int maxSeq) {
        this.maxSeq = maxSeq;
    }
    
    /**
     * 设置循环序列的最大值
     * 
     * @param maxSeq
     *            循环序列的最大值
     */
    public void setMaxSeq(int maxSeq) {
        this.maxSeq = maxSeq;
    }
    
    /**
     * 获取下一个循环序列
     * 
     * @return 循环序列值
     */
    public final int getSeq() {
        while (true) {
            int current = get();
            int next = current + 1;
            
            if (next >= maxSeq) {
                next = 0;
            }
            
            if (compareAndSet(current, next)) {
                return current;
            }
        }
    }
    
}
