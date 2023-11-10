package com.eggroll.core.utils;

import com.google.common.cache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

public class LockUtils {

    static Logger logger = LoggerFactory.getLogger(LockUtils.class);

    public static <T> void lock(Cache<T, ReentrantLock> lockCache, T key) {
        if (lockCache == null) {
            throw new RuntimeException("lockCache is null");
        }
        if (key == null) {
            throw new RuntimeException("lockCache key is null");
        }
        ReentrantLock lock;
        if (lockCache.getIfPresent(key) == null) {
            lockCache.put(key,new ReentrantLock());
        }
        lock = lockCache.getIfPresent(key);
        lock.lock();
    }

    public static <T> void unLock(Cache<T, ReentrantLock> lockCache, T key) {
        if (lockCache == null) {
            throw new RuntimeException("lockCache is null");
        }
        if (key == null) {
            throw new RuntimeException("lockCache key is null");
        }
        ReentrantLock lock = lockCache.getIfPresent(key);
        if (lock != null) {
            lock.unlock();
        }
    }

}
