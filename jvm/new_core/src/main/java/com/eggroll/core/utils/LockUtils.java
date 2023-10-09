package com.eggroll.core.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public class LockUtils {

    static Logger logger = LoggerFactory.getLogger(LockUtils.class);

    public static <T> void lock(ConcurrentHashMap<T, ReentrantLock> lockMap, T key){
//        if(lockMap == null){
//            throw new RuntimeException("lockMap is null");
//        }
//        ReentrantLock lock;
//        if (!lockMap.containsKey(key)) {
//            lockMap.putIfAbsent(key, new ReentrantLock());
//        }
//        lock = lockMap.get(key);
////        logger.info("lock key {}",key);
//        lock.lock();
    }

    public static <T> void unLock(ConcurrentHashMap<T, ReentrantLock> lockMap, T key){
//        if(lockMap == null){
//            throw new RuntimeException("lockMap is null");
//        }
////        logger.info("unlock key {}",key);
//        ReentrantLock  lock = lockMap.get(key);
//        if(lock!=null){
//            lock.unlock();
//        }
    }

    public static void main(String[] args) {
//        ConcurrentHashMap<Object, ReentrantLock> lockMap = new ConcurrentHashMap<>();
//        LockUtils.lock(lockMap,String.valueOf(123));
//        LockUtils.unLock(lockMap,String.valueOf(123));
    }


}
