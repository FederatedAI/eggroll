package com.webank.eggroll.core.util;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.webank.eggroll.core.meta.ErProcessor;

import java.util.concurrent.TimeUnit;

public class CacheUtil {

    public static   Cache<String,ErProcessor> buildErProcessorCache(int  maxsize, int expireTime, TimeUnit  timeUnit){
        return CacheBuilder.newBuilder()
                .maximumSize(maxsize)
                .expireAfterWrite(expireTime,timeUnit)
                .build();

    }







    
}