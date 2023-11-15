package org.fedai.eggroll.core.utils;

import org.fedai.eggroll.core.pojo.ErProcessor;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.concurrent.TimeUnit;

public class CacheUtil {

    public static Cache<String, ErProcessor> buildErProcessorCache(int maxsize, int expireTime, TimeUnit timeUnit) {
        return CacheBuilder.newBuilder()
                .maximumSize(maxsize)
                .expireAfterWrite(expireTime, timeUnit)
                .build();

    }
}