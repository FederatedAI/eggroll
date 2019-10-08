package com.webank.ai.eggroll.framework.egg.manager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.core.utils.TypeConversionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

@Component
public class EngineStatusTracker {
    @Autowired
    private TypeConversionUtils typeConversionUtils;
    @Autowired
    private ToStringUtils toStringUtils;

    private Map<String, Long> aliveEngineCache;

    private static final Logger LOGGER = LogManager.getLogger();

    public EngineStatusTracker() {
        aliveEngineCache = Maps.newConcurrentMap();
    }

    public boolean addAliveEngine(ComputingEngine computingEngine) {
        BasicMeta.Endpoint endpoint = typeConversionUtils.toEndpoint(computingEngine);

        return addAliveEngine(endpoint);
    }

    public boolean addAliveEngine(BasicMeta.Endpoint endpoint) {
        aliveEngineCache.put(toStringUtils.toOneLineString(endpoint), System.currentTimeMillis());
        return true;
    }

    public boolean isEngineAlive(ComputingEngine computingEngine) {
        BasicMeta.Endpoint endpoint = typeConversionUtils.toEndpoint(computingEngine);
        return isEngineAlive(endpoint);
    }

    public boolean isEngineAlive(BasicMeta.Endpoint endpoint) {
        boolean result = false;

        String key = toStringUtils.toOneLineString(endpoint);
        Long l = aliveEngineCache.get(key);
        if (l != null && System.currentTimeMillis() - l < 75000) {
            result = true;
        } else {
            if (aliveEngineCache.size() > 10000) {
                aliveEngineCache.remove(key);
            }
        }

        return result;
    }
}
