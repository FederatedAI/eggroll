package com.webank.ai.eggroll.framework.egg.manager;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.Sets;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.utils.TypeConversionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Set;
import java.util.concurrent.TimeUnit;

@Component
public class EngineStatusTracker {
    @Autowired
    private TypeConversionUtils typeConversionUtils;

    private Cache<BasicMeta.Endpoint, Long> aliveEngineCache;

    private static final Logger LOGGER = LogManager.getLogger();

    public EngineStatusTracker() {
        aliveEngineCache = CacheBuilder.newBuilder()
                .expireAfterWrite(60, TimeUnit.SECONDS)
                .maximumSize(10000)
                .initialCapacity(10000)
                .weakKeys()
                .weakValues()
                .build();
    }

    public boolean addAliveEngine(ComputingEngine computingEngine) {
        BasicMeta.Endpoint endpoint = typeConversionUtils.toEndpoint(computingEngine);

        return addAliveEngine(endpoint);
    }

    public boolean addAliveEngine(BasicMeta.Endpoint endpoint) {
        aliveEngineCache.put(endpoint, System.currentTimeMillis());
        return true;
    }

    public boolean isEngineAlive(ComputingEngine computingEngine) {
        BasicMeta.Endpoint endpoint = typeConversionUtils.toEndpoint(computingEngine);
        return isEngineAlive(endpoint);
    }

    public boolean isEngineAlive(BasicMeta.Endpoint endpoint) {
        Long result = aliveEngineCache.getIfPresent(endpoint);
        // LOGGER.info("alive result: {}", result);

        return result != null;
    }
}
