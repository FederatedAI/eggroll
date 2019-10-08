package com.webank.ai.eggroll.framework.roll.manager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.constant.StringConstants;
import com.webank.ai.eggroll.core.utils.TypeConversionUtils;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Node;
import com.webank.ai.eggroll.framework.roll.api.grpc.client.EggSessionServiceClient;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.concurrent.GuardedBy;
import java.util.Map;
import java.util.Set;

@Component
public class RollSessionManager {
    private @GuardedBy("sessionLock") Map<String, BasicMeta.SessionInfo> sessionIdToSessions;
    private @GuardedBy("sessionLock") Map<String, Set<BasicMeta.Endpoint>> sessionIdToInitializedEndpoints;
    private final Object sessionLock;

    @Autowired
    private EggSessionServiceClient eggSessionServiceClient;
    @Autowired
    private TypeConversionUtils typeConversionUtils;

    private static final Logger LOGGER = LogManager.getLogger();

    public RollSessionManager() {
        sessionLock = new Object();
        sessionIdToSessions = Maps.newConcurrentMap();
        sessionIdToInitializedEndpoints = Maps.newConcurrentMap();
    }

    public BasicMeta.SessionInfo getOrCreateSession(BasicMeta.SessionInfo sessionInfo) {
        Preconditions.checkNotNull(sessionInfo);

        String sessionId = sessionInfo.getSessionId();
        BasicMeta.SessionInfo result = null;

        if (!sessionIdToSessions.containsKey(sessionId)) {
            synchronized (sessionLock) {
                if (!sessionIdToSessions.containsKey(sessionId)) {
                    sessionIdToSessions.put(sessionId, sessionInfo);
                    sessionIdToInitializedEndpoints.put(sessionId, Sets.newConcurrentHashSet());
                }
            }
        }
        result = sessionIdToSessions.get(sessionId);

        return result;
    }

    public boolean stopSession(String sessionId) {
        boolean result = false;

        BasicMeta.SessionInfo sessionInfo = sessionIdToSessions.get(sessionId);
        if (sessionInfo == null) {
            result = true;
            return result;
        }
        Set<BasicMeta.Endpoint> initializedEndpoints = Sets.newHashSet();
        synchronized (sessionLock) {
            sessionIdToSessions.remove(sessionId);
            initializedEndpoints = sessionIdToInitializedEndpoints.remove(sessionId);
        }

        for (BasicMeta.Endpoint endpoint : initializedEndpoints) {
            eggSessionServiceClient.stopSession(sessionInfo, endpoint);
        }

        result = true;
        return result;
    }

    public boolean initializeEgg(BasicMeta.SessionInfo sessionInfo, Node node) {
        boolean result = false;

        if (sessionInfo == null) {
            return result;
        }

        String sessionId = sessionInfo.getSessionId();
        if (isEggInitialized(sessionId, node)) {
            return result;
        }

        Set<BasicMeta.Endpoint> initializedEndpoints = null;
        synchronized (sessionLock) {
            if (!sessionIdToSessions.containsKey(sessionId)) {
                return result;
            }
            sessionIdToSessions.put(sessionId, sessionInfo);
            initializedEndpoints = sessionIdToInitializedEndpoints.get(sessionId);
        }

        String target = node.getIp();
        if (StringUtils.isBlank(target)) {
            target = node.getHost();
        }

        // injecting ip for target engine - need to improve
        sessionInfo = sessionInfo.toBuilder()
                .putComputingEngineConf("eggroll.computing.processor.engine-addr", target)
                .putComputingEngineConf("eggroll.computing.processor.node-manager", String.join(StringConstants.COLON, target, String.valueOf(node.getPort())))
                .build();
        BasicMeta.SessionInfo initializedSessionInfo = eggSessionServiceClient.getOrCreateSession(sessionInfo, node);
        if (initializedSessionInfo != null) {
            initializedEndpoints.add(typeConversionUtils.toEndpoint(node));
        }
        return initializedSessionInfo != null;
    }

    public boolean initializeEgg(String sessionId, Node node) {
        return initializeEgg(sessionIdToSessions.get(sessionId), node);
    }

    public boolean isEggInitialized(String sessionId, Node node) {
        boolean result = false;
        BasicMeta.SessionInfo sessionInfo = null;
        Set<BasicMeta.Endpoint> initializedEndpoints = null;
        synchronized (sessionLock) {
            sessionInfo = sessionIdToSessions.get(sessionId);

            if (sessionInfo == null) {
                return result;
            }
            initializedEndpoints = sessionIdToInitializedEndpoints.get(sessionId);
        }

        result = initializedEndpoints.contains(typeConversionUtils.toEndpoint(node));
        return result;
    }
}
