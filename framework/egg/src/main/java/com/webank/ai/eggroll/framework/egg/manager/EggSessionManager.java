package com.webank.ai.eggroll.framework.egg.manager;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.retry.RetryException;
import com.webank.ai.eggroll.core.retry.Retryer;
import com.webank.ai.eggroll.core.retry.factory.RetryerBuilder;
import com.webank.ai.eggroll.core.retry.factory.StopStrategies;
import com.webank.ai.eggroll.core.retry.factory.WaitStrategies;
import com.webank.ai.eggroll.core.retry.factory.WaitTimeStrategies;
import com.webank.ai.eggroll.core.server.ServerConf;
import com.webank.ai.eggroll.core.utils.PropertyGetter;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.core.utils.TypeConversionUtils;
import com.webank.ai.eggroll.framework.egg.computing.EngineOperator;
import com.webank.ai.eggroll.framework.egg.model.EggrollSession;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.concurrent.GuardedBy;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Component
public class EggSessionManager {
    @Autowired
    private PropertyGetter propertyGetter;
    @Autowired
    private EngineOperator engineOperator;
    @Autowired
    private TypeConversionUtils typeConversionUtils;
    @Autowired
    private EngineStatusTracker engineStatusTracker;
    @Autowired
    private ToStringUtils toStringUtils;
    @Autowired
    private ServerConf serverConf;

    @GuardedBy("sessionIdToResourceLock")
    private final Map<String, EggrollSession> sessionIdToResource;

    private final Object sessionIdToResourceLock;

    private static final Logger LOGGER = LogManager.getLogger();

    public EggSessionManager() {
        this.sessionIdToResource = new ConcurrentHashMap<>();
        this.sessionIdToResourceLock = new Object();
    }

    public BasicMeta.SessionInfo getSession(String sessionId) {
        BasicMeta.SessionInfo result = null;

        EggrollSession eggrollSession = sessionIdToResource.get(sessionId);
        if (eggrollSession != null) {
            result = eggrollSession.getSessionInfo();
        }

        return result;
    }

    public synchronized boolean getOrCreateSession(BasicMeta.SessionInfo sessionInfo) {
        boolean result = false;
        String sessionId = sessionInfo.getSessionId();

        if (sessionIdToResource.containsKey(sessionId)) {
            result = true;
            return result;
        }
        final EggrollSession eggrollSession = new EggrollSession(sessionInfo);
        synchronized (sessionIdToResourceLock) {
            if (!sessionIdToResource.containsKey(sessionId)) {
                sessionIdToResource.putIfAbsent(sessionId, eggrollSession);
            } else {
                result = true;
                return result;
            }
        }

        Properties sessionProperties = new Properties();
        sessionProperties.putAll(sessionInfo.getComputingEngineConfMap());
        String maxSessionEngineCountInConfString = propertyGetter.getPropertyWithTemporarySource("eggroll.computing.processor.session.max.count", "1", sessionProperties);
        int maxSessionEngineCountInConf = Integer.parseInt(maxSessionEngineCountInConfString);
        int finalMaxSessionEngineCount = Math.max(maxSessionEngineCountInConf, 1);
        finalMaxSessionEngineCount = Math.min(finalMaxSessionEngineCount, 512);

        LOGGER.info("[EGG][SESSIONMANAGER] maxSessionEngineCountInConf: {}, finalMaxSessionEngineCount: {}", maxSessionEngineCountInConf, finalMaxSessionEngineCount);

        ComputingEngine typeParamComputingEngine = new ComputingEngine(ComputingEngine.ComputingEngineType.EGGROLL);
        for (int i = 0; i < finalMaxSessionEngineCount; ++i) {
            LOGGER.info("[EGG][SESSIONMANAGER] starting Computing Engine: {}", sessionProperties);
            ComputingEngine engine = engineOperator.start(typeParamComputingEngine, sessionProperties);
            eggrollSession.addComputingEngine(engine);
        }

        Retryer<Boolean> allReadyRetryer = RetryerBuilder.<Boolean>newBuilder()
                .retryIfResult(b -> Objects.equals(b, Boolean.FALSE))
                .withStopStrategy(StopStrategies.stopAfterMaxDelay(5000))
                .withWaitStrategy(WaitStrategies.threadSleepWait())
                .withWaitTimeStrategy(WaitTimeStrategies.fixedWaitTime(100, TimeUnit.MILLISECONDS))
                .build();

        LOGGER.info("[EGG][SESSIONMANAGER] ready to check if processors are up");
        final Callable<Boolean> engineStatusChecker = () -> {
            boolean isAllReady = true;
            for (ComputingEngine engine : eggrollSession.getAllComputingEngines()) {
                isAllReady = engineStatusTracker.isEngineAlive(engine);
                if (!isAllReady) {
                    break;
                }
            }

            LOGGER.info("[EGG][SESSIONMANAGER] check result: {}", isAllReady);
            return isAllReady;
        };

        try {
            result = allReadyRetryer.call(engineStatusChecker);
            LOGGER.info("[EGG][SESSIONMANAGER] returned check result: {}", result);
        } catch (ExecutionException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("[EGG][SESSIONMANAGER] start engine failed in retries. sessionId: {}", sessionId);
            stopSession(sessionId);
            throw new RuntimeException(e);
        } catch (RetryException e) {
            Thread.currentThread().interrupt();
            LOGGER.error("[EGG][SESSIONMANAGER] start engine failed after retries. sessionId: {}", sessionId);
            stopSession(sessionId);
            throw new RuntimeException(e);
        }

        return result;
    }

    public boolean stopSession(String sessionId) {
        boolean result = true;
        // todo: kill and remove resources
        EggrollSession eggrollSession = sessionIdToResource.remove(sessionId);

        if (eggrollSession == null) {
            return result;
        }
        List<ComputingEngine> computingEngines = eggrollSession.getAllComputingEngines();

        computingEngines.forEach((engine) -> engineOperator.stop(engine));

        eggrollSession.close();
        result = true;
        return result;
    }

    public synchronized ComputingEngine getComputeEngine(String sessionId) {
        EggrollSession eggrollSession = sessionIdToResource.get(sessionId);
        if (eggrollSession == null) {
            throw new IllegalStateException("sessionId " + sessionId + " does not exist");
        }

        ComputingEngine result = eggrollSession.getComputingEngine();

        if (!engineOperator.isAlive(result)) {
            // todo: remove dead computingEngine when restart is needed
            LOGGER.info("[EGG][SESSIONMANAGER] engine {} is dead, restarting.", toStringUtils.toOneLineString(result));
            ComputingEngine deadEngine = eggrollSession.removeComputingEngine(result);
            engineOperator.stopForcibly(result);
            result = engineOperator.start(result, typeConversionUtils.toProperties(eggrollSession.getSessionInfo().getComputingEngineConfMap()));
        }

        return result;
    }
}
