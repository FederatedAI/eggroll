package com.webank.ai.eggroll.framework.egg.manager;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.utils.PropertyGetter;
import com.webank.ai.eggroll.core.utils.TypeConversionUtils;
import com.webank.ai.eggroll.framework.egg.computing.EngineOperator;
import com.webank.ai.eggroll.framework.egg.model.EggrollSession;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class EggSessionManager {
    @Autowired
    private PropertyGetter propertyGetter;
    @Autowired
    private EngineOperator engineOperator;
    @Autowired
    private TypeConversionUtils typeConversionUtils;

    private final Map<String, EggrollSession> sessionIdToResource;

    public EggSessionManager() {
        this.sessionIdToResource = new ConcurrentHashMap<>();
    }

    public BasicMeta.SessionInfo getSession(String sessionId) {
        BasicMeta.SessionInfo result = null;

        EggrollSession eggrollSession = sessionIdToResource.get(sessionId);
        if (eggrollSession != null) {
            result = eggrollSession.getSessionInfo();
        }

        return result;
    }

    public boolean getOrCreateSession(BasicMeta.SessionInfo sessionInfo) {
        Properties sessionProperties = new Properties();
        sessionProperties.putAll(sessionInfo.getComputingEngineConfMap());
        String maxSessionEngineCountInConfString = propertyGetter.getPropertyWithTemporarySource("eggroll.computing.processor.session.max.count", "1", sessionProperties);
        int maxSessionEngineCountInConf = Integer.valueOf(maxSessionEngineCountInConfString);

        int finalMaxSessionEngineCount = Math.max(maxSessionEngineCountInConf, 1);
        finalMaxSessionEngineCount = Math.min(finalMaxSessionEngineCount, 512);

        EggrollSession eggrollSession = new EggrollSession(sessionInfo);
        ComputingEngine computingEngine = new ComputingEngine(ComputingEngine.ComputingEngineType.EGGROLL);
        for (int i = 0; i < finalMaxSessionEngineCount; ++i) {
            ComputingEngine engine = engineOperator.start(computingEngine, sessionProperties);
            eggrollSession.addComputingEngine(engine);
        }

        return sessionIdToResource.putIfAbsent(sessionInfo.getSessionId(), eggrollSession) == null;
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

    public ComputingEngine getComputeEngineDescriptor(String sessionId) {
        EggrollSession eggrollSession = sessionIdToResource.get(sessionId);
        if (eggrollSession == null) {
            throw new IllegalStateException("sessionId " + sessionId + " does not exist");
        }

        ComputingEngine result = eggrollSession.getComputingEngine();

        if (!engineOperator.isAlive(result)) {
            // todo: remove dead computingEngine when restart is needed
            ComputingEngine deadEngine = eggrollSession.removeComputingEngine(result);
            engineOperator.stopForcibly(result);
            result = engineOperator.start(result, typeConversionUtils.toProperties(eggrollSession.getSessionInfo().getComputingEngineConfMap()));
        }

        return result;
    }
}
