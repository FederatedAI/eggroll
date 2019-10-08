package com.webank.ai.eggroll.framework.egg.computing;


import com.webank.ai.eggroll.core.model.ComputingEngine;

import java.util.Properties;

public interface EngineOperator {
    public ComputingEngine start(ComputingEngine computingEngine, Properties prop);
    public void stop(ComputingEngine computingEngine);
    public ComputingEngine stopForcibly(ComputingEngine computingEngine);
    public boolean isAlive(ComputingEngine computingEngine);
}
