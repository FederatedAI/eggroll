package com.webank.ai.eggroll.framework.egg.computing.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.webank.ai.eggroll.core.constant.StringConstants;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.server.ServerConf;
import com.webank.ai.eggroll.core.utils.PropertyGetter;
import com.webank.ai.eggroll.core.utils.RuntimeUtils;
import com.webank.ai.eggroll.core.utils.impl.PriorityPropertyGetter;
import com.webank.ai.eggroll.framework.egg.computing.EngineOperator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.StringSubstitutor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

@Component
public class ProcessorEngineOperator implements EngineOperator {
    private static final Logger LOGGER = LogManager.getLogger();

    private static final String COMPUTING_ENGINE_NAME = "processor";
    private static final String VENV = "venv";
    private static final String PYTHON_PATH = "python-path";
    private static final String ENGINE_PATH = "engine-path";
    private static final String PORT = "port";
    private static final String DATA_DIR = "data-dir";
    private static final String LOGS_DIR = "logs-dir";
    private static final String BOOTSTRAP_SCRIPT = "bootstrap.script";
    private static final String START_PORT = "start.port";
    private static final String NODE_MANAGER = "node-manager";
    private static final String ENGINE_ADDR = "engine-addr";
    private static final String BASH = "/bin/bash";

    private static String[] scriptArgs = {VENV, PYTHON_PATH, ENGINE_PATH, PORT, DATA_DIR, LOGS_DIR, NODE_MANAGER, ENGINE_ADDR};
    private static final String startCmdTemplate;

    static {
        String argTemplate = "${e}";
        String longOptTemplate = "--e";
        String e = "e";

        List<String> startCmdElement = Lists.newArrayList();
        startCmdElement.add(BASH);
        startCmdElement.add(argTemplate.replace(e, BOOTSTRAP_SCRIPT));

        for (String arg : scriptArgs) {
            startCmdElement.add(longOptTemplate.replace(e, arg));
            startCmdElement.add(argTemplate.replace(e, arg));
        }

        startCmdTemplate = String.join(StringConstants.SPACE, startCmdElement);
        System.out.println(startCmdTemplate);
    }

    @Autowired
    private ServerConf serverConf;
    @Autowired
    private RuntimeUtils runtimeUtils;
    @Autowired
    private PropertyGetter propertyGetter;

    private final String confPrefix;

    private String startScriptPath;
    private AtomicInteger lastPort;
    private int maxPort;
    private int startPort;
    private AtomicBoolean inited;

    public ProcessorEngineOperator() {
        confPrefix = String.join(StringConstants.DOT, StringConstants.EGGROLL,
                StringConstants.COMPUTING,
                COMPUTING_ENGINE_NAME,
                StringConstants.EMPTY);

        inited = new AtomicBoolean(false);
    }

    public void init() {
        if (inited.get()) {
            return;
        }

        String startPortString = propertyGetter.getPropertyWithTemporarySource(confPrefix + START_PORT, "50000", serverConf.getProperties());
        lastPort = new AtomicInteger(Integer.valueOf(startPortString));
        startPort = lastPort.get();
        maxPort = lastPort.get() + 5000;
        inited.compareAndSet(false,true);
    }

    @Override
    public synchronized ComputingEngine start(ComputingEngine computingEngine, Properties prop) {
        init();
        List<Properties> allSources = Lists.newArrayList();
        allSources.add(prop);
        allSources.addAll(propertyGetter.getAllSources());
        allSources.add(serverConf.getProperties());
        PriorityPropertyGetter priorityPropertyGetter = (PriorityPropertyGetter) propertyGetter;
        Map<String, String> valueBindingsMap = Maps.newHashMap();
        int port = -1;

        String bootStrapScript = priorityPropertyGetter.getPropertyInIterable(confPrefix + BOOTSTRAP_SCRIPT, allSources);
        valueBindingsMap.put(BOOTSTRAP_SCRIPT, bootStrapScript);
        for (String key : scriptArgs) {
            String actualValue = null;
            if (key.equals(PYTHON_PATH)) {
                actualValue = priorityPropertyGetter.getAllMatchingPropertiesInIterable(StringConstants.COLON, confPrefix + key, allSources);
            } else {
                actualValue = priorityPropertyGetter.getPropertyInIterable(confPrefix + key, allSources);
            }

            if (StringUtils.isBlank(actualValue) && !key.equals(PORT)) {
                throw new IllegalArgumentException("key: " +
                        key + " is blank when starting session");
            }
            valueBindingsMap.put(key, actualValue);
        }

        Process engineParentProcess = null;

        try {
            while (engineParentProcess == null) {
                LOGGER.info("[EGG][ENGINE][PROCESSOR] before update port info, startPort: {}, maxPort: {}, lastPort:{}", startPort, maxPort, lastPort.get());
                if (lastPort.get() == maxPort) {
                    lastPort.compareAndSet(maxPort, startPort);
                }
                port = lastPort.incrementAndGet();
                LOGGER.info("[EGG][ENGINE][PROCESSOR] update port: {}", port);
                if (runtimeUtils.isPortAvailable(port)) {
                    valueBindingsMap.put(PORT, String.valueOf(port));

                    String actualStartCmd = StringSubstitutor.replace(startCmdTemplate, valueBindingsMap);
                    LOGGER.info("[EGG][ENGINE][PROCESSOR] start cmd: {}", actualStartCmd);
                    engineParentProcess = Runtime.getRuntime().exec(actualStartCmd);

                    if (!engineParentProcess.isAlive()) {
                        throw new IllegalStateException("Processor engine dead: " + actualStartCmd);
                    }
                }
            }
        } catch (Throwable e) {
            Thread.currentThread().interrupt();
            throw new IllegalArgumentException(e);
        }

        return new ComputingEngine(runtimeUtils.getMySiteLocalAddress(), port, computingEngine.getComputingEngineType(), engineParentProcess);
    }

    @Override
    public void stop(ComputingEngine computingEngine) {
        Process engineParentProcess = computingEngine.getProcess();

        try {
            long pid = runtimeUtils.getPidOfProcess(engineParentProcess);
            if (pid != -1) {
                Process killer = Runtime.getRuntime().exec("pkill -P " + String.valueOf(pid));
                killer.waitFor();
            } else {
                computingEngine.getProcess().destroy();
            }
        } catch (IOException | InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }

    @Override
    public ComputingEngine stopForcibly(ComputingEngine computingEngine) {
        Process process = computingEngine.getProcess();
        process.destroyForcibly();
        return computingEngine;
    }

    @Override
    public boolean isAlive(ComputingEngine computingEngine) {
        Process process = computingEngine.getProcess();
        return process != null && process.isAlive();
    }
}
