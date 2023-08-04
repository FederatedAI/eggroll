package com.webank.eggroll.nodemanager.service;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.eggroll.core.pojo.RuntimeErConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@Service
public class ContainerService {

    Logger logger = LoggerFactory.getLogger(ContainerService.class);

    private String confPrefix;
    private boolean isWindows;
    private String bootStrapShell;
    private String exeCmd;
    private String bootStrapShellArgs;
    private String exePath;
    private String sessionId;
    private String myServerNodeId;
    private String boot;
    private String logsDir;
    private String cmPort;
    private String pythonPath;
    private String pythonVenv;
    private Long processorId;
    private String moduleName;


    public void initParam(RuntimeErConf conf, String moduleName, Long processorId) {
        confPrefix = Dict.EGGROLL_RESOURCEMANAGER_BOOTSTRAP + "." + moduleName;
        isWindows = System.getProperty("os.name").toLowerCase().indexOf("windows") >= 0;
        bootStrapShell = conf.get(Dict.BOOTSTRAP_SHELL, isWindows ? "C:\\Windows\\System32\\cmd.exe" : "/bin/bash");
        exeCmd = isWindows ? "start /b python" : bootStrapShell;
        bootStrapShellArgs = conf.getString(Dict.BOOTSTRAP_SHELL_ARGS, isWindows ? "/c" : "-c");
        exePath = conf.get(confPrefix + ".exepath", "");
        sessionId = conf.getString(Dict.CONFKEY_SESSION_ID, "");
        myServerNodeId = conf.get(Dict.SERVER_NODE_ID, "2");
        boot = conf.get(Dict.BOOTSTRAP_ROOT_SCRIPT, "bin/eggroll_boot." + (isWindows ? "py" : "sh"));
        logsDir = MetaInfo.EGGROLL_LOGS_DIR;
        cmPort = conf.get(Dict.CONFKEY_CLUSTER_MANAGER_PORT, "4670");
        pythonPath = conf.getString(Dict.EGGROLL_SESSION_PYTHON_PATH, "");
        pythonVenv = conf.getString(Dict.EGGROLL_SESSION_PYTHON_VENV, "");
        this.processorId = processorId;
        this.moduleName = moduleName;
    }

    public ErSessionMeta operateContainers(ErSessionMeta sessionMeta, String opType) {
        List<ErProcessor> processors = sessionMeta.getProcessors();
        RuntimeErConf runtimeErConf = new RuntimeErConf(sessionMeta);
        Long myServerNodeId = runtimeErConf.get(Dict.SERVER_NODE_ID, -1L);
        for (ErProcessor p : processors) {
            if (p.getServerNodeId() != myServerNodeId) {
                continue;
            }
            switch (opType) {
                case Dict.NODE_CMD_START:
                    start();
                    break;
                case Dict.NODE_CMD_STOP:
                    stop();
                    break;
                case Dict.NODE_CMD_KILL:
                    kill();
                    break;
                default:
                    logger.error("option not support: {}", opType);
                    break;
            }
        }
        return sessionMeta;
    }


    private boolean start() {
        String startCmd = "";
        String pythonPathArgs = "";
        String pythonVenvArgs = "";
        String staticConfigPath = MetaInfo.STATIC_CONF_PATH;
        if (StringUtils.hasText(pythonPath)) {
            pythonPathArgs = "--python-path " + pythonPath;
        }
        if (StringUtils.hasText(pythonVenv)) {
            pythonVenvArgs = "--python-venv " + pythonVenv;
        }

        StringJoiner joiner = new StringJoiner(" ");
        joiner.add(exeCmd)
                .add(boot)
                .add("start")
                .add(exePath)
                .add("--config")
                .add(staticConfigPath)
                .add(pythonPathArgs)
                .add(pythonVenvArgs)
                .add("--session-id")
                .add(sessionId)
                .add(myServerNodeId)
                .add("--processor-id")
                .add(String.valueOf(processorId))
                .add(moduleName + "-" + processorId)
                .add("&");

        startCmd = joiner.toString();
        String standaloneTag = System.getProperty("eggroll.standalone.tag", "");
        logger.info(standaloneTag + joiner);

        Thread thread = runCommand(startCmd);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.error("InterruptedException: ", e.getMessage());
        }
        return thread.isAlive();
    }

    private boolean stop() {
        return true;
    }

    private boolean kill() {
        return true;
    }

    private Thread runCommand(String startCmd) {
        return new Thread(() -> {
            ProcessBuilder processorBuilder = new ProcessBuilder(bootStrapShell, bootStrapShellArgs, startCmd);
            Map<String, String> builderEnv = processorBuilder.environment();
            if (StringUtils.hasText(System.getProperty("eggroll.standalone.tag"))) {
                logger.info("set EGGROLL_STANDALONE_PORT :", cmPort);
                builderEnv.put("EGGROLL_STANDALONE_PORT", cmPort);
            }
            File logPath = new File(logsDir + File.separator + sessionId + File.separator);
            if (!logPath.exists()) {
                logPath.mkdirs();
            }
            processorBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logPath, "bootstrap-" + moduleName + "-" + processorId + ".out")));
            processorBuilder.redirectError(ProcessBuilder.Redirect.appendTo(new File(logPath, "bootstrap-" + moduleName + "-" + processorId + ".err")));
            try {
                Process process = processorBuilder.start();
                process.waitFor();
            } catch (IOException | InterruptedException e) {
                logger.error(e.getMessage());
            }
        });
    }

}
