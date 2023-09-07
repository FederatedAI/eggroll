package com.webank.eggroll.nodemanager.service;

import com.eggroll.core.config.Dict;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.eggroll.core.pojo.RuntimeErConf;
import com.eggroll.core.utils.JsonUtil;
import com.google.inject.Singleton;
import com.webank.eggroll.nodemanager.meta.NodeManagerMeta;
import com.webank.eggroll.nodemanager.pojo.ContainerParam;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;

@Singleton
public class ContainerService {

    Logger logger = LoggerFactory.getLogger(ContainerService.class);

    public ErSessionMeta operateContainers(Context context, ErSessionMeta sessionMeta, String opType) {
        context.setSessionId(sessionMeta.getId());
        List<ErProcessor> processors = sessionMeta.getProcessors();
        RuntimeErConf runtimeErConf = new RuntimeErConf(sessionMeta);
        Long myServerNodeId = NodeManagerMeta.serverNodeId;
        logger.info("operateContainers param opType: {}, myServerNodeId:{}",opType,myServerNodeId);
        for (ErProcessor p : processors) {
            if (p.getServerNodeId() != myServerNodeId) {
                continue;
            }
            ContainerParam param = new ContainerParam(runtimeErConf, p.getProcessorType(), p.getId());
            switch (opType) {
                case Dict.NODE_CMD_START:
                    start(param);
                    break;
                case Dict.NODE_CMD_STOP:
                    stop(param);
                    break;
                case Dict.NODE_CMD_KILL:
                    kill(param);
                    break;
                default:
                    logger.error("option not support: {}", opType);
                    break;
            }
        }
        return sessionMeta;
    }

    private boolean start(ContainerParam param) {
        String pythonPathArgs = "";
        String pythonVenvArgs = "";
        if (param.getPythonPath() != null && !param.getPythonPath().isEmpty()) {
            pythonPathArgs = "--python-path " + param.getPythonPath();
        }
        if (param.getPythonVenv() != null && !param.getPythonVenv().isEmpty()) {
            pythonVenvArgs = "--python-venv " + param.getPythonVenv();
        }
        StringJoiner joiner = new StringJoiner(" ");
        joiner.add(param.getExeCmd())
                .add(param.getBoot())
                .add("start")
                .add("\"" + param.getExePath())
                .add("--config")
                .add(param.getStaticConfPath())
                .add(pythonPathArgs)
                .add(pythonVenvArgs)
                .add("--session-id")
                .add(param.getSessionId())
                .add("--server-node-id")
                .add(param.getServerNodeId())
                .add("--processor-id")
                .add(param.getProcessorId() + "\"")
                .add(param.getModuleName() + "-" + param.getProcessorId())
                .add("&");

        param.setStartCmd(joiner.toString());
        String standaloneTag = System.getProperty("eggroll.standalone.tag", "");
        logger.info(standaloneTag + joiner);
        logger.info("============runCommand===========: {}", JsonUtil.object2Json(param));
        Thread thread = runCommand(param);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.error("InterruptedException: {}", e.getMessage());
        }
        return thread.isAlive();
    }

    private boolean stop(ContainerParam param) {
        return doStop(param, false);
    }

    private boolean kill(ContainerParam param) {
        return doStop(param, true);
    }

    private boolean doStop(ContainerParam param, boolean force) {
        String option = force ? "kill" : "stop";
        String linuxSubCmd = String.format("ps aux | grep 'session-id %s' | grep 'server-node-id %s' | grep 'processor-id %s", param.getSessionId(), param.getServerNodeId(), param.getProcessorId());
        String subCmd = param.isWindows() ? "None" : linuxSubCmd;
        String doStopCmd = new StringJoiner(" ")
                .add(param.getExeCmd())
                .add(param.getBoot())
                .add(option)
                .add(String.format("\"%s\"", subCmd))
                .add(String.format("%s-%s", param.getModuleName(), param.getProcessorId()))
                .toString();
        logger.info("doStopCmd : {}", doStopCmd);
        param.setExeCmd(doStopCmd);
        Thread thread = runCommand(param);
        thread.start();
        try {
            thread.join();
        } catch (InterruptedException e) {
            logger.error("InterruptedException: {}", e.getMessage());
        }
        return thread.isAlive();
    }

    private Thread runCommand(ContainerParam param) {
        return new Thread(() -> {
            ProcessBuilder processorBuilder = new ProcessBuilder(param.getBootStrapShell(), param.getBootStrapShellArgs(), param.getStartCmd());
            Map<String, String> builderEnv = processorBuilder.environment();
            if (System.getProperty("eggroll.standalone.tag") != null && !System.getProperty("eggroll.standalone.tag").isEmpty()) {
                logger.info("set EGGROLL_STANDALONE_PORT :", param.getCmPort());
                builderEnv.put("EGGROLL_STANDALONE_PORT", String.valueOf(param.getCmPort()));
            }
            File logPath = new File(param.getLogsDir() + File.separator + param.getSessionId() + File.separator);
            if (!logPath.exists()) {
                logPath.mkdirs();
            }
            processorBuilder.redirectOutput(ProcessBuilder.Redirect.appendTo(new File(logPath, "bootstrap-" + param.getModuleName() + "-" + param.getProcessorId() + ".out")));
            processorBuilder.redirectError(ProcessBuilder.Redirect.appendTo(new File(logPath, "bootstrap-" + param.getModuleName() + "-" + param.getProcessorId() + ".err")));
            try {
                Process process = processorBuilder.start();
                process.waitFor();
            } catch (IOException | InterruptedException e) {
                logger.error(e.getMessage());
            }
        });
    }

}