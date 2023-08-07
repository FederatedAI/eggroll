package com.webank.eggroll.nodemanager.pojo;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.pojo.RuntimeErConf;
import lombok.Data;

@Data
public class ContainerParam {
    private String confPrefix;
    private boolean isWindows;
    private String bootStrapShell;
    private String exeCmd;
    private String bootStrapShellArgs;
    private String exePath;
    private String sessionId;
    private String serverNodeId;
    private String boot;
    private String logsDir;
    private String cmPort;
    private String pythonPath;
    private String pythonVenv;
    private Long processorId;
    private String moduleName;
    private String startCmd;

    public ContainerParam(RuntimeErConf conf,String moduleName, Long processorId) {
        confPrefix = Dict.EGGROLL_RESOURCEMANAGER_BOOTSTRAP + "." + moduleName;
        isWindows = System.getProperty("os.name").toLowerCase().indexOf("windows") >= 0;
        bootStrapShell = conf.get(Dict.BOOTSTRAP_SHELL, isWindows ? "C:\\Windows\\System32\\cmd.exe" : "/bin/bash");
        exeCmd = isWindows ? "start /b python" : bootStrapShell;
        bootStrapShellArgs = conf.getString(Dict.BOOTSTRAP_SHELL_ARGS, isWindows ? "/c" : "-c");
        exePath = conf.get(confPrefix + ".exepath", "");
        sessionId = conf.getString(Dict.CONFKEY_SESSION_ID, "");
        serverNodeId = conf.get(Dict.SERVER_NODE_ID, "2");
        boot = conf.get(Dict.BOOTSTRAP_ROOT_SCRIPT, "bin/eggroll_boot." + (isWindows ? "py" : "sh"));
        logsDir = MetaInfo.EGGROLL_LOGS_DIR;
        cmPort = conf.get(Dict.CONFKEY_CLUSTER_MANAGER_PORT, "4670");
        pythonPath = conf.getString(Dict.EGGROLL_SESSION_PYTHON_PATH, "");
        pythonVenv = conf.getString(Dict.EGGROLL_SESSION_PYTHON_VENV, "");
        this.processorId = processorId;
        this.moduleName = moduleName;
    }
}
