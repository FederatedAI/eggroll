package com.webank.eggroll.nodemanager.pojo;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.pojo.RuntimeErConf;
import com.webank.eggroll.core.meta.Meta;
import com.webank.eggroll.nodemanager.meta.NodeManagerMeta;
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
    private Integer cmPort;
    private String pythonPath;
    private String pythonVenv;
    private Long processorId;
    private String moduleName;
    private String startCmd;
    private String staticConfPath;

    public ContainerParam(RuntimeErConf conf,String moduleName, Long processorId) {

        // exePath
        if (moduleName != null && !moduleName.isEmpty()) {
            switch (moduleName){
                case Dict.EGG_PAIR:
                    exePath = MetaInfo.CONFKEY_RESOURCE_MANAGER_BOOTSTRAP_EGG_PAIR_EXE_PATH;
                    break;
                case Dict.EGG_FRAME:
                    exePath = MetaInfo.CONFKEY_RESOURCE_MANAGER_BOOTSTRAP_EGG_FRAME_EXE_PATH;
                    break;
                default:
                    break;
            }
        }
        isWindows = System.getProperty("os.name").toLowerCase().indexOf("windows") >= 0;
        bootStrapShell = isWindows ? "C:\\Windows\\System32\\cmd.exe" : "/bin/bash";
        exeCmd = isWindows ? "start /b python" : bootStrapShell;
        bootStrapShellArgs = isWindows ? "/c" : "-c";
        sessionId = conf.get(Dict.CONFKEY_SESSION_ID,"");
        serverNodeId = String.valueOf(NodeManagerMeta.serverNodeId);
        boot = MetaInfo.BOOTSTRAP_ROOT_SCRIPT == null ? "bin/eggroll_boot." + (isWindows ? "py" : "sh") : MetaInfo.BOOTSTRAP_ROOT_SCRIPT;
        logsDir = MetaInfo.EGGROLL_LOGS_DIR;
        cmPort =  MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT;
        pythonPath = MetaInfo.EGGROLL_SESSION_PYTHON_PATH;
        pythonVenv = MetaInfo.EGGROLL_SESSION_PYTHON_VENV;
        staticConfPath = MetaInfo.STATIC_CONF_PATH;
        this.processorId = processorId;
        this.moduleName = moduleName;
    }
}
