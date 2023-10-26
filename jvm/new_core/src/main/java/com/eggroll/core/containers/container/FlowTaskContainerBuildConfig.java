package com.eggroll.core.containers.container;

import lombok.Data;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

@Data
public class FlowTaskContainerBuildConfig {
    private String sessionId;
    private long processorId;
    private Path containerWorkspace;
    private List<String> commandArguments;
    private Map<String, String> environmentVariables;
    private Map<String, String> options;
    private PythonContainerRuntimeConfig conf;
    private String pythonExec;
    private String scriptPath;
    private Path workingDir;
    private WorkingDirectoryPreparer workingDirectoryPreparer;
    private Map<String, String> containerEnvs;
    private Path stdErrFile;
    private Path stdOutFile;
    private Map<String, String> extraEnv;

    public FlowTaskContainerBuildConfig(String sessionId,
                                        long processorId,
                                        Path containerWorkspace,
                                        List<String> commandArguments,
                                        Map<String, String> environmentVariables,
                                        Map<String, String> options,
                                        String scriptPath) throws Exception {

        this.sessionId = sessionId;
        this.processorId = processorId;
        this.containerWorkspace = containerWorkspace;
        this.commandArguments = commandArguments;
        this.environmentVariables = environmentVariables;
        this.options = options;
        this.scriptPath = scriptPath;

        this.workingDir = containerWorkspace.toAbsolutePath();
        this.workingDirectoryPreparer = new WorkingDirectoryPreparer();


        Path logDir = workingDir.resolve("logs");
        this.stdErrFile = logDir.resolve("stderr.log");
        this.stdOutFile = logDir.resolve("stdout.log");
    }
}
