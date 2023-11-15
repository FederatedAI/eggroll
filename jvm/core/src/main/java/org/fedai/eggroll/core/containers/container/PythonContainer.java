package org.fedai.eggroll.core.containers.container;


import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class PythonContainer extends ProcessContainer {
    private String pythonExec;
    private String scriptPath;
    private Path cwd;
    private List<String> scriptArgs;
    private Map<String, String> extraEnv;
    private Path stdErrFile;
    private Path stdOutFile;
    private WorkingDirectoryPreparer workingDirectoryPreparer;
    private long processorId;

    public PythonContainer(String pythonExec, String scriptPath, Path cwd, List<String> scriptArgs,
                           Map<String, String> extraEnv, Path stdErrFile, Path stdOutFile,
                           WorkingDirectoryPreparer workingDirectoryPreparer, long processorId) {
        super(buildCommand(pythonExec, scriptPath, scriptArgs), cwd, extraEnv, stdOutFile, stdErrFile,
                workingDirectoryPreparer, processorId);

        this.pythonExec = pythonExec;
        this.scriptPath = scriptPath;
        this.cwd = cwd;
        this.scriptArgs = scriptArgs;
        this.extraEnv = extraEnv;
        this.stdErrFile = stdErrFile;
        this.stdOutFile = stdOutFile;
        this.workingDirectoryPreparer = workingDirectoryPreparer;
        this.processorId = processorId;
    }

    private static List<String> buildCommand(String pythonExec, String scriptPath, List<String> scriptArgs) {
        List<String> command = new ArrayList<>();
        command.add(pythonExec);
        command.add("-u");
        command.add(scriptPath);
        command.addAll(scriptArgs);
        return command;
    }
}