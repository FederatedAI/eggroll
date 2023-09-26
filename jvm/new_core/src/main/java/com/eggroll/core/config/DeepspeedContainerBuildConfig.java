package com.eggroll.core.config;

import com.eggroll.core.containers.container.Container;
import com.eggroll.core.containers.container.PythonContainerRuntimeConfig;
import com.eggroll.core.containers.container.WarpedDeepspeedContainerConfig;
import com.eggroll.core.containers.container.WorkingDirectoryPreparer;
import lombok.Data;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

@Data
public class DeepspeedContainerBuildConfig {
    private String sessionId;
    private long processorId;
    private Path containerWorkspace;
    private WarpedDeepspeedContainerConfig deepspeedContainerConfig;
    private List<String> commandArguments;
    private Map<String, String> environmentVariables;
    private Map<String, byte[]> files;
    private Map<String, byte[]> zippedFiles;
    private Map<String, String> options;

    private PythonContainerRuntimeConfig conf;
    private String pythonExec;
    private byte[] runScript;
    private String scriptPath;
    private Path workingDir;
    private WorkingDirectoryPreparer workingDirectoryPreparer;
    private Map<String, String> containerEnvs;
    private Path stdErrFile;
    private Path stdOutFile;
    private Map<String, String> extraEnv;

    public DeepspeedContainerBuildConfig(String sessionId, long processorId,
                                         Path containerWorkspace,
                                         WarpedDeepspeedContainerConfig deepspeedContainerConfig,
                                         List<String> commandArguments,
                                         Map<String, String> environmentVariables,
                                         Map<String, byte[]> files,
                                         Map<String, byte[]> zippedFiles,
                                         Map<String, String> options) throws Exception{
        this.sessionId = sessionId;
        this.processorId = processorId;
        this.containerWorkspace = containerWorkspace;
        this.deepspeedContainerConfig = deepspeedContainerConfig;
        this.commandArguments = commandArguments;
        this.environmentVariables = environmentVariables;
        this.files = files;
        this.zippedFiles = zippedFiles;
        this.options = options;

        this.conf = new PythonContainerRuntimeConfig(options);
        this.pythonExec = conf.getPythonExec(Dict.DEEPSPEED_PYTHON_EXEC);

        this.runScript = ("import runpy;\n" +
                "import pprint;\n" +
                "import os;\n" +
                "import sys;\n" +
                "\n" +
                "from eggroll.deepspeed.boost import init_deepspeed;\n" +
                "\n" +
                "print(\"===========current envs==============\")\n" +
                "pprint.pprint(dict(os.environ))\n" +
                "print(\"===========current argv==============\")\n" +
                "pprint.pprint(sys.argv)\n" +
                "\n" +
                "try:\n" +
                "    init_deepspeed()\n" +
                "except Exception as e:\n" +
                "    import traceback\n" +
                "\n" +
                "    print(\"===========init deepspeed failed=============\")\n" +
                "    traceback.print_exc(file=sys.stdout)\n" +
                "    raise e\n" +
                "runpy.run_path(\"${conf.getString(ContainerKey.DEEPSPEED_SCRIPT_PATH)}\", run_name='__main__')\n" +
                "\n").getBytes();
        this.scriptPath = "_boost.py";

        this.workingDir = containerWorkspace.toAbsolutePath();

        Map<String, byte[]> updatedFiles = new HashMap<>(files);
        updatedFiles.put(scriptPath, runScript);
        this.workingDirectoryPreparer = new WorkingDirectoryPreparer();
        this.workingDirectoryPreparer.setFiles(updatedFiles);
        this.workingDirectoryPreparer.setZippedFiles(zippedFiles);
        this.workingDirectoryPreparer.setWorkingDir(workingDir);
        this.containerEnvs = workingDirectoryPreparer.getContainerDirEnv();

        Path logDir = workingDir.resolve("logs");
        this.stdErrFile = logDir.resolve("stderr.log");
        this.stdOutFile = logDir.resolve("stdout.log");

        Map<String, String> mutableEnv = new HashMap<>();
        mutableEnv.putAll(environmentVariables);

        for (Map.Entry<String, String> entry : deepspeedContainerConfig.getPytorchDistributedEnvironments().entrySet()) {
            mutableEnv.put(entry.getKey(), entry.getValue());
        }
        for (Map.Entry<String, String> entry : deepspeedContainerConfig.getEggrollCustomizedEnvironments().entrySet()) {
            mutableEnv.put(entry.getKey(), entry.getValue());
        }
        mutableEnv.putAll(containerEnvs);

        String eggrollHome = System.getenv("EGGROLL_HOME");
        if (eggrollHome != null) {
            mutableEnv.put("EGGROLL_HOME", eggrollHome);
            if (System.getenv("PYTHONPATH") != null) {
                mutableEnv.put("PYTHONPATH", System.getenv("PYTHONPATH") + ":" + eggrollHome + "/python");
            } else {
                mutableEnv.put("PYTHONPATH", eggrollHome + "/python");
            }
        } else {
            throw new Exception("EGGROLL_HOME not set");
        }

        this.extraEnv = Collections.unmodifiableMap(mutableEnv);
    }
}