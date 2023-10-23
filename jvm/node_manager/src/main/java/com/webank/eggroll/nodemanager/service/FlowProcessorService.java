package com.webank.eggroll.nodemanager.service;

import com.eggroll.core.containers.container.ContainerTrait;
import com.eggroll.core.containers.container.WorkingDirectoryPreparer;
import com.google.inject.Singleton;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Singleton
public class FlowProcessorService implements ContainerTrait {

    Logger logger = LoggerFactory.getLogger(FlowProcessorService.class);

    private Process process;
    private String scriptPath;
    private List<String> command;
    private Path cwd;
    private Map<String, String> extraEnv;
    private List<String> scriptArgs;
    private Path stdOutFile;
    private Path stdErrFile;
    private Path workingDir;
    private long processorId;

    public FlowProcessorService(String scriptPath, Path cwd, Map<String, String> extraEnv, List<String> scriptArgs,long processorId) {
        // py脚本路径
        this.scriptPath = scriptPath;
        this.command = buildCommand("python", scriptPath, scriptArgs);
        this.cwd = cwd;
        // 用户定义的环境变量
        this.extraEnv = extraEnv;
        // 用户自定义脚本参数
        this.scriptArgs = scriptArgs;
        this.workingDir = cwd.toAbsolutePath();
        Path logDir = workingDir.resolve("logs");
        this.stdErrFile = logDir.resolve("stderr.log");
        this.stdOutFile = logDir.resolve("stdout.log");
        this.processorId = processorId;

        // 用户设定的环境变量
        Map<String, String> mutableEnv = new HashMap<>();
        mutableEnv.putAll(extraEnv);
    }

    private static List<String> buildCommand(String pythonExec, String scriptPath, List<String> scriptArgs) {
        List<String> command = new ArrayList<>();
        command.add(pythonExec);
        command.add("-u");
        command.add(scriptPath);
        command.addAll(scriptArgs);
        return command;
    }

    @Override
    public boolean start() {
        Boolean output = null;
        try {
            logger.info("ProcessContainer start command : {}", command);
            ProcessBuilder processBuilder = new ProcessBuilder(command);
            processBuilder.directory(cwd.toFile());
            if (stdOutFile != null) {
                File logFile = cwd.resolve(stdOutFile).toFile();
                logFile.getParentFile().mkdirs();
                if (!logFile.exists()) {
                    logFile.createNewFile();
                }
                processBuilder.redirectOutput(logFile);
            }
            if (stdOutFile != null) {
                File logFile = cwd.resolve(stdOutFile).toFile();
                logFile.getParentFile().mkdirs();
                if (!logFile.exists()) {
                    logFile.createNewFile();
                }
                processBuilder.redirectError(logFile);
            }

            Map<String, String> environment = processBuilder.environment();
            extraEnv.forEach(environment::put);
            process = processBuilder.start();
            output = process.isAlive();
        } catch (Exception e) {
            logger.error("start processContainer error : ", e);
        }
        return output;
    }

    @Override
    public boolean stop() {
        if (process.isAlive()) {
            process.destroy();
        }
        return process.isAlive();
    }

    @Override
    public boolean kill() {
        if (process.isAlive()) {
            process.destroyForcibly();
        }
        return process.isAlive();
    }

    @Override
    public int waitForCompletion() {
        try {
            process.waitFor();
            return process.exitValue();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return -1;
        }
    }

    public int getPid() {
        try {
            Field pidField = process.getClass().getDeclaredField("pid");
            pidField.setAccessible(true);
            return pidField.getInt(process);
        } catch (NoSuchFieldException | IllegalAccessException e) {
            e.printStackTrace();
            return -1;
        }
    }

    @Override
    public long getProcessorId() {
        return processorId;
    }

    // 查看一个进程的状态
    public void status() {

    }

    @Override
    public String toString() {
        return "FlowProcessorService{" +
                "process=" + process +
                ", scriptPath='" + scriptPath + '\'' +
                ", command=" + command +
                ", cwd=" + cwd +
                ", extraEnv=" + extraEnv +
                ", scriptArgs=" + scriptArgs +
                ", stdOutFile=" + stdOutFile +
                ", stdErrFile=" + stdErrFile +
                ", workingDir=" + workingDir +
                '}';
    }
}
