package com.eggroll.core.containers.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.lang.InterruptedException;
import java.lang.reflect.Field;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class ProcessContainer implements ContainerTrait {

    Logger logger = LoggerFactory.getLogger(ProcessContainer.class);
    private Process process;
    private final List<String> command;
    private final Path cwd;
    private final Map<String, String> extraEnv;
    private final Path stdOutFile;
    private final Path stdErrFile;
    private final WorkingDirectoryPreparer workingDirectoryPreparer;
    private final long processorId;

    public ProcessContainer(List<String> command, Path cwd, Map<String, String> extraEnv, Path stdOutFile,
                            Path stdErrFile, WorkingDirectoryPreparer workingDirectoryPreparer,
                            long processorId) {
        this.command = command;
        this.cwd = cwd;
        this.extraEnv = extraEnv;
        this.stdOutFile = stdOutFile;
        this.stdErrFile = stdErrFile;
        this.workingDirectoryPreparer = workingDirectoryPreparer;
        this.processorId = processorId;
        if (this.workingDirectoryPreparer != null) {
            workingDirectoryPreparer.setWorkingDir(cwd);
        }
    }

    public void preStart() {
    }

    public void postStart() {
    }

    public boolean start() {
        preStart();
        Boolean output = null;
        try {
            workingDirectoryPreparer.prepare();
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
        } finally {
            try {
                workingDirectoryPreparer.cleanup();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        postStart();
        return output;
    }


    public int waitForCompletion() {
        try {
            process.waitFor();
            return process.exitValue();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return -1;
        }
    }


    public boolean stop() {
        if (process.isAlive()) {
            process.destroy();
        }
        return process.isAlive();
    }

    public boolean kill() {
        if (process.isAlive()) {
            process.destroyForcibly();
        }
        return process.isAlive();
    }

    @Override
    public String toString() {
        return "ProcessContainer{" +
                "command=" + command +
                ", cwd=" + cwd +
                ", extraEnv=" + extraEnv +
                ", stdOutFile=" + stdOutFile +
                ", stdErrFile=" + stdErrFile +
                ", workingDirectoryPreparer=" + workingDirectoryPreparer +
                '}';
    }

    @Override
    public long getProcessorId() {
        return processorId;
    }

    @Override
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
}
