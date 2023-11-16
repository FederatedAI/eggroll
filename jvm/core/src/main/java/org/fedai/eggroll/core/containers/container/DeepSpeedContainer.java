package org.fedai.eggroll.core.containers.container;

import org.fedai.eggroll.core.config.DeepspeedContainerBuildConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class DeepSpeedContainer extends PythonContainer {

    Logger logger = LoggerFactory.getLogger(DeepSpeedContainer.class);

    private DeepspeedContainerBuildConfig config;


    public DeepSpeedContainer(DeepspeedContainerBuildConfig config) {
        super(
                config.getPythonExec(),
                config.getScriptPath(),
                config.getWorkingDir(),
                config.getCommandArguments(),
                config.getExtraEnv(),
                config.getStdErrFile(),
                config.getStdOutFile(),
                config.getWorkingDirectoryPreparer(),
                config.getProcessorId()

        );
        this.config = config;
    }

    public DeepSpeedContainer(
            String sessionId,
            long processorId,
            WarpedDeepspeedContainerConfig deepspeedContainerConfig,
            Path containerWorkspace,
            List<String> commandArguments,
            Map<String, String> environmentVariables,
            Map<String, byte[]> files,
            Map<String, byte[]> zippedFiles,
            Map<String, String> options
    ) throws Exception {
        this(new DeepspeedContainerBuildConfig(
                sessionId,
                processorId,
                containerWorkspace,
                deepspeedContainerConfig,
                commandArguments,
                environmentVariables,
                files,
                zippedFiles,
                options
        ));
    }

    @Override
    public void preStart() {
        super.preStart();
        logger.info("prepare DeepSpeedContainer start: " + config.toString());
    }
}
