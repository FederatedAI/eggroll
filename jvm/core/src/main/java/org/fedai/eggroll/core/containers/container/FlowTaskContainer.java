package org.fedai.eggroll.core.containers.container;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class FlowTaskContainer extends PythonContainer {

    Logger logger = LoggerFactory.getLogger(FlowTaskContainer.class);

    public FlowTaskContainer(FlowTaskContainerBuildConfig config) {
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
    }

    public FlowTaskContainer(
            String sessionId,
            long processorId,
            Path containerWorkspace,
            List<String> commandArguments,
            Map<String, String> environmentVariables,
            Map<String, String> options,
            String scriptPath
    ) throws Exception {
        this(new FlowTaskContainerBuildConfig(
                sessionId,
                processorId,
                containerWorkspace,
                commandArguments,
                environmentVariables,
                options,
                scriptPath
        ));
    }
}
