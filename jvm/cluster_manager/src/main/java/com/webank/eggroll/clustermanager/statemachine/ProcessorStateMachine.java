package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class ProcessorStateMachine extends AbstractStateMachine<ErProcessor> {
    Logger logger = LoggerFactory.getLogger(ProcessorStateMachine.class);
    @Inject
    SessionProcessorService sessionProcessorService;
    @Inject
    ResourceStateMechine resourceStateMechine;
    @Inject
    ProcessorStateRunningHandler processorStateRunningHandler;
    @Inject
    ProcessorStatusRunningStopHandler processorStatusRunningStopHandler;
    @Inject
    ProcessorStateNewStopHandler processorStateNewStopHandler;
    @Inject
    ProcessorCreateHandler processorCreateHandler;
    @Inject
    ProcessorStateInvalidHandler processorStateInvalidHandler;
    @Inject
    ProcessorStateIgnoreHandler processorStateIgnoreHandler;

    @Override
    String buildStateChangeLine(Context context, ErProcessor erProcessor, String preStateParam, String desStateParam) {
        String line = "";
        SessionProcessor processorInDb = sessionProcessorService.getById(erProcessor.getId());
        if (processorInDb != null) {
            erProcessor.setProcessorType(processorInDb.getProcessorType());
            context.putData(Dict.PROCESSOR_IN_DB, processorInDb.toErProcessor());
        }
        if (StringUtils.isEmpty(preStateParam)) {
            if (processorInDb == null) {
                preStateParam = "";
            } else {
                preStateParam = processorInDb.getStatus();
            }
        }
        line = preStateParam + "_" + desStateParam;
        context.putLogData("processor_status", line);
        logger.info("processor {} prepare to change status {}", erProcessor.getId(), line);
        erProcessor.setBeforeStatus(preStateParam);
        return line;
    }

    @Override
    public String getLockKey(Context context, ErProcessor erProcessor) {
        String prefix = "PK_";
        ErProcessor erInDb = (ErProcessor) context.getData(Dict.PROCESSOR_IN_DB);
        if (erInDb != null) {
            return prefix + erInDb.getSessionId();
        }
        return prefix + Long.toString(erProcessor.getId());
    }


    @Inject
    public void afterPropertiesSet() throws Exception {
        this.registeStateHander("_NEW", processorCreateHandler);

        this.registeStateHander("NEW_RUNNING", processorStateRunningHandler);
        this.registeStateHander("RUNNING_FINISHED", processorStatusRunningStopHandler);
        this.registeStateHander("RUNNING_STOPPED", processorStatusRunningStopHandler);
        this.registeStateHander("RUNNING_KILLED", processorStatusRunningStopHandler);
        this.registeStateHander("RUNNING_ERROR", processorStatusRunningStopHandler);
        this.registeStateHander("NEW_FINISHED", processorStateNewStopHandler);
        this.registeStateHander("NEW_STOPPED", processorStateNewStopHandler);
        this.registeStateHander("NEW_KILLED", processorStateNewStopHandler);
        this.registeStateHander("NEW_ERROR", processorStateNewStopHandler);

        this.registeStateHander("KILLED_RUNNING", processorStateInvalidHandler);
        this.registeStateHander("STOPPED_RUNNING", processorStateInvalidHandler);
        this.registeStateHander("ERROR_RUNNING", processorStateInvalidHandler);
        this.registeStateHander("RUNNING_RUNNING", processorStateIgnoreHandler);
        this.registeStateHander("_RUNNING", processorStateInvalidHandler);
        this.registeStateHander(IGNORE, processorStateIgnoreHandler);
    }

}



