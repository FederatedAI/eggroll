package org.fedai.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.google.inject.Inject;
import org.fedai.eggroll.clustermanager.dao.impl.SessionProcessorService;
import org.fedai.eggroll.clustermanager.entity.SessionProcessor;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.pojo.ErProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public abstract class AbstractProcessorStateHandler implements StateHandler<ErProcessor> {

    Logger logger = LoggerFactory.getLogger(AbstractProcessorStateHandler.class);

    @Inject
    ResourceStateMechine resourceStateMechine;

    @Inject
    SessionProcessorService sessionProcessorService;


    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        if (context.getData(Dict.PROCESSOR_IN_DB) != null) {
            return (ErProcessor) context.getData(Dict.PROCESSOR_IN_DB);
        } else {
            SessionProcessor sessionProcessor = sessionProcessorService.getById(data.getId());
            if (sessionProcessor != null) {
                return sessionProcessor.toErProcessor();
            } else {
                logger.error("processor {} is not found", data.getId());
                return data;
            }
        }
    }

    protected boolean checkNeedChangeResource(ErProcessor erProcessor) {
        if (MetaInfo.EGGROLL_SESSION_USE_RESOURCE_DISPATCH || "DeepSpeed".equals(erProcessor.getProcessorType())) {
            return true;
        }
        return false;
    }


    protected void updateState(ErProcessor data, String desStateParam) {
        LambdaUpdateWrapper<SessionProcessor> lambdaUpdateWrapper = new LambdaUpdateWrapper<SessionProcessor>()
                .set(SessionProcessor::getStatus, desStateParam)
                .eq(SessionProcessor::getProcessorId, data.getId());
        if (data.getCommandEndpoint() != null) {
            lambdaUpdateWrapper.set(SessionProcessor::getCommandEndpoint, data.getCommandEndpoint().toString());
        }
        if (data.getPid() != null && data.getPid() > 0) {
            lambdaUpdateWrapper.set(SessionProcessor::getPid, data.getPid());
        }
        if (data.getTransferEndpoint() != null) {
            lambdaUpdateWrapper.set(SessionProcessor::getTransferEndpoint, data.getTransferEndpoint().toString());
        }
        if (data.getBeforeStatus() != null) {
            lambdaUpdateWrapper.set(SessionProcessor::getBeforeStatus, data.getBeforeStatus());
        }

        sessionProcessorService.update(lambdaUpdateWrapper);
    }


}
