package com.webank.eggroll.clustermanager.statemachine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.google.inject.Inject;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.apache.commons.lang3.StringUtils;


public abstract class  AbstractProcessorStateHandler  implements   StateHandler<ErProcessor>{

    @Inject
    ResourceStateMechine  resourceStateMechine;

    @Inject
    ProcessorService processorService;


    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        if( context.getData(Dict.PROCESSOR_IN_DB)!=null){
            return (ErProcessor) context.getData(Dict.PROCESSOR_IN_DB);
        }else{
            SessionProcessor  sessionProcessor = this.processorService.getById(data.getId());
            if(sessionProcessor!=null){
                return sessionProcessor.toErProcessor();
            }else
            {
                throw  new RuntimeException("");
            }
        }
    }

    protected   boolean  checkNeedChangeResource(ErProcessor erProcessor){
        if(MetaInfo.EGGROLL_SESSION_USE_RESOURCE_DISPATCH||"DeepSpeed".equals(erProcessor.getProcessorType())) {
            return true;
        }
        return  true;
    }


    protected   void updateState(ErProcessor  data,String   desStateParam){
        LambdaUpdateWrapper<SessionProcessor> lambdaUpdateWrapper =  new LambdaUpdateWrapper<SessionProcessor>()
                .set(SessionProcessor::getStatus,desStateParam)
                .eq(SessionProcessor::getProcessorId,data.getId());
        if(data.getCommandEndpoint()!=null) {
            lambdaUpdateWrapper.set(SessionProcessor::getCommandEndpoint, data.getCommandEndpoint().toString());

        }

        if(data.getTransferEndpoint()!=null) {
            lambdaUpdateWrapper.set(SessionProcessor::getTransferEndpoint, data.getTransferEndpoint().toString());

        }

        processorService.update(lambdaUpdateWrapper);
    }








}
