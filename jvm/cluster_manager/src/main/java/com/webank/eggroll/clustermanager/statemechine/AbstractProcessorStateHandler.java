package com.webank.eggroll.clustermanager.statemechine;

import com.baomidou.mybatisplus.core.conditions.update.LambdaUpdateWrapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.clustermanager.dao.impl.ProcessorService;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;
import org.springframework.beans.factory.annotation.Autowired;

public abstract class  AbstractProcessorStateHandler  implements   StateHandler<ErProcessor>{
    @Autowired
    ResourceStateMechine  resourceStateMechine;
    @Autowired
    ProcessorService processorService;


    @Override
    public ErProcessor prepare(Context context, ErProcessor data, String preStateParam, String desStateParam) {
        if( context.getData(Dict.PROCESSOR_IN_DB)!=null){
            return (ErProcessor) context.getData(Dict.PROCESSOR_IN_DB);
        }else{
            SessionProcessor  sessionProcessor =   (SessionProcessor) this.processorService.getById(data.getId());
            if(sessionProcessor==null){
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
        processorService.update(new LambdaUpdateWrapper<SessionProcessor>().set(SessionProcessor::getStatus,desStateParam).eq(SessionProcessor::getProcessorId,data.getId()));
    }








}
