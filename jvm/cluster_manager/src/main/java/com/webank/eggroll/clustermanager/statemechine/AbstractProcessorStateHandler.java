package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErSessionMeta;
import com.webank.eggroll.clustermanager.entity.SessionProcessor;

public abstract class  AbstractProcessorStateHandler  implements   StateHandler<ErProcessor>{
    protected   boolean  checkNeedChangeResource(ErProcessor erProcessor){
        if(MetaInfo.EGGROLL_SESSION_USE_RESOURCE_DISPATCH||"DeepSpeed".equals(erProcessor.getProcessorType())) {
            return true;
        }
        return  true;
    }





}
