package com.webank.eggroll.clustermanager.dao.impl.resourcemanager;

import com.webank.eggroll.core.meta.ErProcessor;
import com.webank.eggroll.core.util.Logging;
import org.springframework.stereotype.Service;

@Service
public class ProcessorStateMachineNew_3 implements Logging {

    public void changeStatus(ErProcessor paramProcessor, String preStateParam ,String desStateParam){
        ErProcessor erProcessor = paramProcessor;
        long beginTimeStamp = System.currentTimeMillis();
        String preState = preStateParam;
        String processorType = erProcessor.processorType();
        if(preState==null){

        }
    }
}
