package com.webank.eggroll.clustermanager.dao.impl.resourcemanager;

import com.eggroll.core.pojo.ErProcessor;
import org.springframework.stereotype.Service;

@Service
public class ProcessorStateMachine {

    public void changeStatus(ErProcessor paramProcessor, String preStateParam , String desStateParam){
        ErProcessor erProcessor = paramProcessor;
        long beginTimeStamp = System.currentTimeMillis();
        String preState = preStateParam;
        String processorType = erProcessor.getProcessorType();
        if(preState==null){
        }
    }
}
