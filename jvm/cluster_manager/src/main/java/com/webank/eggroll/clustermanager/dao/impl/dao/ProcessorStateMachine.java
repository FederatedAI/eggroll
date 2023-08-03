package com.webank.eggroll.clustermanager.dao.impl.dao;


import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.exceptions.ErProcessorException;
import com.eggroll.core.pojo.ErProcessor;
import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class ProcessorStateMachine {

    @Autowired
    SessionProcessorService sessionProcessorService;

    public void changeStatus(ErProcessor paramProcessor, String preStateParam, String desStateParam) {
        ErProcessor erProcessor = paramProcessor;
        long beginTimeStamp = System.currentTimeMillis();
        String preState = preStateParam;
        String processorType = erProcessor.getProcessorType();
        if (preState == null) {
            erProcessor.setStatus(null);
            List<ErProcessor> processorsInDb = sessionProcessorService.doQueryProcessor(erProcessor);
            if (processorsInDb.size() == 0) {
                //log.error("can not found processor , {}", erProcessor);
                throw new ErProcessorException("can not found processor id " + erProcessor.getId());
            } else {
                preState = processorsInDb.get(0).getStatus();
                processorType = processorsInDb.get(0).getProcessorType();
            }
            String statusLine = preState + "_" + desStateParam;
            ErProcessor desErProcessor = new ErProcessor();
            BeanUtils.copyProperties(erProcessor, desErProcessor);
            desErProcessor.setStatus(desStateParam);
            boolean dispatchConfig = MetaInfo.EGGROLL_SESSION_USE_RESOURCE_DISPATCH;
            switch (statusLine) {
                case "_NEW":

                    break;
                case "NEW_RUNNING":
                    break;
                case "NEW_STOPPED":
                case "NEW_KILLED":
                case "NEW_ERROR":
                    break;
                case "RUNNING_FINISHED":
                case "RUNNING_STOPPED":
                case "RUNNING_KILLED":
                case "RUNNING_ERROR":
                    break;
                default:
                   // log.info("there is no need to do something with {} state {}", erProcessor.getId(), statusLine);
                    break;
            }
        }
    }


//    private ErProcessor createNewProcessor(ErProcessor erProcessor, Consumer<ErProcessor> beforeCall,
//                                           Consumer<ErProcessor> afterCall) {
//            if (beforeCall != null)
//                beforeCall.accept(erProcessor);
//        ErProcessor newProcessor = smDao.createProcessor(conn, erProcessor);
//            if (afterCall != null)
//                afterCall.accept(conn, newProcessor);
//            return newProcessor;
//    }
}

