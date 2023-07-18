package com.webank.eggroll.clustermanager.dao.impl.dao;


import com.webank.eggroll.clustermanager.dao.impl.SessionProcessorService;
import com.webank.eggroll.clustermanager.entity.scala.ErProcessor_JAVA;
import com.webank.eggroll.clustermanager.entity.scala.StaticErConf_JAVA;
import com.webank.eggroll.core.exceptions.ErProcessorException_JAVA;
import com.webank.eggroll.core.constant.SessionConfKeys;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

@Slf4j
@Service
public class ProcessorStateMachine_JAVA {

    @Autowired
    SessionProcessorService sessionProcessorService;

    public void changeStatus(ErProcessor_JAVA paramProcessor, String preStateParam, String desStateParam) {
        ErProcessor_JAVA erProcessor = paramProcessor;
        long beginTimeStamp = System.currentTimeMillis();
        String preState = preStateParam;
        String processorType = erProcessor.getProcessorType();
        if (preState == null) {
            erProcessor.setStatus(null);
            List<ErProcessor_JAVA> processorsInDb = sessionProcessorService.doQueryProcessor(erProcessor);
            if (processorsInDb.size() == 0) {
                log.error("can not found processor , {}", erProcessor);
                throw new ErProcessorException_JAVA("can not found processor id " + erProcessor.getId());
            } else {
                preState = processorsInDb.get(0).getStatus();
                processorType = processorsInDb.get(0).getProcessorType();
            }
            String statusLine = preState + "_" + desStateParam;
            ErProcessor_JAVA desErProcessor = new ErProcessor_JAVA();
            BeanUtils.copyProperties(erProcessor,desErProcessor);
            desErProcessor.setStatus(desStateParam);
            String dispatchConfig = StaticErConf_JAVA.getProperty(SessionConfKeys.EGGROLL_SESSION_USE_RESOURCE_DISPATCH(), "false");

        }
    }
}
