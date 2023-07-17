package com.webank.eggroll.clustermanager.dao.impl.dao;

import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.dao.impl.SessionOptionService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import com.webank.eggroll.clustermanager.entity.SessionOption;
import com.webank.eggroll.clustermanager.entity.scala.ErProcessor_JAVA;
import com.webank.eggroll.clustermanager.entity.scala.ErSessionMeta_JAVA;
import com.webank.eggroll.core.constant.ProcessorStatus;
import com.webank.eggroll.core.resourcemanager.ProcessorStateMachine;
import com.webank.eggroll.core.util.Logging;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@Service
public class SessionMetaDaoNew_2 implements Logging {

    @Autowired
    SessionMainService sessionMainService;

    @Autowired
    SessionOptionService sessionOptionService;

    @Autowired
    ProcessorStateMachine_JAVA processorStateMachine_java;
    
    @Transactional
    public void registerWithResourceV2(ErSessionMeta_JAVA sessionMeta){
        String sid = sessionMeta.getId();
        SessionMain sessionMain = new SessionMain();
        sessionMain.setSessionId(sid);
        sessionMain.setName(sessionMeta.getName());
        sessionMain.setStatus(sessionMeta.getStatus());
        sessionMain.setTag(sessionMeta.getTag());
        sessionMain.setTotalProcCount(sessionMeta.getTotalProcCount());
        sessionMain.setActiveProcCount(sessionMeta.getActiveProcCount());
        sessionMainService.save(sessionMain);
        Map<String, String> opts = sessionMeta.getOptions();
        List<SessionOption> optsList = new ArrayList<>();
        if(!opts.isEmpty()){
            opts.forEach((k,v)->{
                SessionOption sessionOption = new SessionOption();
                sessionOption.setSessionId(sid);
                sessionOption.setName(k);
                sessionOption.setData(v);
                optsList.add(sessionOption);
            });
        }
        sessionOptionService.saveBatch(optsList);
        List<ErProcessor_JAVA> procs = sessionMeta.getProcessors();
        if(!procs.isEmpty()){
            for (ErProcessor_JAVA proc : procs) {
                proc.setSessionId(sid);
                processorStateMachine_java.changeStatus(proc,"", ProcessorStatus.NEW());
            }
        }

    }
}
