//package com.webank.eggroll.clustermanager.dao.impl.dao;
//
//import com.eggroll.core.config.Dict;
//import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
//import com.webank.eggroll.clustermanager.dao.impl.SessionOptionService;
//import com.webank.eggroll.clustermanager.entity.SessionMain;
//import com.webank.eggroll.clustermanager.entity.SessionOption;
//import com.eggroll.core.pojo.ErProcessor;
//import com.eggroll.core.pojo.ErSessionMeta;
//import com.webank.eggroll.clustermanager.statemachine.ProcessorStateMachine;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.util.ArrayList;
//import java.util.List;
//import java.util.Map;
//
//@Service
//public class SessionMetaDao {
//
//    @Autowired
//    SessionMainService sessionMainService;
//
//    @Autowired
//    SessionOptionService sessionOptionService;
//
//    @Autowired
//    ProcessorStateMachine processorStateMachine;
//
//    @Transactional
//    public void registerWithResource(ErSessionMeta sessionMeta){
//        String sid = sessionMeta.getId();
//        SessionMain sessionMain = new SessionMain();
//        sessionMain.setSessionId(sid);
//        sessionMain.setName(sessionMeta.getName());
//        sessionMain.setStatus(sessionMeta.getStatus());
//        sessionMain.setTag(sessionMeta.getTag());
//        sessionMain.setTotalProcCount(sessionMeta.getTotalProcCount());
//        sessionMain.setActiveProcCount(sessionMeta.getActiveProcCount());
//        sessionMainService.save(sessionMain);
//        Map<String, String> opts = sessionMeta.getOptions();
//        List<SessionOption> optsList = new ArrayList<>();
//        if(!opts.isEmpty()){
//            opts.forEach((k,v)->{
//                SessionOption sessionOption = new SessionOption();
//                sessionOption.setSessionId(sid);
//                sessionOption.setName(k);
//                sessionOption.setData(v);
//                optsList.add(sessionOption);
//            });
//        }
//        sessionOptionService.saveBatch(optsList);
//        List<ErProcessor> procs = sessionMeta.getProcessors();
//        if(!procs.isEmpty()){
//            for (ErProcessor proc : procs) {
//                proc.setSessionId(sid);
//                processorStateMachine.changeStatus(proc,"", Dict.NEW);
//            }
//        }
//
//    }
//}
