package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.ProcessorType;
import com.eggroll.core.constant.SessionEvents;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.pojo.ErSessionMeta;

import com.google.common.collect.Lists;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import com.webank.eggroll.clustermanager.entity.SessionMain;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Date;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
@Service
public class SessionStateMachine extends AbstractStateMachine<ErSessionMeta>{

    @Autowired
    SessionMainService   sessionMainService;
    @Autowired
    ProcessorStateMechine  processorStateMechine;

    @Override
    public String getLockKey(ErSessionMeta erSessionMeta) {
        return erSessionMeta.getId();
    }




//    @Override
//    @Transactional
//    protected ErSessionMeta doChangeStatus(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
//        String statusLine = buildStateChangeLine(preStateParam,desStateParam);
//        ErSessionMeta result = null;
//        switch ( statusLine){
//
//            //PREPARE(false), NEW(false),NEW_TIMEOUT(true),ACTIVE(false),CLOSED(true),KILLED(true),ERROR(true),FINISHED(true);
//            case "_NEW":result = createSession(context,erSessionMeta,preStateParam,desStateParam) ;break;
//            case "NEW_ACTIVE" :  updateStatus(context,erSessionMeta,preStateParam,desStateParam);break;
//            case "NEW_KILLED" : ;
//            case "NEW_ERROR" : handleFailedStatus(context,erSessionMeta,preStateParam,desStateParam );break;
//            case "NEW_FINISHED" : ;break;
//            case "NEW_CLOSED" : ;break;
//            default:
//                throw  new RuntimeException();
//        }
//        statueChangeHandlerMap.put("",this.handleFailedStatus());
//
//        return  result;
//    }

    @State(value = {"NEW_KILLED","NEW_ERROR"})
    private  void handleFailedStatus(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam){
        updateStatus(context,erSessionMeta,preStateParam,desStateParam);
        erSessionMeta.getProcessors().forEach(processor ->{
            this.processorStateMechine.changeStatus(context,processor,null,desStateParam );
        });
    }


    private  void  updateStatus(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam){
        SessionMain   sessionMain =   new SessionMain();
        sessionMain.setSessionId(erSessionMeta.getId());
        sessionMain.setStatus(erSessionMeta.getStatus());
        sessionMainService.updateById(sessionMain);
    }


    @Override
    public ErSessionMeta prepare(ErSessionMeta erSessionMeta) {
        return erSessionMeta;
    }

    @State(value={"_NEW"})
    private ErSessionMeta  createSession(Context context,ErSessionMeta  erSessionMeta,String preStateParam,String desStateParam){

        ErSessionMeta   sessionInDb =  sessionMainService.getSession(erSessionMeta.getId(),true);
        if(sessionInDb!=null)
            return  sessionInDb;
        // TODO: 2023/8/3
        List<ErServerNode> serverNodeList =(List<ErServerNode>)  context.getData("");
        if(CollectionUtils.isEmpty(serverNodeList)){
            throw  new RuntimeException("xxxx");
        }
        List<ErProcessor>  processors = Lists.newArrayList();
        Integer eggsPerNode = MetaInfo.CONFKEY_SESSION_PROCESSORS_PER_NODE;
        if(erSessionMeta.getOptions().get("eggroll.session.processors.per.node")!=null){
            eggsPerNode =  Integer.parseInt(erSessionMeta.getOptions().get("eggroll.session.processors.per.node"));
        }
       for(ErServerNode  erServerNode:serverNodeList) {
                for(int i=0;i<eggsPerNode;i++){
                    ErProcessor  processor= new ErProcessor();
                    processor.setServerNodeId(erServerNode.getId());
                    processor.setSessionId(erSessionMeta.getId());
                    processor.setProcessorType(ProcessorType.EGG_PAIR.name());
                    processor.setStatus(ProcessorStatus.NEW.name());
                    processor.setCommandEndpoint(new ErEndpoint(erServerNode.getEndpoint().getHost(),0));
                    processors.add(processor);
                }
        };
       doInserSession(context,erSessionMeta);
       return  erSessionMeta;
    }
    @Transactional
    private  void  doInserSession(Context context ,ErSessionMeta erSessionMeta){
        SessionMain  sessionMain = new  SessionMain(erSessionMeta.getId(),erSessionMeta.getName(),erSessionMeta.getStatus(),
                erSessionMeta.getTag(),erSessionMeta.getTotalProcCount(),erSessionMeta.getActiveProcCount(),null,null);
        sessionMainService.save(sessionMain);
        erSessionMeta.getProcessors().forEach(p->{
            processorStateMechine.changeStatus(context,p,null,ProcessorStatus.NEW.name());
        });


    }



}
