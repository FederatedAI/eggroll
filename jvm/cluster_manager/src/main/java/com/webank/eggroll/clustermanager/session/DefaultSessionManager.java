package com.webank.eggroll.clustermanager.session;

import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ServerNodeStatus;
import com.eggroll.core.constant.ServerNodeTypes;
import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.context.Context;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.common.collect.Lists;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;


import com.webank.eggroll.clustermanager.statemechine.SessionStateMachine;


import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.util.List;

@Service
public class DefaultSessionManager implements SessionManager{

    Logger logger = LoggerFactory.getLogger(DefaultSessionManager.class);
    @Autowired
    SessionMainService sessionService;
    @Autowired
    SessionStateMachine  sessionStateMachine;

    @Autowired
    ServerNodeService  serverNodeService;



    @Override
    public com.eggroll.core.pojo.ErProcessor heartbeat(Context context, com.eggroll.core.pojo.ErProcessor proc) {
        return null;
    }

    @Override
    public ErSessionMeta getSessionMain(String sessionId) {
        return null;
    }

    @Override
    public ErSessionMeta getOrCreateSession(Context context,ErSessionMeta sessionMeta) {
        System.err.println("getOrCreateSession =====");
        if(MetaInfo.EGGROLL_SESSION_USE_RESOURCE_DISPATCH){

        }else{
            getOrCreateSessionWithoutResourceDispath(context,sessionMeta);
        }

        return  sessionMeta;

    }

    private   ErSessionMeta  getOrCreateSessionWithoutResourceDispath(Context context,ErSessionMeta  sessionMeta){
      String sessionId = sessionMeta.getId();
      List<ErServerNode>   serverNodes =  getHealthServerNode();
      if(serverNodes.size()==0){
          throw  new RuntimeException("");
      }
      sessionStateMachine.changeStatus(context,sessionMeta,null,SessionStatus.NEW.name());
      ErSessionMeta  newSession =   sessionService.getSession(sessionMeta.getId(),true);
      serverNodes.parallelStream().forEach(node->{
            ErSessionMeta  sendSession =new ErSessionMeta();
            try {
                BeanUtils.copyProperties(newSession, sendSession);
                NodeManagerClient nodeManagerClient = new  NodeManagerClient(node.getEndpoint());
                sendSession.getOptions().put("eggroll.resourcemanager.server.node.id",Long.toString(node.getId()));
                nodeManagerClient.startContainers(sendSession);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch (InvocationTargetException e) {
                e.printStackTrace();
            }
        });
        long startTimeout = System.currentTimeMillis() + MetaInfo.EGGROLL_SESSION_START_TIMEOUT_MS;
        boolean isStarted = false;

            while (System.currentTimeMillis() <= startTimeout) {
                 ErSessionMeta cur = this.sessionService.getSession(sessionId,false);
                if (cur.getActiveProcCount() < cur.getTotalProcCount()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    isStarted = true;
                    break;
                }
            }




        return  null;

    }

    private List<ErServerNode> getHealthServerNode(){

        ErServerNode  serverNodeExample = new ErServerNode();
        serverNodeExample.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        serverNodeExample.setStatus(ServerNodeStatus.HEALTHY.name());

        int tryCount = 0;
        do{
            List<ErServerNode> healthyServerNodes= serverNodeService.doGetServerNodes(serverNodeExample);
            tryCount+=1;
            if(healthyServerNodes.size()==0){
                logger.info("cluster is not ready,waitting next try");
                try {
                    Thread.sleep(MetaInfo.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                return  healthyServerNodes;
            }
        }
        while(tryCount < 2);

        return Lists.newArrayList();
    }


    @Override
    public ErSessionMeta getSession(Context context,ErSessionMeta sessionMeta) {

        //logDebug(s"SESSION getSession: ${sessionMeta}")
        ErSessionMeta result =null;
        //val startTimeout = System.currentTimeMillis() + SessionConfKeys.EGGROLL_SESSION_START_TIMEOUT_MS.get().toLong
        long  startTimeout = System.currentTimeMillis()+ 20000;
        while (System.currentTimeMillis() <= startTimeout) {
           result = sessionService.getSession(sessionMeta.getId(),true);
                if (result != null && !result.getStatus().equals(SessionStatus.NEW.name()) && !StringUtils.isBlank(result.getId())) {
                    break;
                } else {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }

        return  result;
    }

    @Override
    public ErSessionMeta registerSession(Context context,ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta stopSession(Context context,ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta killSession(Context context,ErSessionMeta sessionMeta) {
        return null;
    }

    @Override
    public ErSessionMeta killSession(Context context,ErSessionMeta sessionMeta, String afterState) {

        ErSessionMeta erSessionMeta = sessionService.getSession(sessionMeta.getId(),true);
        if(erSessionMeta==null){
            return null;
        }






//        def killSession(sessionMeta: ErSessionMeta, afterState: String): ErSessionMeta = {
//                val sessionId = sessionMeta.id
//        if (!smDao.existSession(sessionId)) {
//            return null
//        }
//        val dbSessionMeta = smDao.getSession(sessionId)
//        if (StringUtils.equalsAny(dbSessionMeta.status, SessionStatus.KILLED, SessionStatus.CLOSED, SessionStatus.ERROR)) {
//            return dbSessionMeta
//        }
//        if(dbSessionMeta.processors.length>0) {
//            val sessionHosts = dbSessionMeta.processors.filter(p=>p.commandEndpoint!=null&&StringUtils.isNotEmpty(p.commandEndpoint.host))
//        .map(p => p.commandEndpoint.host).toSet
//            if(!sessionHosts.isEmpty) {
//                val serverNodeCrudOperator = new ServerNodeCrudOperator()
//                val sessionServerNodes = serverNodeCrudOperator.getServerClusterByHosts(sessionHosts.toList.asJava).serverNodes
//
//                sessionServerNodes.par.foreach(n => {
//                        // TODO:1: add new params?
//                        val newSessionMeta = dbSessionMeta.copy(
//                        options = dbSessionMeta.options ++ Map(ResourceManagerConfKeys.SERVER_NODE_ID -> n.id.toString))
//                val nodeManagerClient = new NodeManagerClient(
//                        ErEndpoint(host = n.endpoint.host,
//                                port = n.endpoint.port))
//                nodeManagerClient.killContainers(newSessionMeta)
//        })
//            }
//        }
//
//        // todo:1: update selective
//        smDao.updateSessionMain(dbSessionMeta.copy(activeProcCount = 0, status = afterState),afterCall=ProcessorStateMachine.defaultSessionCallback)
//        var  resultSession = getSession(dbSessionMeta)
//        resultSession
//    }




        return null;
    }

    @Override
    public ErSessionMeta killAllSessions(Context context,ErSessionMeta sessionMeta) {
        return null;
    }




}
