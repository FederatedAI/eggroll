package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.context.Context;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import org.apache.commons.lang3.StringUtils;

import java.util.List;


@Singleton
public class SessionStopHandler extends AbstractSessionStateHandler{
    @Inject
    SessionMainService sessionMainService;

    @Override
    public  void asynPostHandle(Context context, ErSessionMeta data , String preStateParam, String desStateParam){
        List<ErServerNode> serverNodes = (List< ErServerNode>)context.getData(Dict.SERVER_NODES);
        serverNodes.parallelStream().forEach(serverNode -> {
            try{
                NodeManagerClient nodeManagerClient = new NodeManagerClient(serverNode.getEndpoint());
                nodeManagerClient.stopContainers(context,data);
            }catch (Exception e){
                logger.error("send stop command error",e);
            }
        });
    };

    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data , String preStateParam, String desStateParam) {
        ErSessionMeta  erSessionMeta =sessionMainService.getSession(data.getId(),true,false,false);
        if(erSessionMeta==null){
            throw new RuntimeException("");
        }
        if(StringUtils.isNotEmpty(preStateParam)&&preStateParam.equals(erSessionMeta.getStatus())){
            throw new RuntimeException("");
        }
        if(data.getActiveProcCount()!=null)
            erSessionMeta.setActiveProcCount(data.getActiveProcCount());
        return erSessionMeta;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        updateStatus(context,erSessionMeta,preStateParam,desStateParam);
        erSessionMeta.getProcessors().forEach(processor ->{
            processorStateMachine.changeStatus(context,processor,null,desStateParam );
        });
        return sessionMainService.getSession(erSessionMeta.getId(),true,false,false);
    }
}
