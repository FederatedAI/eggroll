package com.webank.eggroll.clustermanager.statemachine;

import com.eggroll.core.config.Dict;
import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.constant.ProcessorStatus;
import com.eggroll.core.constant.ProcessorType;
import com.eggroll.core.constant.ServerNodeStatus;
import com.eggroll.core.constant.ServerNodeTypes;
import com.eggroll.core.context.Context;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.ErEndpoint;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.pojo.ErSessionMeta;
import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

@Singleton
public class SessionCreateHandler  extends AbstractSessionStateHandler{
    Logger logger = LoggerFactory.getLogger(SessionCreateHandler.class);
    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data , String preStateParam, String desStateParam) {
        logger.info("session create prepare {}",data);
        ErServerNode   serverNode = new  ErServerNode();
        serverNode.setStatus(ServerNodeStatus.HEALTHY.name());
        serverNode.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        List<ErServerNode>  serverNodes = serverNodeService.getListByErServerNode(serverNode);
//        System.err.println("xxxxxxxxxxx"+serverNodes);
        logger.info("session create , health node {}",serverNodes);
        context.putData(Dict.SERVER_NODES,serverNodes);
        return data;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        logger.info("session create handle begin");
        ErSessionMeta   sessionInDb =  sessionMainService.getSession(erSessionMeta.getId(),true,true,false);
        if(sessionInDb!=null)
            return  sessionInDb;
        // TODO: 2023/8/3
        List<ErServerNode> serverNodeList =(List<ErServerNode>)  context.getData(Dict.SERVER_NODES);
        if(CollectionUtils.isEmpty(serverNodeList)){
            throw  new RuntimeException("no health server node");
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
            erSessionMeta.setProcessors(processors);
        };
        doInserSession(context,erSessionMeta);

        this.openAsynPostHandle(context);
        return  erSessionMeta;
    }


    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        logger.info("create session  asyn post handle begin");
       List<ErServerNode> serverNodes = (List<ErServerNode>)context.getData(Dict.SERVER_NODES);
        serverNodes.parallelStream().forEach(node->{
            ErSessionMeta  sendSession =new ErSessionMeta();

                //BeanUtils.copyProperties(data, sendSession);
                List<ErProcessor>  processors = Lists.newArrayList();
                data.getProcessors().forEach(erProcessor -> {
                    if(erProcessor.getServerNodeId()==node.getId())
                        processors.add(erProcessor);
                });
                sendSession.setId(data.getId());
                sendSession.setProcessors(processors);
                NodeManagerClient nodeManagerClient = new  NodeManagerClient(node.getEndpoint());
                //sendSession.getOptions().put("eggroll.resourcemanager.server.node.id",Long.toString(node.getId()));
                nodeManagerClient.startContainers(context,sendSession);

        });

    }
}
