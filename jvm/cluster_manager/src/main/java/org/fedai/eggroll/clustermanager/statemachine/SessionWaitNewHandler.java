package org.fedai.eggroll.clustermanager.statemachine;

import com.google.common.collect.Lists;
import com.google.inject.Singleton;
import org.apache.commons.collections.CollectionUtils;
import org.fedai.eggroll.clustermanager.entity.SessionMain;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.*;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.NodeManagerClient;
import org.fedai.eggroll.core.pojo.ErEndpoint;
import org.fedai.eggroll.core.pojo.ErProcessor;
import org.fedai.eggroll.core.pojo.ErServerNode;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

@Singleton
public class SessionWaitNewHandler extends AbstractSessionStateHandler {
    Logger logger = LoggerFactory.getLogger(SessionWaitNewHandler.class);

    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        logger.info("session create prepare {}", data);
        ErServerNode serverNode = new ErServerNode();
        serverNode.setStatus(ServerNodeStatus.HEALTHY.name());
        serverNode.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        List<ErServerNode> serverNodes = serverNodeService.getListByErServerNode(serverNode);
        logger.info("session create , health node {}", serverNodes);
        context.putData(Dict.SERVER_NODES, serverNodes);
        return data;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        logger.info("session create handle begin");
        erSessionMeta.setStatus(SessionStatus.NEW.name());
        List<ErServerNode> serverNodeList = (List<ErServerNode>) context.getData(Dict.SERVER_NODES);
        if (CollectionUtils.isEmpty(serverNodeList)) {
            throw new RuntimeException("no health server node");
        }
        List<ErProcessor> processors = Lists.newArrayList();
        Integer eggsPerNode = MetaInfo.CONFKEY_SESSION_PROCESSORS_PER_NODE;
        if (erSessionMeta.getOptions().get("eggroll.session.processors.per.node") != null) {
            eggsPerNode = Integer.parseInt(erSessionMeta.getOptions().get("eggroll.session.processors.per.node"));
        }

        logger.info("server node list {}", serverNodeList);
        logger.info("session meta {}", erSessionMeta);
        logger.info("eggsPerNode {}", eggsPerNode);
        if (!ProcessorType.DeepSpeed.name().equals(context.getData(Dict.KEY_PROCESSOR_TYPE))) {
            for (ErServerNode erServerNode : serverNodeList) {
                for (int i = 0; i < eggsPerNode; i++) {
                    ErProcessor processor = new ErProcessor();
                    processor.setServerNodeId(erServerNode.getId());
                    processor.setSessionId(erSessionMeta.getId());
                    processor.setProcessorType(context.getData(Dict.KEY_PROCESSOR_TYPE) == null ? ProcessorType.egg_pair.name() : context.getData(Dict.KEY_PROCESSOR_TYPE).toString());
                    processor.setStatus(ProcessorStatus.NEW.name());
                    processor.setCommandEndpoint(new ErEndpoint(erServerNode.getEndpoint().getHost(), 0));
                    processor.setOptions(erSessionMeta.getOptions());
                    processors.add(processor);
                }
                erSessionMeta.setProcessors(processors);
            }

        } else {
            for (ErProcessor processor : erSessionMeta.getProcessors()) {
                processor.setSessionId(erSessionMeta.getId());
            }
        }
        int activeProcCount = 0;
        SessionMain sessionMain = new SessionMain(erSessionMeta.getId(), erSessionMeta.getName(), SessionStatus.NEW.name(),
                erSessionMeta.getTag(), erSessionMeta.getProcessors().size(), activeProcCount, new Date(), new Date());
        sessionMainService.updateById(sessionMain);

        doInserSession(context, erSessionMeta);

        this.openAsynPostHandle(context);
        return erSessionMeta;
    }


    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        if (ProcessorType.DeepSpeed.name().equals(context.getData(Dict.KEY_PROCESSOR_TYPE))) {
            return;
        }
        logger.info("create session  asyn post handle begin");
        List<ErServerNode> serverNodes = (List<ErServerNode>) context.getData(Dict.SERVER_NODES);

        serverNodes.parallelStream().forEach(node -> {
            ErSessionMeta sendSession = new ErSessionMeta();
            List<ErProcessor> processors = Lists.newArrayList();
            data.getProcessors().forEach(erProcessor -> {
                if (erProcessor.getServerNodeId().equals(node.getId())) {
                    processors.add(erProcessor);
                }
            });
            sendSession.setId(data.getId());
            logger.info("==============processor {}", processors);
            sendSession.setProcessors(processors);
            NodeManagerClient nodeManagerClient = new NodeManagerClient(node.getEndpoint());
            nodeManagerClient.startContainers(context, sendSession);
        });

    }
}
