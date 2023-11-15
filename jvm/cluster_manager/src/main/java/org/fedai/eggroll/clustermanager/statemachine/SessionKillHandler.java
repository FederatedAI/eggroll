package org.fedai.eggroll.clustermanager.statemachine;

import org.fedai.eggroll.clustermanager.dao.impl.SessionMainService;
import org.fedai.eggroll.core.constant.ProcessorStatus;
import org.fedai.eggroll.core.constant.ServerNodeStatus;
import org.fedai.eggroll.core.constant.ServerNodeTypes;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.NodeManagerClient;
import org.fedai.eggroll.core.pojo.ErServerNode;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.List;

@Singleton
public class SessionKillHandler extends AbstractSessionStateHandler {
    Logger logger = LoggerFactory.getLogger(SessionKillHandler.class);

    @Inject
    SessionMainService sessionMainService;

    @Inject
    ServerNodeService serverNodeService;

    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        ErServerNode erServerNodeExample = new ErServerNode();
        erServerNodeExample.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        erServerNodeExample.setStatus(ServerNodeStatus.HEALTHY.name());
        List<ErServerNode> serverNodes = serverNodeService.getListByErServerNode(erServerNodeExample);
        logger.info("==============servernodes {}", serverNodes);
        serverNodes.parallelStream().forEach(serverNode -> {
            try {
                NodeManagerClient nodeManagerClient = new NodeManagerClient(serverNode.getEndpoint());
                logger.info("send to node {} to stop container", serverNode.getEndpoint());
                nodeManagerClient.stopContainers(context, data);
            } catch (Exception e) {
                logger.error("send stop command error", e);
            }
        });
    }

    ;

    @Override
    public ErSessionMeta prepare(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        ErSessionMeta erSessionMeta = sessionMainService.getSession(data.getId(), true, false, false);
        if (erSessionMeta == null) {
            throw new RuntimeException("");
        }
        if (StringUtils.isNotEmpty(preStateParam) && preStateParam.equals(erSessionMeta.getStatus())) {
            throw new RuntimeException("");
        }
        if (data.getActiveProcCount() != null) {
            erSessionMeta.setActiveProcCount(data.getActiveProcCount());
        }
        this.openAsynPostHandle(context);

        return erSessionMeta;
    }

    @Override
    public ErSessionMeta handle(Context context, ErSessionMeta erSessionMeta, String preStateParam, String desStateParam) {
        updateStatus(context, erSessionMeta, preStateParam, desStateParam);
        logger.info("===================={}", erSessionMeta);
        erSessionMeta.getProcessors().forEach(processor -> {
            processorStateMachine.changeStatus(context, processor, null, ProcessorStatus.KILLED.name());
        });
        return sessionMainService.getSession(erSessionMeta.getId(), true, false, false);
    }
}
