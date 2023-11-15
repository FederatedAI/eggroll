package com.webank.eggroll.clustermanager.statemachine;

import org.fedai.eggroll.core.constant.ProcessorStatus;
import org.fedai.eggroll.core.constant.ServerNodeStatus;
import org.fedai.eggroll.core.constant.ServerNodeTypes;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.NodeManagerClient;
import org.fedai.eggroll.core.pojo.ErServerNode;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import org.apache.commons.lang3.StringUtils;

import java.util.List;


@Singleton
public class SessionStopHandler extends AbstractSessionStateHandler {
    @Inject
    SessionMainService sessionMainService;

    @Override
    public void asynPostHandle(Context context, ErSessionMeta data, String preStateParam, String desStateParam) {
        ErServerNode erServerNodeExample = new ErServerNode();
        erServerNodeExample.setNodeType(ServerNodeTypes.NODE_MANAGER.name());
        erServerNodeExample.setStatus(ServerNodeStatus.HEALTHY.name());
        List<ErServerNode> serverNodes = serverNodeService.getListByErServerNode(erServerNodeExample);
        serverNodes.parallelStream().forEach(serverNode -> {
            try {
                NodeManagerClient nodeManagerClient = new NodeManagerClient(serverNode.getEndpoint());
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
        erSessionMeta.getProcessors().forEach(processor -> {
            processorStateMachine.changeStatus(context, processor, null, ProcessorStatus.STOPPED.name());
        });
        return sessionMainService.getSession(erSessionMeta.getId(), true, false, false);
    }
}
