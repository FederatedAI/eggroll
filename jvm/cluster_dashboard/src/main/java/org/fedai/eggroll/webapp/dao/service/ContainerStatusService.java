package org.fedai.eggroll.webapp.dao.service;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.gson.Gson;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.apache.commons.collections.CollectionUtils;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionMainService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionProcessorService;
import org.fedai.eggroll.clustermanager.entity.SessionProcessor;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.ProcessorType;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.ClusterManagerClient;
import org.fedai.eggroll.core.grpc.NodeManagerClient;
import org.fedai.eggroll.core.pojo.*;
import org.fedai.eggroll.core.utils.JsonUtil;
import org.fedai.eggroll.webapp.queryobject.SessionProcessorQO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Singleton
public class ContainerStatusService {

    Logger logger = LoggerFactory.getLogger(NodeDetailService.class);

    ClusterManagerClient client = new ClusterManagerClient(new ErEndpoint(MetaInfo.CONFKEY_CLUSTER_MANAGER_HOST, MetaInfo.CONFKEY_CLUSTER_MANAGER_PORT));

    @Inject
    private SessionMainService sessionMainService;

    @Inject
    private SessionProcessorService sessionProcessorService;

    @Inject
    ServerNodeService serverNodeService;


    public void killSession(SessionProcessorQO req) {
        logger.info("kill session req: {}",new Gson().toJson(req));
        String type = null;
        String sessionId = req.getSessionId();
        // todo 判断session状态来确定是否要发起
        ErSessionMeta session = sessionMainService.getSession(sessionId);

        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("session_id", sessionId);
        List<SessionProcessor> processorList = sessionProcessorService.list(queryWrapper);

        if (CollectionUtils.isNotEmpty(processorList)) {
            type = processorList.get(0).getProcessorType();
        }

        if (ProcessorType.egg_pair.equals(type)) {
            List<ErProcessor> processors = new ArrayList<>();
            for (SessionProcessor sessionProcessor : processorList) {
                ErProcessor erProcessor = sessionProcessor.toErProcessor();
                processors.add(erProcessor);
            }
            ErSessionMeta erSessionMeta = new ErSessionMeta();
            erSessionMeta.setId(sessionId);
            erSessionMeta.setProcessors(processors);

            client.killSession(new Context(), erSessionMeta);
        } else {
            KillJobRequest request = new KillJobRequest();
            request.setSessionId(sessionId);
            client.killJob(new Context(), request);
        }
    }

    /**
     *
     * 根据进程的类型，直接请求到nodeManager的killSession和KillContainer接口
     * 将单个进程封装到对应的请求参数中。
     * @param pid
     */
    public void killProcessor(Long pid) {
        logger.info("kill processor req, pid :{}",pid);

        QueryWrapper<SessionProcessor> queryWrapper = new QueryWrapper<>();
        queryWrapper.eq("processor_id", pid);
        List<SessionProcessor> processorList = sessionProcessorService.list(queryWrapper);
        SessionProcessor sessionProcessor = processorList.get(0);
        ErProcessor erProcessor = sessionProcessor.toErProcessor();
        String processorType = sessionProcessor.getProcessorType();
        if (ProcessorType.egg_pair.name().equals(processorType)) {
            killEggPairProcessor(new Context(), erProcessor);
        } else if (ProcessorType.DeepSpeed.name().equals(processorType)) {
            killDeepSpedProcessor(new Context(), erProcessor);
        } else {
            return;
        }
    }

    private void killEggPairProcessor(Context context, ErProcessor processor) {
        logger.info("prepare to kill eggPair single processor {}", JsonUtil.object2Json(processor));
        ErServerNode serverNodeInDb = serverNodeService.getById(processor.getServerNodeId()).toErServerNode();
        ErSessionMeta erSessionMeta = sessionMainService.getSession(processor.getSessionId(), true, false, false);
        if (erSessionMeta != null) {
            if (CollectionUtils.isEmpty(erSessionMeta.getProcessors())) {
                return;
            }
            List<ErProcessor> collect = erSessionMeta.getProcessors().stream().filter((erProcessor) -> erProcessor.getId().equals(processor.getId())).collect(Collectors.toList());
            erSessionMeta.setProcessors(collect);
            erSessionMeta.getOptions().put(Dict.SERVER_NODE_ID, processor.getServerNodeId().toString());
            NodeManagerClient nodeManagerClient = new NodeManagerClient(serverNodeInDb.getEndpoint());
            nodeManagerClient.killContainers(context, erSessionMeta);
        }
    }

    private void killDeepSpedProcessor(Context context, ErProcessor processor) {
        logger.info("prepare to kill deepSped single processor {}", JsonUtil.object2Json(processor));
        KillContainersRequest killContainersRequest = new KillContainersRequest();
        killContainersRequest.setSessionId(processor.getSessionId());
        List<Long> processorIdList = new ArrayList<>();
        processorIdList.add(processor.getId());

        Long serverNodeId = processor.getServerNodeId();
        ErServerNode erServerNode = serverNodeService.getById(serverNodeId).toErServerNode();
        killContainersRequest.setContainers(processorIdList);
        NodeManagerClient nodeManagerClient = new NodeManagerClient(erServerNode.getEndpoint());
        nodeManagerClient.killJobContainers(context, killContainersRequest);
    }

    public Map<String,String> queryNodeMetaInfo(Context context, Long serverNodeId) {
        ErServerNode erServerNode = serverNodeService.getById(serverNodeId).toErServerNode();
        NodeManagerClient nodeManagerClient = new NodeManagerClient(erServerNode.getEndpoint());
        MetaInfoRequest request = new MetaInfoRequest();
        MetaInfoResponse metaInfoResponse = nodeManagerClient.queryNodeMetaInfo(context, request);
        Map<String, String> metaMap = metaInfoResponse.getMetaMap();
        return metaMap;
    }

    public Integer getWaitingQueue() {
        QueueViewRequest queueViewRequest = new QueueViewRequest();
        QueueViewResponse queueViewResponse = client.getQueueView(new Context(), queueViewRequest);
        return queueViewResponse.getQueueSize();
    }
}
