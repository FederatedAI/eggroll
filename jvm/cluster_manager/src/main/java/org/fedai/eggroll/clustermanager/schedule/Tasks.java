package org.fedai.eggroll.clustermanager.schedule;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.google.common.collect.Lists;
import org.apache.ibatis.session.Configuration;
import org.fedai.eggroll.clustermanager.cluster.ClusterManagerService;
import org.fedai.eggroll.clustermanager.cluster.ClusterResourceManager;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionMainService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionProcessorService;
import org.fedai.eggroll.clustermanager.entity.SessionProcessor;
import org.fedai.eggroll.clustermanager.statemachine.ProcessorStateMachine;
import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.core.constant.*;
import org.fedai.eggroll.core.context.Context;
import org.fedai.eggroll.core.grpc.NodeManagerClient;
import org.fedai.eggroll.core.pojo.ErProcessor;
import org.fedai.eggroll.core.pojo.ErServerNode;
import org.fedai.eggroll.core.pojo.ErSessionMeta;
import org.mybatis.guice.configuration.ConfigurationSettingListener;
import org.mybatis.guice.configuration.settings.ConfigurationSetting;
import org.mybatis.guice.configuration.settings.MapperConfigurationSetting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import javax.inject.Provider;
import javax.inject.Singleton;
import java.util.*;
import java.util.stream.Collectors;

@Singleton
public class Tasks implements Provider<Configuration>, ConfigurationSettingListener {

    Logger log = LoggerFactory.getLogger(Tasks.class);


    @Inject
    ServerNodeService serverNodeService;

    @Inject
    ClusterManagerService managerService;

    @Inject
    SessionMainService sessionMainService;

    @Inject
    SessionProcessorService sessionProcessorService;

    @Inject
    ProcessorStateMachine processorStateMachine;

    @Inject
    ClusterResourceManager clusterResourceManager;


    /**
     * 检查DB中状态为running的进程,分别到到节点中查找是否存在以及状态。
     * 然后对账。
     * 则表示该进程异常
     */
    @Schedule(cron = "0/10 * * * * ?")
    public void checkDbRunningProcessor() {
        try {
            long now = System.currentTimeMillis();
            ErProcessor erProcessor = new ErProcessor();
            erProcessor.setStatus(ProcessorStatus.RUNNING.name());
            List<ErProcessor> erProcessors = sessionProcessorService.doQueryProcessor(erProcessor);

            // 根据节点分组
            Map<Long, List<ErProcessor>> grouped = erProcessors.stream().collect(Collectors.groupingBy(ErProcessor::getServerNodeId));
            Context context = new Context();
            grouped.forEach((serverNodeId, processorList) -> {
                // 从缓存中拿出该节点的坐标信息，并建立该客户端连接
                ErServerNode serverNode = serverNodeService.getByIdFromCache(serverNodeId);
                if (serverNode != null) {
                    NodeManagerClient nodeManagerClient = new NodeManagerClient(serverNode.getEndpoint());
                    // 检查节点上每个进程的状态
                    for (ErProcessor processor : processorList) {
                        ErProcessor result = nodeManagerClient.checkNodeProcess(context, processor);

                        // 如果该节点上的进程状态为kill或者不存在
                        if (result == null || ProcessorStatus.KILLED.name().equals(result.getStatus())) {
                            SessionProcessor processorInDb = sessionProcessorService.getById(processor.getId());
                            if (processorInDb != null) {
                                if (ProcessorStatus.RUNNING.name().equals(processorInDb.getStatus())) {
                                    ErProcessor checkNodeProcessResult = nodeManagerClient.checkNodeProcess(context, processor);
                                    if (checkNodeProcessResult == null || ProcessorStatus.KILLED.name().equals(checkNodeProcessResult.getStatus())) {
                                        Context statusContext = new Context();
                                        statusContext.putData(Dict.STATUS_REASON, StatusReason.TASK_CHECK.name());
                                        processorStateMachine.changeStatus(statusContext, processor, null, ProcessorStatus.ERROR.name());
                                    }
                                }
                            }
                        }
                    }
                }
            });
        } catch (Exception e) {
            log.error("checkDbRunningProcessor error :", e);
        }
    }

    /**
     * 根据node最后上报的时间来判断该node是否与集群失去连接
     */
    @Schedule(cron = "0/5 * * * * ?")
    public void checkNodeHeartBeat() {

        long expire = MetaInfo.CONFKEY_CLUSTER_MANAGER_NODE_HEARTBEAT_EXPIRED_COUNT *
                MetaInfo.CONFKEY_NODE_MANAGER_HEARTBEAT_INTERVAL;
        try {
            long now = System.currentTimeMillis();
            ErServerNode erServerNode = new ErServerNode();
            erServerNode.setStatus(ServerNodeStatus.HEALTHY.name());
            List<ErServerNode> nodes = serverNodeService.getListByErServerNode(erServerNode);

            for (ErServerNode node : nodes) {
                long interval = now - (node.getLastHeartBeat() != null ?
                        node.getLastHeartBeat().getTime() : now);
                if (interval > expire && ServerNodeTypes.NODE_MANAGER.name().equals(node.getNodeType())) {
                    synchronized (serverNodeService.getReadLockTag()) {
                        QueryWrapper<SessionProcessor> processorQueryWrapper = new QueryWrapper<>();
                        processorQueryWrapper.lambda().eq(SessionProcessor::getServerNodeId, node.getId())
                                .eq(SessionProcessor::getStatus, ProcessorStatus.RUNNING.name());
                        if (sessionProcessorService.count(processorQueryWrapper) == 0) {
                            log.info("server node " + node + " change status to LOSS");
                            node.setStatus(ServerNodeStatus.LOSS.name());
                            managerService.updateNode(node, false, false);
                        }
                    }
                }
            }
        } catch (Throwable e) {
            log.error("handle node heart beat error: ", e);
        }
    }

    /**
     * 定时kill掉泄露的进程（收到了已经标记为关闭的心跳）
     */
    @Schedule(cron = "0/10 * * * * ?")
    public void checkRedidualProcessor() {
        try {
            Context context = new Context();
            ClusterManagerService.residualHeartbeatMap.forEach((k, v) -> {
                try {
                    managerService.killResidualProcessor(context, v);
                    ClusterManagerService.residualHeartbeatMap.remove(k);
                } catch (Throwable e) {
                    e.printStackTrace();
                    log.error("kill residual processor error: " + e.getMessage());
                }
            });
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    /**
     * 检查状态为ACTIVE和NEW的session，如果该session超时，或者该session下存在非正常结束的进程，则kill掉该session
     */
    @Schedule(cron = "0/5 * * * * ?")
    public void sessionWatcherSchedule() {
        try {
            // 从DB中查询出状态为ACTIVE和NEW的Session
            List<ErSessionMeta> sessions = sessionMainService.getSessionMainsByStatus(Arrays.asList(SessionStatus.ACTIVE.name(), SessionStatus.NEW.name()));

            for (ErSessionMeta session : sessions) {
                try {
                    List<ErProcessor> sessionProcessors = sessionMainService.getSession(session.getId(), true, false, false).getProcessors();
                    String ACTIVE = SessionStatus.ACTIVE.name();
                    String NEW = SessionStatus.NEW.name();

                    switch (session.getName()) {
                        case "DeepSpeed":
                            log.debug("watch deepspeed session: " + session.getId() + " " + session.getStatus());
                            if (SessionStatus.ACTIVE.name().equals(session.getStatus())) {
                                // 如果该session下processor中有存在失败的，则kill掉该session，若全部为FINISHED则修改该session状态为FINISHED
                                managerService.checkAndHandleDeepspeedActiveSession(new Context(), session, sessionProcessors);
                            } else if (SessionStatus.NEW.name().equals(session.getStatus())) {
                                //  如果启动超时 则kill掉
                                managerService.checkAndHandleDeepspeedOutTimeSession(new Context(), session, sessionProcessors);
                            }
                            break;
                        default:
                            if (SessionStatus.ACTIVE.name().equals(session.getStatus())) {
                                // 如果运行超时，或者存在非正常完成(KILLED，ERROR，STOPPED)的进程，则kill掉
                                managerService.checkAndHandleEggpairActiveSession(session, sessionProcessors);
                            } else if (SessionStatus.NEW.name().equals(session.getStatus())) {
                                //  如果启动超时 则kill掉
                                managerService.checkAndHandleEggpairOutTimeSession(session, sessionProcessors);
                            }
                            break;
                    }
                } catch (Throwable e) {
                    log.error("session watcher handle session " + session.getId() + " error " + e.getMessage());
                    e.printStackTrace();
                }
            }
        } catch (Throwable e) {
            log.error("session watcher handle error ", e);
        }
    }


//    @Schedule(cron = "0/1 * * * * ?")

    /**
     * 统计集群中各个节点的资源情况
     */
    public void countNodeResource() {
        try {
            List<Long> nodeList = Lists.newArrayList();
            clusterResourceManager.getNodeResourceUpdateQueue().drainTo(nodeList);
            Set<Long> nodeSet = new HashSet<>();
            nodeSet.addAll(nodeList);
            for (Long nodeId : nodeSet) {
                clusterResourceManager.countAndUpdateNodeResourceInner(nodeId);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }


    @Override
    public Configuration get() {
        return null;
    }

    @Override
    public void addConfigurationSetting(ConfigurationSetting configurationSetting) {

    }

    @Override
    public void addMapperConfigurationSetting(MapperConfigurationSetting mapperConfigurationSetting) {

    }
}
