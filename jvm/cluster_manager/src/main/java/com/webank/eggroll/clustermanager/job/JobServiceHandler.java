package com.webank.eggroll.clustermanager.job;

import com.eggroll.core.constant.SessionStatus;
import com.eggroll.core.grpc.NodeManagerClient;
import com.eggroll.core.pojo.ErProcessor;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.pojo.ErSessionMeta;
import com.eggroll.core.pojo.KillContainersRequest;
import com.webank.eggroll.clustermanager.cluster.ClusterResourceManager;
import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
import com.webank.eggroll.clustermanager.dao.impl.SessionMainService;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;


@Service
public class JobServiceHandler {
    Logger log = LoggerFactory.getLogger(JobServiceHandler.class);

    @Autowired
    ClusterResourceManager clusterResourceManager;
    @Autowired
    SessionMainService sessionMainService;
    @Autowired
    ServerNodeService serverNodeService;

    public void killJob(String sessionId) {
        log.info("killing job {}", sessionId);
        try {
            clusterResourceManager.lockSession(sessionId);
            clusterResourceManager.getKillJobMap().put(sessionId, System.currentTimeMillis());
            if (sessionMainService.getById(sessionId) == null) {
                return;
            }
            ErSessionMeta sessionMeta = sessionMainService.getSession(sessionId);
            if (StringUtils.equalsAny(sessionMeta.getStatus(), SessionStatus.KILLED.name(), SessionStatus.CLOSED.name(), SessionStatus.ERROR.name())) {
                return;
            }
            Map<Long, List<ErProcessor>> groupMap = sessionMeta.getProcessors().stream().collect(Collectors.groupingBy(ErProcessor::getServerNodeId));
            Map<ErServerNode, List<ErProcessor>> nodeAndProcessors = new HashMap<>();
            groupMap.forEach((nodeId, processors) -> {
                ErServerNode erServerNode = serverNodeService.getById(nodeId).toErServerNode();
                KillContainersRequest killContainersRequest = new KillContainersRequest();
                killContainersRequest.setSessionId(sessionId);
                List<Long> processorIdList = new ArrayList<>();
                for (ErProcessor processor : processors) {
                    processorIdList.add(processor.getId());
                }
                try {
                    killContainersRequest.setContainers(processorIdList);
                    new NodeManagerClient(erServerNode.getEndpoint()).killJobContainers(killContainersRequest);
                } catch (Exception e) {
                    log.error("killContainers error : ", e);
                }
            });
        } finally {
            clusterResourceManager.unlockSession(sessionId);
        }
    }

//    public SubmitJobResponse handleSubmit(SubmitJobRequest submitJobMeta) {
//        JobProcessorTypes jobType = JobProcessorTypes.fromString(submitJobMeta.getJobType());
//        if (jobType == JobProcessorTypes.DeepSpeed) {
//            return handleDeepspeedSubmit(submitJobMeta);
//        } else {
//            throw new IllegalArgumentException("unsupported job type: " + submitJobMeta.getJobType());
//        }
//    }
//
//
//    public QueryJobStatusResponse handleJobStatusQuery(QueryJobStatusRequest queryJobStatusRequest) {
//        String sessionId = queryJobStatusRequest.getSessionId();
//        String status = smDao.getSessionMain(sessionId).getStatus();
//        return new QueryJobStatusResponse(sessionId, status);
//    }
//
//    public KillJobResponse handleJobKill(KillJobRequest killJobRequest) {
//        String sessionId = killJobRequest.getSessionId();
//        killJob(sessionId, false);
//        return new KillJobResponse(sessionId);
//    }
//
//    public StopJobResponse handleJobStop(StopJobRequest stopJobRequest) {
//        String sessionId = stopJobRequest.getSessionId();
//        killJob(sessionId, false);
//        return new StopJobResponse(sessionId);
//    }
}
