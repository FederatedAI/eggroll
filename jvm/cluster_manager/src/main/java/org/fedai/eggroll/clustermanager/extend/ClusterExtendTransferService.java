package org.fedai.eggroll.clustermanager.extend;

import org.fedai.eggroll.clustermanager.entity.SessionRanks;
import org.fedai.eggroll.core.exceptions.RankNotExistException;
import org.fedai.eggroll.core.grpc.GrpcConnectionFactory;
import org.fedai.eggroll.core.pojo.ErEndpoint;
import org.fedai.eggroll.core.pojo.ErServerNode;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import org.fedai.eggroll.clustermanager.dao.impl.ServerNodeService;
import org.fedai.eggroll.clustermanager.dao.impl.SessionRanksService;
import com.webank.eggroll.core.transfer.Extend;
import com.webank.eggroll.core.transfer.ExtendTransferServerGrpc;
import io.grpc.ManagedChannel;
import io.grpc.stub.StreamObserver;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

@Singleton
public class ClusterExtendTransferService extends ExtendTransferServerGrpc.ExtendTransferServerImplBase {

    Logger log = LoggerFactory.getLogger(ClusterExtendTransferService.class);

    @Inject
    SessionRanksService sessionRanksService;

    @Inject
    ServerNodeService serverNodeService;

    private String INIT_STATUS = "INIT";
    private String PREPARE_STATUS = "PREPARE";
    private String RUNNING_STATUS = "RUNNING";
    private String ERROR_STATUS = "ERROR";


    @Override
    public StreamObserver<Extend.GetLogRequest> getLog(StreamObserver<Extend.GetLogResponse> responseObserver) {
        return new StreamObserver<Extend.GetLogRequest>() {
            String status = INIT_STATUS;
            StreamObserver<Extend.GetLogRequest> requestSb = null;
            int index = 0;

            @Override
            public void onNext(Extend.GetLogRequest request) {
                try {
                    if (INIT_STATUS.equals(status)) {
                        status = PREPARE_STATUS;
                        log.info("receive get log request {}", request.toString());

                        if (StringUtils.isNotEmpty(request.getRank()) && Integer.parseInt(request.getRank()) > 0) {
                            index = Integer.parseInt(request.getRank());
                        }
                        SessionRanks sessionRank = new SessionRanks();
                        sessionRank.setSessionId(request.getSessionId());
                        List<SessionRanks> rankInfos = sessionRanksService.list(sessionRank);

                        if (rankInfos == null || rankInfos.size() == 0) {
                            log.error("can not found rank info for session {}", request.getSessionId());
                            throw new Exception("can not found rank info for session " + request.getSessionId());
                        }
                        List<SessionRanks> nodeIdMeta = rankInfos.stream().filter(rank -> rank.getLocalRank().intValue() == index).collect(Collectors.toList());
                        if (nodeIdMeta == null || nodeIdMeta.size() == 0) {
                            log.error("can not found rank info for session {} rank {}", request.getSessionId(), index);
                            throw new RankNotExistException("can not found rank info for session " + request.getSessionId() + " rank " + index);
                        }
                        SessionRanks rankInfo = nodeIdMeta.get(0);

                        ErServerNode erServerNode = serverNodeService.getByIdFromCache(rankInfo.getServerNodeId());

                        log.info("prepare to send log request to {} {}", erServerNode.getEndpoint().getHost(), erServerNode.getEndpoint().getPort());
                        ErEndpoint erEndpoint = new ErEndpoint(erServerNode.getEndpoint().getHost(), erServerNode.getEndpoint().getPort());
                        ManagedChannel managedChannel = GrpcConnectionFactory.createManagedChannel(erEndpoint, true);
                        ExtendTransferServerGrpc.ExtendTransferServerStub stub = ExtendTransferServerGrpc.newStub(managedChannel);
                        requestSb = stub.getLog(responseObserver);
                        status = RUNNING_STATUS;
                    }
                } catch (Exception e) {
                    status = ERROR_STATUS;
                    responseObserver.onNext(Extend.GetLogResponse.newBuilder().setCode("111").setMsg(e.getMessage()).build());
                    responseObserver.onCompleted();
                }
                if (RUNNING_STATUS.equals(status) && requestSb != null) {
                    requestSb.onNext(request.toBuilder().setRank(String.valueOf(index)).build());
                }
            }

            @Override
            public void onError(Throwable throwable) {
                if (RUNNING_STATUS.equals(status) && requestSb != null) {
                    requestSb.onError(throwable);
                }
            }

            @Override
            public void onCompleted() {
                if (RUNNING_STATUS.equals(status) && requestSb != null) {
                    requestSb.onCompleted();
                }
            }
        };
    }
}
