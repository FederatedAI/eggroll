package com.webank.ai.eggroll.framework.egg.api.grpc.server;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.framework.egg.NodeManager;
import com.webank.ai.eggroll.api.framework.egg.NodeManagerServiceGrpc;
import com.webank.ai.eggroll.core.api.grpc.server.GrpcServerWrapper;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.framework.egg.manager.EngineStatusTracker;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class NodeManagerServiceImpl extends NodeManagerServiceGrpc.NodeManagerServiceImplBase {
    @Autowired
    private GrpcServerWrapper grpcServerWrapper;
    @Autowired
    private EngineStatusTracker engineStatusTracker;
    @Autowired
    private ToStringUtils toStringUtils;

    private static final Logger LOGGER = LogManager.getLogger();
    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void heartbeat(NodeManager.HeartbeatRequest request, StreamObserver<NodeManager.HeartbeatResponse> responseObserver) {
        grpcServerWrapper.wrapGrpcServerRunnable(responseObserver, () -> {
            LOGGER.info("[EGG][NM][HEARTBEAT] heartbeat. request: {}", toStringUtils.toOneLineString(request));
            BasicMeta.Endpoint endpoint = request.getEndpoint();

            engineStatusTracker.addAliveEngine(endpoint);
            responseObserver.onNext(NodeManager.HeartbeatResponse.getDefaultInstance());
            responseObserver.onCompleted();
        });
    }
}
