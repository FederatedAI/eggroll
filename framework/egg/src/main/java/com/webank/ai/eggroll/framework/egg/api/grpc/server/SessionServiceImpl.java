package com.webank.ai.eggroll.framework.egg.api.grpc.server;

import com.webank.ai.eggroll.api.computing.ComputingBasic;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.framework.egg.NodeManager;
import com.webank.ai.eggroll.api.framework.egg.SessionServiceGrpc;
import com.webank.ai.eggroll.core.api.grpc.server.GrpcServerWrapper;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.framework.egg.manager.EggSessionManager;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class SessionServiceImpl extends SessionServiceGrpc.SessionServiceImplBase {
    @Autowired
    private EggSessionManager eggSessionManager;
    @Autowired
    private ToStringUtils toStringUtils;
    @Autowired
    private GrpcServerWrapper grpcServerWrapper;

    private static final Logger LOGGER = LogManager.getLogger();

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getOrCreateSession(BasicMeta.SessionInfo request, StreamObserver<BasicMeta.SessionInfo> responseObserver) {
        grpcServerWrapper.wrapGrpcServerRunnable(responseObserver, () -> {
            LOGGER.info("[EGG][SESSIONSERVICE] getOrCreateSession. request: {}", toStringUtils.toOneLineString(request));
            eggSessionManager.getOrCreateSession(request);

            String sessionId = request.getSessionId();
            BasicMeta.SessionInfo result = eggSessionManager.getSession(sessionId);
            responseObserver.onNext(result == null ? BasicMeta.SessionInfo.getDefaultInstance() : eggSessionManager.getSession(sessionId));
            responseObserver.onCompleted();
        });
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void stopSession(BasicMeta.SessionInfo request, StreamObserver<BasicMeta.SessionInfo> responseObserver) {
        grpcServerWrapper.wrapGrpcServerRunnable(responseObserver, () -> {
            LOGGER.info("[EGG][SESSIONSERVICE] stopSession. request: {}", toStringUtils.toOneLineString(request));
            eggSessionManager.stopSession(request.getSessionId());

            String sessionId = request.getSessionId();
            BasicMeta.SessionInfo result = eggSessionManager.getSession(sessionId);
            responseObserver.onNext(result == null ? BasicMeta.SessionInfo.getDefaultInstance() : eggSessionManager.getSession(sessionId));
            responseObserver.onCompleted();
        });
    }

    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getComputingEngine(NodeManager.ComputingEngineRequest request, StreamObserver<ComputingBasic.ComputingEngineDescriptor> responseObserver) {
        grpcServerWrapper.wrapGrpcServerRunnable(responseObserver, () -> {
            LOGGER.info("[EGG][SESSIONSERVICE] getComputeEngine. request: {}", toStringUtils.toOneLineString(request));
            ComputingEngine computingEngine = eggSessionManager.getComputeEngineDescriptor(request.getSession().getSessionId());

            responseObserver.onNext(computingEngine.toProtobuf());
            responseObserver.onCompleted();
        });
    }
}
