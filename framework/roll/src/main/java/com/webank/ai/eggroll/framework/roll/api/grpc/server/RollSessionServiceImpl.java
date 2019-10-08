package com.webank.ai.eggroll.framework.roll.api.grpc.server;

import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.framework.egg.SessionServiceGrpc;
import com.webank.ai.eggroll.core.api.grpc.server.GrpcServerWrapper;
import com.webank.ai.eggroll.core.utils.ToStringUtils;
import com.webank.ai.eggroll.framework.roll.manager.RollSessionManager;
import io.grpc.stub.StreamObserver;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

@Component
@Scope("prototype")
public class RollSessionServiceImpl extends SessionServiceGrpc.SessionServiceImplBase {
    @Autowired
    private GrpcServerWrapper grpcServerWrapper;
    @Autowired
    private RollSessionManager rollSessionManager;
    @Autowired
    private ToStringUtils toStringUtils;

    private static final Logger LOGGER = LogManager.getLogger();
    /**
     * @param request
     * @param responseObserver
     */
    @Override
    public void getOrCreateSession(BasicMeta.SessionInfo request, StreamObserver<BasicMeta.SessionInfo> responseObserver) {
        grpcServerWrapper.wrapGrpcServerRunnable(responseObserver, () -> {
            LOGGER.info("[ROLL][SESSION] getOrCreateSession. request: {}", toStringUtils.toOneLineString(request));

            BasicMeta.SessionInfo response = rollSessionManager.getOrCreateSession(request);

            responseObserver.onNext(response);
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
            LOGGER.info("[ROLL][SESSION] stopSession. request: {}", toStringUtils.toOneLineString(request));

            boolean result = rollSessionManager.stopSession(request.getSessionId());

            BasicMeta.SessionInfo response = null;
            if (result) {
                response = request;
            } else {
                response = BasicMeta.SessionInfo.getDefaultInstance();
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
        });
    }
}
