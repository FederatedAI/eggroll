package org.fedai.eggroll.nodemanager.extend;

import org.fedai.eggroll.core.exceptions.PathNotExistException;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.webank.eggroll.core.transfer.Extend;
import com.webank.eggroll.core.transfer.ExtendTransferServerGrpc;
import org.fedai.eggroll.nodemanager.containers.ContainersServiceHandler;
import io.grpc.stub.StreamObserver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;

@Singleton
public class NodeExtendTransferService extends ExtendTransferServerGrpc.ExtendTransferServerImplBase {

    Logger log = LoggerFactory.getLogger(NodeExtendTransferService.class);

    @Inject
    ContainersServiceHandler containersServiceHandler;

    private String INIT_STATUS = "INIT";
    private String PREPARE_STATUS = "PREPARE";
    private String RUNNING_STATUS = "RUNNING";
    private String ERROR_STATUS = "ERROR";

    private static final String STOP_STATUS = "STOP";

    @Override
    public StreamObserver<Extend.GetLogRequest> getLog(StreamObserver<Extend.GetLogResponse> responseObserver) {

        return new StreamObserver<Extend.GetLogRequest>() {
            private String status = INIT_STATUS;
            private LogStreamHolder logStreamHolder = null;
            private UUID instanceId = UUID.randomUUID();

            @Override
            public void onNext(Extend.GetLogRequest request) {
                synchronized (this) {
                    log.info("instance {} receive get log request for {}", request.getSessionId(), request.getLogType());
                    if (status.equals(INIT_STATUS)) {
                        status = PREPARE_STATUS;
                        Thread thread = new Thread(new Runnable() {
                            @Override
                            public void run() {
                                int tryCount = 0;
                                while (tryCount < 20 && status.equals(PREPARE_STATUS)) {
                                    tryCount++;
                                    try {
                                        log.info("try to create log stream holder {}, try count {}", request.getSessionId(), tryCount);
                                        logStreamHolder = containersServiceHandler.createLogStream(request, responseObserver);
                                        if (status.equals(PREPARE_STATUS)) {
                                            logStreamHolder.run();
                                            status = RUNNING_STATUS;
                                        } else {
                                            responseObserver.onCompleted();
                                        }
                                        break;
                                    } catch (PathNotExistException e) {
                                        try {
                                            Thread.sleep(5000);
                                            log.error("path not found for {}", e.getMessage());
                                        } catch (InterruptedException ex) {
                                            ex.printStackTrace();
                                        }
                                    }
                                }
                                if (logStreamHolder == null) {
                                    Extend.GetLogResponse response = Extend.GetLogResponse.newBuilder()
                                            .setCode("110")
                                            .setMsg("log file is not found")
                                            .build();
                                    responseObserver.onNext(response);
                                    responseObserver.onCompleted();
                                }
                            }
                        });
                        thread.start();
                    }
                }
            }

            @Override
            public void onError(Throwable throwable) {
                status = ERROR_STATUS;
                log.info("instance {} receive onError", instanceId);
                if (logStreamHolder != null) {
                    logStreamHolder.stop();
                }
            }

            @Override
            public void onCompleted() {
                log.info("instance {} receive onCompleted", instanceId);
                status = STOP_STATUS;
                if (logStreamHolder != null) {
                    logStreamHolder.stop();
                }
            }
        };
    }
}
