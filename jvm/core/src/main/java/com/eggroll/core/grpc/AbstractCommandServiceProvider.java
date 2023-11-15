package com.eggroll.core.grpc;

import com.google.protobuf.ByteString;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;
import io.grpc.stub.StreamObserver;

public class AbstractCommandServiceProvider extends CommandServiceGrpc.CommandServiceImplBase {

    protected Dispatcher dispatcher;

    @Override
    public void call(Command.CommandRequest request,
                     StreamObserver<Command.CommandResponse> responseObserver) {
        String uri = request.getUri();
        byte[] resultBytes = dispatcher.dispatch(uri, request.getArgsList().get(0).toByteArray());
        Command.CommandResponse.Builder responseBuilder = Command.CommandResponse.newBuilder();
        responseBuilder.setId(request.getId());
        responseBuilder.addResults(ByteString.copyFrom(resultBytes));
        responseObserver.onNext(responseBuilder.build());
        responseObserver.onCompleted();
    }
}
