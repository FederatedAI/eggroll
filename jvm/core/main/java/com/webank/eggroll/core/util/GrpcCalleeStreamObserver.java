package com.webank.eggroll.core.util;

import io.grpc.stub.StreamObserver;

public abstract class GrpcCalleeStreamObserver<R, E> implements StreamObserver<R> {
    private final StreamObserver<E> caller;

    public GrpcCalleeStreamObserver(StreamObserver<E> caller) {
        this.caller = caller;
    }

    @Override
    public void onError(Throwable throwable) {
        Throwable grpcThrowable = ErrorUtils.toGrpcRuntimeException(throwable);
        caller.onError(grpcThrowable);

        logError("callee streaming error", throwable);
    }

    @Override
    public void onCompleted() {
        caller.onCompleted();
    }

}