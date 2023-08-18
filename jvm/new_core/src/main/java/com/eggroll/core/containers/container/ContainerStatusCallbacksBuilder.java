package com.eggroll.core.containers.container;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public class ContainerStatusCallbacksBuilder {

    public List<Container.ContainerStatusCallback> callbacks = new ArrayList<>();


    public ContainerStatusCallbacksBuilder withCallback(ContainerStatus status, Container.ContainerStatusCallback callback) {
        Container.ContainerStatusCallback _callback = (s, c, e) -> {
            if (s == status) {
                callback.apply(s,c,e);
            }
        };
        callbacks.add(_callback);
        return this;
    }



    public ContainerStatusCallbacksBuilder withCallbacks(List<Container.ContainerStatusCallback> callbacks) {
        this.callbacks.addAll(callbacks);
        return this;
    }


    public ContainerStatusCallbacksBuilder withPendingCallback(ContainerTrait callback) {
        Container.ContainerStatusCallback containerStatusCallback = new Container.ContainerStatusCallback() {
            @Override
            public void apply(ContainerStatus status, ContainerTrait container, Optional<Exception> exception) {
                System.out.println(status);
            }
        };
        return withCallback(ContainerStatus.Pending, (a,s,d)-> System.out.println("containerStatusCallback = " + a));
    }
//
//    public ContainerStatusCallbacksBuilder withStartedCallback(ContainerTrait callback) {
//        return withCallback(ContainerStatus.Started, (s,e,c) -> callback.a(e));
//    }
//
//    public ContainerStatusCallbacksBuilder withFailedCallback(ContainerTrait callback) {
//        return withCallback(ContainerStatus.Failed, (c, e) -> callback.(c));
//    }
//
//    public ContainerStatusCallbacksBuilder withSuccessCallback(Container.ContainerStatusCallback callback) {
//        return withCallback(ContainerStatus.Success, (s, c, e) -> callback.apply(s, c, e));
//    }
//
//
//
//public ContainerStatusCallbacksBuilder withExceptionCallback(Container.ExceptionCallback callback) {
//    return withCallback(ContainerStatus.Exception, callback);
//}

}
