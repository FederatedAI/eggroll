package org.fedai.eggroll.core.containers.container;

import java.util.ArrayList;
import java.util.List;

public class ContainerStatusCallbacksBuilder {

    public List<Container.ContainerStatusCallback> callbacks = new ArrayList<>();

    public ContainerStatusCallbacksBuilder withCallback(ContainerStatus status, Container.ContainerStatusCallback callback) {
        Container.ContainerStatusCallback _callback = (s, c, e) -> {
            if (s == status) {
                callback.apply(s, c, e);
            }
        };
        callbacks.add(_callback);
        return this;
    }


    public ContainerStatusCallbacksBuilder withCallbacks(List<Container.ContainerStatusCallback> callbacks) {
        this.callbacks.addAll(callbacks);
        return this;
    }


    public ContainerStatusCallbacksBuilder withPendingCallback(Container.ContainerStatusCallback callback) {
        return withCallback(ContainerStatus.Pending, callback);
    }

    public ContainerStatusCallbacksBuilder withStartedCallback(Container.ContainerStatusCallback callback) {
        return withCallback(ContainerStatus.Started, callback);
    }

    public ContainerStatusCallbacksBuilder withFailedCallback(Container.ContainerStatusCallback callback) {
        return withCallback(ContainerStatus.Failed, callback);
    }

    public ContainerStatusCallbacksBuilder withSuccessCallback(Container.ContainerStatusCallback callback) {
        return withCallback(ContainerStatus.Success, callback);
    }

    public ContainerStatusCallbacksBuilder withExceptionCallback(Container.ContainerStatusCallback callback) {
        return withCallback(ContainerStatus.Exception, callback);
    }

}
