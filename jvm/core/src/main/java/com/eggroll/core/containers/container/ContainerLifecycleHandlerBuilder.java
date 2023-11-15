package com.eggroll.core.containers.container;


import lombok.Data;

import java.util.List;
import java.util.concurrent.ExecutorService;


@Data
public class ContainerLifecycleHandlerBuilder extends ContainerStatusCallbacksBuilder {
    private ContainerTrait container;

    public ContainerLifecycleHandlerBuilder withContainer(ContainerTrait container) {
        this.container = container;
        return this;
    }

    public ContainerLifecycleHandler build(ExecutorService executorService) {
        return new ContainerLifecycleHandler(this.container, this.callbacks, executorService);
    }


    @Override
    public ContainerLifecycleHandlerBuilder withCallbacks(List<Container.ContainerStatusCallback> callbacks) {
        this.callbacks.addAll(callbacks);
        return this;
    }
}