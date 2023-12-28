package org.fedai.eggroll.core.containers.container;

import java.util.concurrent.ExecutorService;

public class ContainersManagerBuilder extends ContainerStatusCallbacksBuilder {

    public ContainersManager build(ExecutorService executor) {
        return new ContainersManager(executor, callbacks);
    }
}