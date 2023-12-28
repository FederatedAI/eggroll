package org.fedai.eggroll.core.containers.container;


import java.util.Optional;

public class Container {

    public interface ContainerStatusCallback {
        void apply(ContainerStatus status, ContainerTrait container, Optional<Exception> exception);
    }

}
