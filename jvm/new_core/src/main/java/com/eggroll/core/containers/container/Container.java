package com.eggroll.core.containers.container;


import java.util.Optional;

public class Container {

//    public static class ContainerKey {
//        public static final String PYTHON_EXEC = "eggroll.container.python.exec";
//        public static final String DEEPSPEED_PYTHON_EXEC = "eggroll.container.deepspeed.python.exec";
//        public static final String DEEPSPEED_SCRIPT_PATH = "eggroll.container.deepspeed.script.path";
//        public static final String DEEPSPEED_TORCH_DISTRIBUTED_BACKEND = "eggroll.container.deepspeed.distributed.backend";
//        public static final String DEEPSPEED_TORCH_DISTRIBUTED_STORE_HOST = "eggroll.container.deepspeed.distributed.store.host";
//        public static final String DEEPSPEED_TORCH_DISTRIBUTED_STORE_PORT = "eggroll.container.deepspeed.distributed.store.port";
//        public static final String EGGPAIR_VENV = "eggroll.resourcemanager.bootstrap.egg_pair.venv";
//    }

    public interface ContainerStatusCallback {
        void apply(ContainerStatus status, ContainerTrait container, Optional<Exception> exception);
    }

}
