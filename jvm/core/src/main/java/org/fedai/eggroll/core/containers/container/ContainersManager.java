package org.fedai.eggroll.core.containers.container;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class ContainersManager {

    Logger log = LoggerFactory.getLogger(ContainersManager.class);

    private Map<Long, ContainerLifecycleHandler> handlers;
    private ExecutorService executor;
    private ScheduledExecutorService scheduledExecutor;
    private final List<Container.ContainerStatusCallback> callbacks = new ArrayList<>();

    public ContainersManager(ExecutorService ec, List<Container.ContainerStatusCallback> callbacks) {
        handlers = new ConcurrentHashMap<>();
        executor = ec;
        scheduledExecutor = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutor.scheduleAtFixedRate(
                () -> {
                    handlers.forEach((containerId, handler) -> {
                        if (handler.isPoisoned()) {
                            handler.killContainer();
                        }
                    });
                    handlers.entrySet().removeIf(entry -> entry.getValue().isCompleted());
                }, 0, 1, TimeUnit.MINUTES);
        this.callbacks.addAll(callbacks);
    }

    public static ContainersManagerBuilder builder() {
        return new ContainersManagerBuilder();
    }

    public void addContainer(Long containerId, ContainerTrait container) {

        ContainerLifecycleHandler lifecycleHandler = ContainerLifecycleHandler
                .builder()
                .withContainer(container)
                .withCallbacks(callbacks)
                .build(executor);
        handlers.putIfAbsent(containerId, lifecycleHandler);
    }

    public void startContainer(Long containerId) {
        ContainerLifecycleHandler handler = handlers.get(containerId);
        if (handler != null) {
            handler.startContainer();
        }
    }

    public void stopContainer(Long containerId) {
        ContainerLifecycleHandler handler = handlers.get(containerId);
        if (handler != null) {
            executor.execute(handler::stopContainer);
        }
    }

    public void killContainer(Long containerId) {
        log.info("killlog1 containerId:{},handlers.size: {}",containerId,handlers.size());
        handlers.entrySet().forEach(entry -> {
            log.info("killlog2 key: {}",entry.getKey());
        });
        ContainerLifecycleHandler handler = handlers.get(containerId);
        if (handler != null) {
            executor.execute(handler::killContainer);
        }
    }

    public void stop() {
        scheduledExecutor.shutdown();
        executor.shutdown();
    }
}
