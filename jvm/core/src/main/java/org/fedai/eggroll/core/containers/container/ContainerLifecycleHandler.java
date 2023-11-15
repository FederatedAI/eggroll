package org.fedai.eggroll.core.containers.container;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class ContainerLifecycleHandler {
    Logger logger = LoggerFactory.getLogger(ContainerLifecycleHandler.class);

    private ContainerTrait container;
    private List<Container.ContainerStatusCallback> callbacks;
    private ExecutorService executor;
    private Future<Void> watcher;
    private ContainerStatus status = ContainerStatus.Pending;

    public ContainerLifecycleHandler(ContainerTrait container,
                                     List<Container.ContainerStatusCallback> callbacks,
                                     ExecutorService executor) {
        this.container = container;
        this.callbacks = callbacks;
        this.executor = executor;
        setStatus(ContainerStatus.Pending, Optional.empty());
    }

    public boolean isCompleted() {
        switch (status) {
            case Failed:
            case Success:
            case Exception:
                return true;
            default:
                return false;
        }
    }

    public boolean isSuccess() {
        return status == ContainerStatus.Success;
    }

    public boolean isFailed() {
        return status == ContainerStatus.Failed;
    }

    public boolean isStarted() {
        return status == ContainerStatus.Started;
    }

    public boolean isPoisoned() {
        return status == ContainerStatus.Poison;
    }

    private void setStatus(ContainerStatus status, Optional<Exception> e) {
        if (!isCompleted() && this.status != status) {
            this.status = status;
            logger.info(String.format("Container %s status changed to %s", container, status));
            if (e.isPresent()) {
                logger.error(String.format("Container %s failed", container), e.get());
            }
            applyCallbacks(status, e);
        }
    }

    public void startContainer() {
        if (isStarted()) {
            throw new IllegalStateException("Container already started");
        }
        if (isCompleted()) {
            throw new IllegalStateException("Container already completed");
        }

        watcher = (Future<Void>) executor.submit(() -> {

            container.start();
            setStatus(ContainerStatus.Started, Optional.empty());
            int exitCode = container.waitForCompletion();
            if (exitCode != 0) {
                setStatus(ContainerStatus.Failed, Optional.empty());
            } else {
                setStatus(ContainerStatus.Success, Optional.empty());
            }
        });
    }

    public boolean stopContainer() {
        if (isStarted()) {
            return container.stop();
        } else {
            logger.error("Container " + container + " not started, cannot stop");
            return false;
        }
    }

    public boolean killContainer() {
        logger.info("Killing container " + container);
        if (isStarted()) {
            return container.kill();
        } else {
            logger.error("Container " + container + " not started, cannot kill immediately, poison instead");
            status = ContainerStatus.Poison;
            return false;
        }
    }

    private void applyCallbacks(ContainerStatus status, Optional<Exception> e) {
        callbacks.forEach(callback -> callback.apply(status, container, e));
    }

    public static ContainerLifecycleHandlerBuilder builder() {
        return new ContainerLifecycleHandlerBuilder();
    }
}
