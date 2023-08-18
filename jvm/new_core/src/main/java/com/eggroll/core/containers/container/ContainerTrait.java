package com.eggroll.core.containers.container;


import com.sun.javafx.util.Logging;

public interface ContainerTrait {

    int getPid();

    long getProcessorId();

    boolean start();

    boolean stop();

    boolean kill();

    int waitForCompletion();
}