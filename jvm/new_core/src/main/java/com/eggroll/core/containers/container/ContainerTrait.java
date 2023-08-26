package com.eggroll.core.containers.container;




public interface ContainerTrait {

    int getPid();

    long getProcessorId();

    boolean start();

    boolean stop();

    boolean kill();

    int waitForCompletion();
}