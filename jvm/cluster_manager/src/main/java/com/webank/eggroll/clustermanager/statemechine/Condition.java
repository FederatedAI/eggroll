package com.webank.eggroll.clustermanager.statemechine;

public interface Condition<C> {
    boolean isSatisfied(C var1);

    default String name() {
        return this.getClass().getSimpleName();
    }
}
