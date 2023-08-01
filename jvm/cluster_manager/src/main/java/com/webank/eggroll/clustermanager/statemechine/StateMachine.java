package com.webank.eggroll.clustermanager.statemechine;

public interface StateMachine<S, E, C> {
    S fireEvent(S var1, E var2, C var3);

    String getMachineId();


}