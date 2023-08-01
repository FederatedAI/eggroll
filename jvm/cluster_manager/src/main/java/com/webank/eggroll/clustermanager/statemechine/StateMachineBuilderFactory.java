
package com.webank.eggroll.clustermanager.statemechine;

public class StateMachineBuilderFactory {
    public StateMachineBuilderFactory() {
    }

    public static <S, E, C> StateMachineBuilder<S, E, C> create() {
        return new StateMachineBuilderImpl();
    }
}
