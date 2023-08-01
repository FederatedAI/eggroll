
package com.webank.eggroll.clustermanager.statemechine;


import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateMachineBuilderImpl<S, E, C> implements StateMachineBuilder<S, E, C> {
    private final Map<S, State<S, E, C>> stateMap = new ConcurrentHashMap();
    private final StateMachineImpl<S, E, C> stateMachine;

    public StateMachineBuilderImpl() {
        this.stateMachine = new StateMachineImpl(this.stateMap);
    }

    public ExternalTransitionBuilder<S, E, C> externalTransition() {
        return new TransitionBuilderImpl(this.stateMap, TransitionType.EXTERNAL);
    }

    public ExternalTransitionsBuilder<S, E, C> externalTransitions() {
        return new TransitionsBuilderImpl(this.stateMap, TransitionType.EXTERNAL);
    }

    public InternalTransitionBuilder<S, E, C> internalTransition() {
        return new TransitionBuilderImpl(this.stateMap, TransitionType.INTERNAL);
    }

    public StateMachine<S, E, C> build(String machineId) {
        this.stateMachine.setMachineId(machineId);
        this.stateMachine.setReady(true);
        StateMachineFactory.register(this.stateMachine);
        return this.stateMachine;
    }
}
