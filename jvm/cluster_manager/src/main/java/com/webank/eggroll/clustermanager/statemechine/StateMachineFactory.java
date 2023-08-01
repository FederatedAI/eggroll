
package com.webank.eggroll.clustermanager.statemechine;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class StateMachineFactory {
    static Map<String, StateMachine> stateMachineMap = new ConcurrentHashMap();

    public StateMachineFactory() {
    }

    public static <S, E, C> void register(StateMachine<S, E, C> stateMachine) {
        String machineId = stateMachine.getMachineId();
        if (stateMachineMap.get(machineId) != null) {
            throw new RuntimeException("The state machine with id [" + machineId + "] is already built, no need to build again");
        } else {
            stateMachineMap.put(stateMachine.getMachineId(), stateMachine);
        }
    }

    public static <S, E, C> StateMachine<S, E, C> get(String machineId) {
        StateMachine stateMachine = (StateMachine)stateMachineMap.get(machineId);
        if (stateMachine == null) {
            throw new RuntimeException("There is no stateMachine instance for " + machineId + ", please build it first");
        } else {
            return stateMachine;
        }
    }
}
