

package com.webank.eggroll.clustermanager.statemechine;


import java.util.Map;

public class StateHelper {
    public StateHelper() {
    }

    public static <S, E, C> State<S, E, C> getState(Map<S, State<S, E, C>> stateMap, S stateId) {
        State<S, E, C> state = (State)stateMap.get(stateId);
        if (state == null) {
            state = new StateImpl(stateId);
            stateMap.put(stateId, state);
        }

        return (State)state;
    }
}
