
package com.webank.eggroll.clustermanager.statemechine;





import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class StateMachineImpl<S, E, C> implements StateMachine<S, E, C> {
    private String machineId;
    private final Map<S, State<S, E, C>> stateMap;
    private boolean ready;

    public StateMachineImpl(Map<S, State<S, E, C>> stateMap) {
        this.stateMap = stateMap;
    }

    public S fireEvent(S sourceStateId, E event, C ctx) {
        this.isReady();
        Transition<S, E, C> transition = this.routeTransition(sourceStateId, event, ctx);
        if (transition == null) {

            return sourceStateId;
        } else {
            return transition.transit(ctx, false).getId();
        }
    }

    private Transition<S, E, C> routeTransition(S sourceStateId, E event, C ctx) {
        State sourceState = this.getState(sourceStateId);
        List<Transition<S, E, C>> transitions = sourceState.getEventTransitions(event);
        if (transitions != null && transitions.size() != 0) {
            Transition<S, E, C> transit = null;
            Iterator var7 = transitions.iterator();

            while(var7.hasNext()) {
                Transition<S, E, C> transition = (Transition)var7.next();
                if (transition.getCondition() == null) {
                    transit = transition;
                } else if (transition.getCondition().isSatisfied(ctx)) {
                    transit = transition;
                    break;
                }
            }

            return transit;
        } else {
            return null;
        }
    }

    private State getState(S currentStateId) {
        State state = StateHelper.getState(this.stateMap, currentStateId);
        if (state == null) {
//            this.showStateMachine();
            throw new RuntimeException(currentStateId + " is not found, please check state machine");
        } else {
            return state;
        }
    }

    private void isReady() {
        if (!this.ready) {
            throw new RuntimeException("State machine is not built yet, can not work");
        }
    }

//    public String accept(Visitor visitor) {
//        StringBuilder sb = new StringBuilder();
//        sb.append(visitor.visitOnEntry(this));
//        Iterator var3 = this.stateMap.values().iterator();
//
//        while(var3.hasNext()) {
//            State state = (State)var3.next();
//            sb.append(state.accept(visitor));
//        }
//
//        sb.append(visitor.visitOnExit(this));
//        return sb.toString();
//    }



    public String getMachineId() {
        return this.machineId;
    }

    public void setMachineId(String machineId) {
        this.machineId = machineId;
    }

    public void setReady(boolean ready) {
        this.ready = ready;
    }
}
