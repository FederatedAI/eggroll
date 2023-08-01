
package com.webank.eggroll.clustermanager.statemechine;
import java.util.Collection;
import java.util.List;

public class StateImpl<S, E, C> implements State<S, E, C> {
    protected final S stateId;
    private EventTransitions eventTransitions = new EventTransitions();

    StateImpl(S stateId) {
        this.stateId = stateId;
    }

    public Transition<S, E, C> addTransition(E event, State<S, E, C> target, TransitionType transitionType) {
        Transition<S, E, C> newTransition = new TransitionImpl();
        newTransition.setSource(this);
        newTransition.setTarget(target);
        newTransition.setEvent(event);
        newTransition.setType(transitionType);

        this.eventTransitions.put(event, newTransition);
        return newTransition;
    }

    public List<Transition<S, E, C>> getEventTransitions(E event) {
        return this.eventTransitions.get(event);
    }

    public Collection<Transition<S, E, C>> getAllTransitions() {
        return this.eventTransitions.allTransitions();
    }

    public S getId() {
        return this.stateId;
    }



//    public String accept(Visitor visitor) {
//        String entry = visitor.visitOnEntry(this);
//        String exit = visitor.visitOnExit(this);
//        return entry + exit;
//    }

    public boolean equals(Object anObject) {
        if (anObject instanceof State) {
            State other = (State)anObject;
            if (this.stateId.equals(other.getId())) {
                return true;
            }
        }

        return false;
    }

    public String toString() {
        return this.stateId.toString();
    }
}
