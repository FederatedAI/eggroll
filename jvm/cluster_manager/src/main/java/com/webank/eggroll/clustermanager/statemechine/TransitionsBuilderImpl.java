
package com.webank.eggroll.clustermanager.statemechine;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class TransitionsBuilderImpl<S, E, C> extends TransitionBuilderImpl<S, E, C> implements ExternalTransitionsBuilder<S, E, C> {
    private List<State<S, E, C>> sources = new ArrayList();
    private List<Transition<S, E, C>> transitions = new ArrayList();

    public TransitionsBuilderImpl(Map<S, State<S, E, C>> stateMap, TransitionType transitionType) {
        super(stateMap, transitionType);
    }

    public From<S, E, C> fromAmong(S... stateIds) {
        Object[] var2 = stateIds;
        int var3 = stateIds.length;

        for(int var4 = 0; var4 < var3; ++var4) {
            S stateId = (S)var2[var4];
            this.sources.add(StateHelper.getState(super.stateMap, stateId));
        }

        return this;
    }

    public On<S, E, C> on(E event) {
        Iterator var2 = this.sources.iterator();

        while(var2.hasNext()) {
            State source = (State)var2.next();
            Transition transition = source.addTransition(event, super.target, super.transitionType);
            this.transitions.add(transition);
        }

        return this;
    }

    public When<S, E, C> when(Condition<C> condition) {
        Iterator var2 = this.transitions.iterator();

        while(var2.hasNext()) {
            Transition transition = (Transition)var2.next();
            transition.setCondition(condition);
        }

        return this;
    }

    public void perform(Action<S, E, C> action) {
        Iterator var2 = this.transitions.iterator();

        while(var2.hasNext()) {
            Transition transition = (Transition)var2.next();
            transition.setAction(action);
        }

    }
}
