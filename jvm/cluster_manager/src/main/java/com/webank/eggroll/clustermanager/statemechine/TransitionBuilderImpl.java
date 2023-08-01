//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.webank.eggroll.clustermanager.statemechine;


import java.util.Map;

class TransitionBuilderImpl<S, E, C> implements ExternalTransitionBuilder<S, E, C>, InternalTransitionBuilder<S, E, C>, From<S, E, C>, On<S, E, C>, To<S, E, C> {
    final Map<S, State<S, E, C>> stateMap;
    private State<S, E, C> source;
    protected State<S, E, C> target;
    private Transition<S, E, C> transition;
    final TransitionType transitionType;

    public TransitionBuilderImpl(Map<S, State<S, E, C>> stateMap, TransitionType transitionType) {
        this.stateMap = stateMap;
        this.transitionType = transitionType;
    }

    public From<S, E, C> from(S stateId) {
        this.source = StateHelper.getState(this.stateMap, stateId);
        return this;
    }

    public To<S, E, C> to(S stateId) {
        this.target = StateHelper.getState(this.stateMap, stateId);
        return this;
    }

    public To<S, E, C> within(S stateId) {
        this.source = this.target = StateHelper.getState(this.stateMap, stateId);
        return this;
    }

    public When<S, E, C> when(Condition<C> condition) {
        this.transition.setCondition(condition);
        return this;
    }

    public On<S, E, C> on(E event) {
        this.transition = this.source.addTransition(event, this.target, this.transitionType);
        return this;
    }

    public void perform(Action<S, E, C> action) {
        this.transition.setAction(action);
    }
}
