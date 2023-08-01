

package com.webank.eggroll.clustermanager.statemechine;


public interface Transition<S, E, C> {
    State<S, E, C> getSource();

    void setSource(State<S, E, C> var1);

    E getEvent();

    void setEvent(E var1);

    void setType(TransitionType var1);

    State<S, E, C> getTarget();

    void setTarget(State<S, E, C> var1);

    Condition<C> getCondition();

    void setCondition(Condition<C> var1);

    Action<S, E, C> getAction();

    void setAction(Action<S, E, C> var1);

    State<S, E, C> transit(C var1, boolean var2);

    void verify();
}
