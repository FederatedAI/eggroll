
package com.webank.eggroll.clustermanager.statemechine;


import java.util.Collection;
import java.util.List;

public interface State<S, E, C> {
    S getId();

    Transition<S, E, C> addTransition(E var1, State<S, E, C> var2, TransitionType var3);

    List<Transition<S, E, C>> getEventTransitions(E var1);

    Collection<Transition<S, E, C>> getAllTransitions();
}
