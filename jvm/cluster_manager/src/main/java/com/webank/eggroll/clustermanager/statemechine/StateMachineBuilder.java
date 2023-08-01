
package com.webank.eggroll.clustermanager.statemechine;



public interface StateMachineBuilder<S, E, C> {
    ExternalTransitionBuilder<S, E, C> externalTransition();

    ExternalTransitionsBuilder<S, E, C> externalTransitions();

    InternalTransitionBuilder<S, E, C> internalTransition();

    StateMachine<S, E, C> build(String var1);
}
