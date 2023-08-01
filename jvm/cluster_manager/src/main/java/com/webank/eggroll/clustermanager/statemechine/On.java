
package com.webank.eggroll.clustermanager.statemechine;

public interface On<S, E, C> extends When<S, E, C> {
    When<S, E, C> when(Condition<C> var1);
}
