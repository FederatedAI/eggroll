

package com.webank.eggroll.clustermanager.statemechine;

public interface InternalTransitionBuilder<S, E, C> {
    To<S, E, C> within(S var1);
}
