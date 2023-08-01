
package com.webank.eggroll.clustermanager.statemechine;

public interface ExternalTransitionBuilder<S, E, C> {
    From<S, E, C> from(S var1);
}
