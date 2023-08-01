

package com.webank.eggroll.clustermanager.statemechine;


public interface To<S, E, C> {
    On<S, E, C> on(E var1);
}
