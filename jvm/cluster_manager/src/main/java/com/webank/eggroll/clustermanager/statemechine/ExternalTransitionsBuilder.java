
package com.webank.eggroll.clustermanager.statemechine;



public interface ExternalTransitionsBuilder<S, E, C> {
    From<S, E, C> fromAmong(S... var1);
}
