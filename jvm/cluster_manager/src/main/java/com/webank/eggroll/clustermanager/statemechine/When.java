
package com.webank.eggroll.clustermanager.statemechine;



public interface When<S, E, C> {
    void perform(Action<S, E, C> var1);
}
