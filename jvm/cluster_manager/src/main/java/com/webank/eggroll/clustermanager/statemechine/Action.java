
package com.webank.eggroll.clustermanager.statemechine;

public interface Action<S, E, C> {
    void execute(S var1, S var2, E var3, C var4);
}
