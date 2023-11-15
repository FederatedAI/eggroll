package com.webank.eggroll.clustermanager.statemachine;

import org.fedai.eggroll.core.context.Context;

public interface Callback<T> {
    public void callback(Context context, T t);
}
