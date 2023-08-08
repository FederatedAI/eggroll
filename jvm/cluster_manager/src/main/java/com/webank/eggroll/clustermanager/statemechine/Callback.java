package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.context.Context;

public interface Callback <T>{
    public  void  callback(Context context , T t);
}
