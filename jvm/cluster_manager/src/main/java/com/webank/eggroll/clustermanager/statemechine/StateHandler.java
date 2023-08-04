package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.context.Context;

interface  StateHandler<T>{

         T  prepare(Context context, T data , String preStateParam, String desStateParam);
         T  handle(Context context, T data , String preStateParam, String desStateParam);
         default  boolean needAsynPostHandle(){return false;}
         default  void asynPostHandle(Context context, T data , String preStateParam, String desStateParam){};

    }