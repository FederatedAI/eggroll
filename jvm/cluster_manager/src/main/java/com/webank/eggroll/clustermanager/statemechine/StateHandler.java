package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.context.Context;

interface  StateHandler{
         Object  handle(Context context, Object data , String preStateParam, String desStateParam);
    }