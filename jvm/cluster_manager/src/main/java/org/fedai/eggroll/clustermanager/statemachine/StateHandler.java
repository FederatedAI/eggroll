package org.fedai.eggroll.clustermanager.statemachine;

import org.fedai.eggroll.core.config.Dict;
import org.fedai.eggroll.core.context.Context;


interface StateHandler<T> {

    T prepare(Context context, T data, String preStateParam, String desStateParam);

    //         @Transactional
    T handle(Context context, T data, String preStateParam, String desStateParam);

    default boolean needAsynPostHandle(Context context) {
        return context.getData(Dict.OPEN_ASYN_POST_HANDLE) != null ? (Boolean) context.getData(Dict.OPEN_ASYN_POST_HANDLE) : false;
    }

    default void setIsBreak(Context context, boolean isBreak) {
        context.putData(Dict.IS_BREAK, isBreak);
    }

    default boolean isBreak(Context context) {
        return context.getData(Dict.IS_BREAK) != null ? (Boolean) context.getData(Dict.IS_BREAK) : false;
    }

    default void openAsynPostHandle(Context context) {
        context.putData(Dict.OPEN_ASYN_POST_HANDLE, true);
    }

    default void asynPostHandle(Context context, T data, String preStateParam, String desStateParam) {
    }

    ;

}