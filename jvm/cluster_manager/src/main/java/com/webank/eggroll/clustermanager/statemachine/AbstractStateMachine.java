package com.webank.eggroll.clustermanager.statemachine;


import com.eggroll.core.config.MetaInfo;
import com.eggroll.core.context.Context;
import com.eggroll.core.utils.LockUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.mybatis.guice.transactional.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractStateMachine<T> {

    Logger logger = LoggerFactory.getLogger(AbstractStateMachine.class);
    public static final String IGNORE = "IGNORE";

    Cache<String, ReentrantLock> stateLockCache = CacheBuilder.newBuilder()
            .maximumSize(3000)
            .expireAfterWrite(MetaInfo.EGGROLL_SESSION_MAX_LIVE_MS, TimeUnit.MILLISECONDS)
            .build();

    ConcurrentHashMap<String, StateHandler<T>> statueChangeHandlerMap = new ConcurrentHashMap<>();
    ThreadPoolExecutor asynThreadPool = new ThreadPoolExecutor(5, 5, 1, TimeUnit.SECONDS, new LinkedBlockingDeque<>(10));

    public AbstractStateMachine() {

    }

    abstract String buildStateChangeLine(Context context, T t, String preStateParam, String desStateParam);

    protected void registeStateHander(String statusLine, StateHandler<T> handler) {

        if (statueChangeHandlerMap.containsKey(statusLine)) {
            throw new RuntimeException("duplicate state handler " + statusLine);
        }
        statueChangeHandlerMap.put(statusLine, handler);

    }

    abstract public String getLockKey(Context context, T t);

    public T changeStatus(Context context, T t, String preStateParam, String desStateParam) {
        return changeStatus(context, t, preStateParam, desStateParam, null);
    }


    public T changeStatus(Context context, T t, String preStateParam, String desStateParam, Callback<T> callback) {
        String statusLine = buildStateChangeLine(context, t, preStateParam, desStateParam);

        StateHandler<T> handler = statueChangeHandlerMap.get(statusLine);
        if (handler == null) {
            handler = statueChangeHandlerMap.get(IGNORE);
        }
        if (handler == null) {
            logger.error("wrong status line {} ", statusLine);
            throw new RuntimeException("no status handler found for " + statusLine);
        }
        String lockKey = getLockKey(context, t);
        try {
            LockUtils.lock(stateLockCache, lockKey);
            T result = handler.prepare(context, t, preStateParam, desStateParam);
            if (!handler.isBreak(context)) {
                result = transactionHandle(context, handler, result, preStateParam, desStateParam, callback);
                if (!handler.isBreak(context)) {
                    if (handler.needAsynPostHandle(context)) {
                        T finalResult = result;
                        StateHandler<T> finalHandler = handler;
                        asynThreadPool.submit(() -> finalHandler.asynPostHandle(context, finalResult, preStateParam, desStateParam));
                    }
                }
            }
            return result;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        } finally {
            LockUtils.unLock(stateLockCache, lockKey);
        }
    }

    @Transactional
    private T transactionHandle(Context context, StateHandler<T> handler, T result, String preStateParam, String desStateParam, Callback<T> callback) {
        result = handler.handle(context, result, preStateParam, desStateParam);
        if (callback != null) {
            callback.callback(context, result);
        }
        return result;
    }

}
