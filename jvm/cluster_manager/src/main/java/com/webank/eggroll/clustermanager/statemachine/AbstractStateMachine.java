package com.webank.eggroll.clustermanager.statemachine;


import com.eggroll.core.context.Context;
import org.springframework.transaction.annotation.Transactional;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractStateMachine<T> {

//    Logger logger = LoggerFactory.getLogger(AbstractStateMachine.class);

    ConcurrentHashMap<String,ReentrantLock>  lockMap = new ConcurrentHashMap<>();
    ConcurrentHashMap<String,StateHandler<T>>  statueChangeHandlerMap = new ConcurrentHashMap<>();

    ThreadPoolExecutor   asynThreadPool =   new ThreadPoolExecutor(5,5,1, TimeUnit.SECONDS,new LinkedBlockingDeque<>(10));

    public AbstractStateMachine(){

    }
    abstract String  buildStateChangeLine(Context context , T t, String preStateParam, String desStateParam);

    protected  void registeStateHander(String  statusLine,StateHandler<T> handler){

        if(statueChangeHandlerMap.containsKey(statusLine)){
            throw  new RuntimeException("duplicate state handler "+statusLine);
        }
        statueChangeHandlerMap.put(statusLine,handler);

//        Method[]  methods =  this.getClass().getMethods();
//        for (Method method : methods) {
//            State state = method.getDeclaredAnnotation(State.class);
//            if(state!=null){
//               for(String  s: state.value()) {
//                   if(statueChangeHandlerMap.contains(s)){
//                       throw  new RuntimeException("duplicate state handler "+s);
//                   }
//
//                   statueChangeHandlerMap.put(s,method);
//               }
//            }
//        }

    }





    public void  tryLock( String key ){
        ReentrantLock lock;
        if (!lockMap.containsKey(key)) {
            lockMap.putIfAbsent(key, new ReentrantLock());
        }
        lock = lockMap.get(key);
        lock.lock();
    }

    public void unLock(String key ){
        ReentrantLock  lock = lockMap.get(key);
        if(lock!=null){
            lock.unlock();
            lockMap.remove(key);
        }
    }


    abstract  public String  getLockKey(T t);
    //abstract  protected T  doChangeStatus(Context context ,T t, String preStateParam, String desStateParam);
//    abstract  public T prepare(T t);

    public  T   changeStatus(Context context , T t, String preStateParam, String desStateParam){
        return    changeStatus( context ,  t,  preStateParam,  desStateParam ,null);
    }

    @Transactional
    public  T   changeStatus(Context context , T t, String preStateParam, String desStateParam ,Callback<T> callback){
        String statusLine = buildStateChangeLine(context,t,preStateParam,desStateParam);
//        t = prepare(t);
        StateHandler<T> handler =  statueChangeHandlerMap.get(statusLine);
        if(handler==null){
            throw new RuntimeException("statusLine's handler not found ");
        }
        String  lockKey =  getLockKey(t);
        try{
            tryLock(lockKey);
            T result= handler.prepare(context,t,preStateParam,desStateParam);
            if(!handler.isBreak(context)) {
                result = handler.handle(context, result, preStateParam, desStateParam);
                if(callback!=null) {
                    callback.callback(context, result);
                }
                if(!handler.isBreak(context)) {
                    if (handler.needAsynPostHandle(context)) {
                        T finalResult = result;
                        asynThreadPool.submit(() -> handler.asynPostHandle(context, finalResult, preStateParam, desStateParam));
                    }
                }
            }
            return result;
        } catch (Exception e) {
           throw  new RuntimeException(e);
        } finally {
            unLock(lockKey);
        }
    }

}
