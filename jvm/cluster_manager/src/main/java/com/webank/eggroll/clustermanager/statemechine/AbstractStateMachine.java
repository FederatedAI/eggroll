package com.webank.eggroll.clustermanager.statemechine;



import com.eggroll.core.context.Context;
import com.eggroll.core.pojo.ErSessionMeta;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.annotation.Transactional;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractStateMachine<T> {

    Logger logger = LoggerFactory.getLogger(AbstractStateMachine.class);

    ConcurrentHashMap<String,ReentrantLock>  lockMap = new ConcurrentHashMap<String,ReentrantLock>();
    ConcurrentHashMap<String,Method>  statueChangeHandlerMap = new ConcurrentHashMap<>();

    public AbstractStateMachine(){
        registeStateHander();
    }
    protected  void registeStateHander(){

        Method[]  methods =  this.getClass().getMethods();
        for (Method method : methods) {
            State state = method.getDeclaredAnnotation(State.class);
            if(state!=null){
               for(String  s: state.value()) {
                   if(statueChangeHandlerMap.contains(s)){
                       throw  new RuntimeException("duplicate state handler "+s);
                   }

                   statueChangeHandlerMap.put(s,method);
               }
            }
        }

    }


    protected String  buildStateChangeLine(String preStateParam,String  desStateParam){
        if(preStateParam==null)
            preStateParam ="";
        return preStateParam+"_"+desStateParam;
    }


    public void  tryLock( String key ){
        ReentrantLock lock  = null;
        if(lockMap.contains(key)){
            lock = lockMap.get(key);
        }else{
            lockMap.putIfAbsent(key,new ReentrantLock());
            lock  = lockMap.get(key);
        }
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
    abstract  public T prepare(T t);
    @Transactional
    public  T   changeStatus(Context context , T t, String preStateParam, String desStateParam){
        String statusLine = buildStateChangeLine(preStateParam,desStateParam);
        Method handleMethod =  statueChangeHandlerMap.get(statusLine);
        if(handleMethod==null){
            throw new RuntimeException("nonono");
        }
        String  lockKey =  getLockKey(t);
        try{
            tryLock(lockKey);
            return   (T)handleMethod.invoke(this,context,t,preStateParam,desStateParam);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            unLock(lockKey);
        }

        return null;

    }

}
