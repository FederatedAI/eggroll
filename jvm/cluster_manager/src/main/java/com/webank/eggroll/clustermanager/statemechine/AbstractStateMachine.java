package com.webank.eggroll.clustermanager.statemechine;



import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

public abstract class AbstractStateMachine<T> {

    ConcurrentHashMap<String,ReentrantLock>  lockMap = new ConcurrentHashMap<String,ReentrantLock>();

    protected String  buildStateChangeLine(String preStateParam,String  desStateParam){
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
    abstract  protected void  doChangeStatus(T t, String preStateParam, String desStateParam);
    abstract  public T prepare(T t);

    public  void   changeStatus(T t, String preStateParam, String desStateParam){

        String  lockKey =  getLockKey(t);
        try{
            tryLock(lockKey);
            doChangeStatus( t,  preStateParam,  desStateParam);
        }finally {
            unLock(lockKey);
        }

    }

}