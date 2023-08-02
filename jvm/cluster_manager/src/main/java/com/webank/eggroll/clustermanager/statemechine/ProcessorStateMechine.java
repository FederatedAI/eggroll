package com.webank.eggroll.clustermanager.statemechine;

import com.eggroll.core.pojo.ErProcessor;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
@Service
public class ProcessorStateMechine {

    ConcurrentHashMap<String, ReentrantLock>   sessionLockMap = new ConcurrentHashMap<String,ReentrantLock>();

//    @Autowired
//    ProcessorDao

    public  void   changeStatus(ErProcessor paramProcessor, String preStateParam, String desStateParam){

        String  sessionId =  paramProcessor.getSessionId();

        try{
            tryLock(sessionId);
            //        var  erProcessor = paramProcessor
//        var  beginTimeStamp = System.currentTimeMillis()
//        var  preState=preStateParam
//        var  processorType = erProcessor.processorType
//        if(preState==null){
//        var processorsInDb =   serverNodeCrudOperator.queryProcessor(connection,erProcessor.copy(status=null))
//        if(processorsInDb.length==0){
//        logError(s"can not found processor , ${erProcessor}")
//        throw new ErProcessorException(s"can not found processor id ${erProcessor.id}")
//        }else{
//        preState = processorsInDb.apply(0).status
//        processorType = processorsInDb.apply(0).processorType
//        }
//        }
//        var statusLine =  preState+"_"+desStateParam;
//
//        var desErProcessor = erProcessor.copy(status = desStateParam)
//        var dispatchConfig =StaticErConf.getProperty(EGGROLL_SESSION_USE_RESOURCE_DISPATCH, "false")
        if(StringUtils.isEmpty(preStateParam)){


        }




        }finally {
            unLock(sessionId);
        }

    }


    public void  tryLock(String sessionId ){
        ReentrantLock  lock  = null;
        if(sessionLockMap.contains(sessionId)){
            lock = sessionLockMap.get(sessionId);
        }else{
            sessionLockMap.putIfAbsent(sessionId,new ReentrantLock());
            lock  = sessionLockMap.get(sessionId);
        }
        lock.lock();
    }

    public void unLock(String sessionId ){
        ReentrantLock  lock = sessionLockMap.get(sessionId);
        if(lock!=null){
            lock.unlock();
            sessionLockMap.remove(sessionId);
        }
    }







}



//    def  changeStatus(  paramProcessor: ErProcessor,
//                        preStateParam:String=null,desStateParam:String,connection: Connection=null ):Unit =synchronized{
//        var  erProcessor = paramProcessor
//        var  beginTimeStamp = System.currentTimeMillis()
//        var  preState=preStateParam
//        var  processorType = erProcessor.processorType
//        if(preState==null){
//        var processorsInDb =   serverNodeCrudOperator.queryProcessor(connection,erProcessor.copy(status=null))
//        if(processorsInDb.length==0){
//        logError(s"can not found processor , ${erProcessor}")
//        throw new ErProcessorException(s"can not found processor id ${erProcessor.id}")
//        }else{
//        preState = processorsInDb.apply(0).status
//        processorType = processorsInDb.apply(0).processorType
//        }
//        }
//        var statusLine =  preState+"_"+desStateParam;
//
//        var desErProcessor = erProcessor.copy(status = desStateParam)
//        var dispatchConfig =StaticErConf.getProperty(EGGROLL_SESSION_USE_RESOURCE_DISPATCH, "false")
//        statusLine match {
//        case "_NEW" =>
//
//        erProcessor = createNewProcessor(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
//        if( dispatchConfig=="true"||processorType=="DeepSpeed")
//        ResourceStateMachine.changeState(conn, Array(erProcessor), ResourceStatus.INIT, ResourceStatus.PRE_ALLOCATED)
//        })
//
//        case "NEW_RUNNING"=>updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
//        if( dispatchConfig=="true"||processorType=="DeepSpeed")
//        ResourceStateMachine.changeState(conn, Array(erProcessor), ResourceStatus.PRE_ALLOCATED, ResourceStatus.ALLOCATED)
//        })
//        case statusLine if(statusLine=="NEW_STOPPED"||statusLine=="NEW_KILLED"||statusLine=="NEW_ERROR") =>updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
//        if(  dispatchConfig=="true"||processorType=="DeepSpeed")
//        ResourceStateMachine.changeState(conn,Array(erProcessor),ResourceStatus.PRE_ALLOCATED,ResourceStatus.ALLOCATE_FAILED)
//        })
//        case statusLine if(statusLine=="RUNNING_FINISHED"||statusLine=="RUNNING_STOPPED"||statusLine=="RUNNING_KILLED"||statusLine=="RUNNING_ERROR")=>
//        updateState(desErProcessor,connection= connection,afterCall = (conn,erProcessor)=>{
//        if(dispatchConfig=="true"||processorType=="DeepSpeed")
//        ResourceStateMachine.changeState(conn,Array(erProcessor),ResourceStatus.ALLOCATED,ResourceStatus.RETURN)
//        })
//        case _=> logInfo(s"there is no need to do something with ${erProcessor.id} state ${statusLine}");
//        }
//        if( dispatchConfig=="false"&&processorType != "DeepSpeed"){
//        logInfo(s"resource dispatch config is ${dispatchConfig}, processor ${erProcessor.id}  without resource change, ")
//        }
//        logInfo(s"processor ${erProcessor.id} change status ${statusLine} cost time ${System.currentTimeMillis()-beginTimeStamp}")
//        }
