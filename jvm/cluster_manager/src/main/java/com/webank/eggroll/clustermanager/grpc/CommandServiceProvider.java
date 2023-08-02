package com.webank.eggroll.clustermanager.grpc;

import com.eggroll.core.pojo.ErServerCluster;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.pojo.ErStore;
import com.eggroll.core.pojo.RpcMessage;
import com.webank.eggroll.core.command.Command;
import com.webank.eggroll.core.command.CommandServiceGrpc;
import io.grpc.stub.StreamObserver;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CommandServiceProvider  extends CommandServiceGrpc.CommandServiceImplBase implements InitializingBean {

    static  class  InvokeInfo{
        Object   object;
        Method   method;
        Class    paramClass;
    }

     public void call(Command.CommandRequest request,
                      StreamObserver<Command.CommandResponse> responseObserver){
//
        //responseObserver.onNext();
        responseObserver.onCompleted();
    }




    private ConcurrentHashMap<String ,InvokeInfo>   uriMap = new ConcurrentHashMap();

    public  Object  dispatch(String uri ,byte[] data){
        InvokeInfo invokeInfo =  uriMap.get(uri);
        try {
            RpcMessage  rpcMessage = (RpcMessage)invokeInfo.paramClass.newInstance();
            rpcMessage.deserialize(data);
            return invokeInfo.method.invoke(invokeInfo.object,rpcMessage);

        } catch (InstantiationException e) {
            e.printStackTrace();
        } catch (IllegalAccessException e) {
            e.printStackTrace();

        } catch (InvocationTargetException e) {
            e.printStackTrace();
        }
        return null;

    }

    @URI(value="v1/cluster-manager/metadata/getServerNode")
    public ErServerNode getServerNodeServiceName(ErServerNode  erServerNode){
        return  null;
    }
    @URI(value="v1/cluster-manager/metadata/getServerNodes")
    public ErServerCluster getServerNodesServiceName(ErServerNode  erServerNode){
        return  null;
    }
    @URI(value="v1/cluster-manager/metadata/getOrCreateServerNode")
    public ErServerNode  getOrCreateServerNode(ErServerNode  erServerNode){
        return null;
    }
    @URI(value="v1/cluster-manager/metadata/createOrUpdateServerNode")
    public  ErServerNode createOrUpdateServerNode (ErServerNode  erServerNode){
        return null;
    }

    @URI(value="v1/cluster-manager/metadata/getStore")
    public  ErStore  getStore(ErStore erStore){
        return null;
    }

    @URI(value="v1/cluster-manager/metadata/getOrCreateStore")
    public  ErStore  getOrCreateStore(ErStore  erStore){
        return null;
    }
    @URI(value="v1/cluster-manager/metadata/deleteStore")
    public ErStore deleteStore(ErStore erStore){
        return  null;
    }
    @URI(value="v1/cluster-manager/metadata/getStoreFromNamespace")
    public ErStore getStoreFromNamespace(ErStore  erStore){
        return null;
    }







    @Override
    public void afterPropertiesSet() throws Exception {

        System.err.println("command  service provider afterPropertiesSet");

    }

    private void doRegister(String uri,Object  service,Method  method,Class  paramClass){

    }

    private void prepareRegister(Object service) {
        Method[] methods;
        if (service instanceof Class) {
            methods = ((Class) service).getMethods();
        } else {
            methods = service.getClass().getMethods();
        }
        for (Method method : methods) {
            URI uri = method.getAnnotation(URI.class);
            if(uri!=null){
                Class[] types = method.getParameterTypes();
                if(types.length>0){
                   Class  paramClass = types[0];

                   if(paramClass.isAssignableFrom(RpcMessage.class)){
                       doRegister(uri.value(),service,method,paramClass);
                   }
                }

            }

        }

    }



}
