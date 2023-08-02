package com.webank.eggroll.clustermanager.grpc;

import com.eggroll.core.pojo.ErServerCluster;
import com.eggroll.core.pojo.ErServerNode;
import com.eggroll.core.pojo.ErStore;
import com.eggroll.core.pojo.RpcMessage;
import org.apache.ibatis.annotations.Param;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class CommandServiceProvider  implements InitializingBean {

    static  class  InvokeInfo{
        Object   object;
        Method   method;
        Class    paramClass;
    }

    private ConcurrentHashMap<String ,InvokeInfo>   uriMap = new ConcurrentHashMap();

    public  void  dispath(){


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
        this.getClass().getMethods();
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
                       doRegister(method);
                   }
                }

            }

        }

    }


//     CommandRouter.register(serviceName = MetadataCommands.getServerNodeServiceName,
//    serviceParamTypes = Array(classOf[ErServerNode]),
//    serviceResultTypes = Array(classOf[ErServerNode]),
//    routeToClass = classOf[ServerNodeCrudOperator],
//    routeToMethodName = MetadataCommands.getServerNode)
//
//            CommandRouter.register(serviceName = MetadataCommands.getServerNodesServiceName,
//    serviceParamTypes = Array(classOf[ErServerNode]),
//    serviceResultTypes = Array(classOf[ErServerCluster]),
//    routeToClass = classOf[ServerNodeCrudOperator],
//    routeToMethodName = MetadataCommands.getServerNodes)
//
//            CommandRouter.register(serviceName = MetadataCommands.getOrCreateServerNodeServiceName,
//    serviceParamTypes = Array(classOf[ErServerNode]),
//    serviceResultTypes = Array(classOf[ErServerNode]),
//    routeToClass = classOf[ServerNodeCrudOperator],
//    routeToMethodName = MetadataCommands.getOrCreateServerNode)
//
//            CommandRouter.register(serviceName = MetadataCommands.createOrUpdateServerNodeServiceName,
//    serviceParamTypes = Array(classOf[ErServerNode]),
//    serviceResultTypes = Array(classOf[ErServerNode]),
//    routeToClass = classOf[ServerNodeCrudOperator],
//    routeToMethodName = MetadataCommands.createOrUpdateServerNode)
//
//            CommandRouter.register(serviceName = MetadataCommands.getStoreServiceName,
//    serviceParamTypes = Array(classOf[ErStore]),
//    serviceResultTypes = Array(classOf[ErStore]),
//    routeToClass = classOf[StoreCrudOperator],
//    routeToMethodName = MetadataCommands.getStore)
//
//            CommandRouter.register(serviceName = MetadataCommands.getOrCreateStoreServiceName,
//    serviceParamTypes = Array(classOf[ErStore]),
//    serviceResultTypes = Array(classOf[ErStore]),
//    routeToClass = classOf[StoreCrudOperator],
//    routeToMethodName = MetadataCommands.getOrCreateStore)
//
//            CommandRouter.register(serviceName = MetadataCommands.deleteStoreServiceName,
//    serviceParamTypes = Array(classOf[ErStore]),
//    serviceResultTypes = Array(classOf[ErStore]),
//    routeToClass = classOf[StoreCrudOperator],
//    routeToMethodName = MetadataCommands.deleteStore)
//
//            CommandRouter.register(serviceName = MetadataCommands.getStoreFromNamespaceServiceName,
//    serviceParamTypes = Array(classOf[ErStore]),
//    serviceResultTypes = Array(classOf[ErStoreList]),
//    routeToClass = classOf[StoreCrudOperator],
//    routeToMethodName = MetadataCommands.getStoreFromNamespace)
//
//            CommandRouter.register(serviceName = SessionCommands.getSession.uriString,
//    serviceParamTypes = Array(classOf[ErSessionMeta]),
//    serviceResultTypes = Array(classOf[ErSessionMeta]),
//    routeToClass = classOf[SessionManagerService],
//    routeToMethodName = SessionCommands.getSession.getName())
//
//            CommandRouter.register(serviceName = SessionCommands.getOrCreateSession.uriString,
//    serviceParamTypes = Array(classOf[ErSessionMeta]),
//    serviceResultTypes = Array(classOf[ErSessionMeta]),
//    routeToClass = classOf[SessionManagerService],
//    routeToMethodName = SessionCommands.getOrCreateSession.getName())
//
//            CommandRouter.register(serviceName = SessionCommands.stopSession.uriString,
//    serviceParamTypes = Array(classOf[ErSessionMeta]),
//    serviceResultTypes = Array(classOf[ErSessionMeta]),
//    routeToClass = classOf[SessionManagerService],
//    routeToMethodName = SessionCommands.stopSession.getName())
//
//            CommandRouter.register(serviceName = SessionCommands.killSession.uriString,
//    serviceParamTypes = Array(classOf[ErSessionMeta]),
//    serviceResultTypes = Array(classOf[ErSessionMeta]),
//    routeToClass = classOf[SessionManagerService],
//    routeToMethodName = SessionCommands.killSession.getName())
//
//            CommandRouter.register(serviceName = SessionCommands.killAllSessions.uriString,
//    serviceParamTypes = Array(classOf[ErSessionMeta]),
//    serviceResultTypes = Array(classOf[ErSessionMeta]),
//    routeToClass = classOf[SessionManagerService],
//    routeToMethodName = SessionCommands.killAllSessions.getName())
//
//            CommandRouter.register(serviceName = SessionCommands.registerSession.uriString,
//    serviceParamTypes = Array(classOf[ErSessionMeta]),
//    serviceResultTypes = Array(classOf[ErSessionMeta]),
//    routeToClass = classOf[SessionManagerService],
//    routeToMethodName = SessionCommands.registerSession.getName())
//
//            CommandRouter.register(serviceName = SessionCommands.heartbeat.uriString,
//    serviceParamTypes = Array(classOf[ErProcessor]),
//    serviceResultTypes = Array(classOf[ErProcessor]),
//    routeToClass = classOf[SessionManagerService],
//    routeToMethodName = SessionCommands.heartbeat.getName())
//
//            CommandRouter.register(serviceName = ManagerCommands.nodeHeartbeat.uriString,
//    serviceParamTypes = Array(classOf[ErNodeHeartbeat]),
//    serviceResultTypes = Array(classOf[ErNodeHeartbeat]),
//    routeToClass = classOf[ClusterManagerService],
//    routeToMethodName = ManagerCommands.nodeHeartbeat.getName())
//
//            CommandRouter.register(serviceName = ManagerCommands.registerResource.uriString,
//    serviceParamTypes = Array(classOf[ErServerNode]),
//    serviceResultTypes = Array(classOf[ErServerNode]),
//    routeToClass = classOf[ClusterManagerService],
//    routeToMethodName = ManagerCommands.registerResource.getName())
//
//
//            // submit job
//            //JobServiceHandler.startSessionWatcher()
//            CommandRouter.register_handler(serviceName = JobCommands.submitJob.uriString,
//    args => JobServiceHandler.handleSubmit(args(0))
//            )
//            CommandRouter.register_handler(serviceName = JobCommands.queryJobStatus.uriString,
//    args => JobServiceHandler.handleJobStatusQuery(args(0))
//            )
//            CommandRouter.register_handler(serviceName = JobCommands.queryJob.uriString,
//    args => JobServiceHandler.handleJobQuery(args(0))
//            )
//            CommandRouter.register_handler(serviceName = JobCommands.killJob.uriString,
//    args => JobServiceHandler.handleJobKill(args(0))
//            )
//            CommandRouter.register_handler(serviceName = JobCommands.stopJob.uriString,
//    args => JobServiceHandler.handleJobStop(args(0))
//            )
//            CommandRouter.register_handler(serviceName = JobCommands.downloadJob.uriString,
//    args => JobServiceHandler.handleJobDownload(args(0))
//            )
//
//            CommandRouter.register_handler(serviceName = JobCommands.prepareJobDownload.uriString,
//    args => JobServiceHandler.prepareJobDownload(args(0))
//            )
//
//
//
//
//
//    val rendezvousStoreService = new RendezvousStoreService()
//    CommandRouter.register_handler(serviceName = RendezvousStoreCommands.set.uriString,
//    args => rendezvousStoreService.set(args(0))
//            )
//            CommandRouter.register_handler(serviceName = RendezvousStoreCommands.get.uriString,
//    args => rendezvousStoreService.get(args(0))
//            )
//            CommandRouter.register_handler(serviceName = RendezvousStoreCommands.add.uriString,
//    args => rendezvousStoreService.add(args(0))
//            )
//            CommandRouter.register_handler(serviceName = RendezvousStoreCommands.destroy.uriString,
//    args => rendezvousStoreService.destroy(args(0))
//            )
}
