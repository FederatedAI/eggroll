//package com.webank.eggroll.clustermanager.statemechine;
//
//import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
//import com.eggroll.core.config.Dict;
//import com.eggroll.core.config.MetaInfo;
//import com.eggroll.core.constant.ResourceStatus;
//import com.eggroll.core.constant.ResourceType;
//import com.eggroll.core.context.Context;
//import com.eggroll.core.pojo.ErProcessor;
//import com.eggroll.core.pojo.ErResource;
//import com.eggroll.core.pojo.ErServerNode;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import com.webank.eggroll.clustermanager.dao.impl.NodeResourceService;
//import com.webank.eggroll.clustermanager.dao.impl.ProcessorResourceService;
//import com.webank.eggroll.clustermanager.entity.ProcessorResource;
//import com.webank.eggroll.clustermanager.entity.ServerNode;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//
//import java.util.List;
//import java.util.Map;
//
//public class NodeResourceCounter implements  Callback{
//
//    @Autowired
//    ProcessorResourceService processorResourceService;
//
//    @Autowired
//    NodeResourceService nodeResourceService;
//
//    protected   boolean  checkNeedChangeResource(Context context){
//        return  context.isNeedDispatchResource();
//    }
//
//
//
//
//    @Override
//    public void callback(Context context, Object o) {
//        if(checkNeedChangeResource(context)) {
//            List<ErServerNode> serverNodes = (List<ErServerNode>) context.getData(Dict.SERVER_NODES);
//            if (serverNodes != null) {
//                for (ErServerNode erServerNode : serverNodes) {
//                    countAndUpdateNodeResource(erServerNode.getId());
//                }
//            }
//        }
//    }
//}
