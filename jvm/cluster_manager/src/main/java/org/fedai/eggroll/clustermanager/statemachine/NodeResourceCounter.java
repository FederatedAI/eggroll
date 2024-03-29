//package com.webank.eggroll.clustermanager.statemechine;
//
//import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
//import org.fedai.core.config.Dict;
//import org.fedai.core.config.MetaInfo;
//import org.fedai.core.constant.ResourceStatus;
//import org.fedai.core.constant.ResourceType;
//import org.fedai.core.context.Context;
//import org.fedai.core.pojo.ErProcessor;
//import org.fedai.core.pojo.ErResource;
//import org.fedai.core.pojo.ErServerNode;
//import com.google.common.collect.Lists;
//import com.google.common.collect.Maps;
//import org.fedai.impl.dao.clustermanager.eggroll.NodeResourceService;
//import org.fedai.impl.dao.clustermanager.eggroll.ProcessorResourceService;
//import org.fedai.entity.clustermanager.eggroll.ProcessorResource;
//import org.fedai.entity.clustermanager.eggroll.ServerNode;
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
