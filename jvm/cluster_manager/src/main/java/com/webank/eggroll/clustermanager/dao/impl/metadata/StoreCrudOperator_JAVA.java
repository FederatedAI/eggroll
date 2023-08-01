//package com.webank.eggroll.clustermanager.dao.impl.metadata;
//
//import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
//import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
//import com.webank.eggroll.clustermanager.dao.impl.ServerNodeService;
//import com.webank.eggroll.clustermanager.dao.impl.StoreLocatorService;
//import com.webank.eggroll.clustermanager.dao.impl.StoreOptionService;
//import com.webank.eggroll.clustermanager.dao.impl.StorePartitionService;
//import com.webank.eggroll.clustermanager.entity.ServerNode;
//import com.webank.eggroll.clustermanager.entity.StoreLocator;
//import com.webank.eggroll.clustermanager.entity.StoreOption;
//import com.webank.eggroll.clustermanager.entity.StorePartition;
//import com.webank.eggroll.clustermanager.entity.scala.*;
//import com.webank.eggroll.core.exceptions.CrudException_JAVA;
//import com.webank.eggroll.core.util.JsonUtil;
//import com.webank.eggroll.core.constant.PartitionStatus;
//import com.webank.eggroll.core.constant.ServerNodeStatus;
//import com.webank.eggroll.core.constant.ServerNodeTypes;
//import com.webank.eggroll.core.constant.StoreStatus;
//import org.apache.commons.lang3.StringUtils;
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.springframework.beans.factory.annotation.Autowired;
//import org.springframework.stereotype.Service;
//import org.springframework.transaction.annotation.Transactional;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//import java.util.stream.Collectors;
//
//@Service
//public class StoreCrudOperator_JAVA {
//
//    @Autowired
//    StoreLocatorService storeLocatorService;
//    @Autowired
//    StorePartitionService storePartitionService;
//    @Autowired
//    ServerNodeService serverNodeService;
//    @Autowired
//    StoreOptionService storeOptionService;
//
//    private final Map<Long, Object> nodeIdToNode = new ConcurrentHashMap<>();
//    private static final Logger LOGGER = LogManager.getLogger(StoreCrudOperator_JAVA.class);
//
//    private synchronized ErStore_JAVA doGetStore(ErStore_JAVA input) {
//        Map<String, String> inputOptions = input.getOptions();
//
//        // getting input locator
//        ErStoreLocator_JAVA inputStoreLocator = input.getStoreLocator();
//
//        StoreLocator params = new StoreLocator();
//        params.setNamespace(inputStoreLocator.getNamespace());
//        params.setName(inputStoreLocator.getName());
//        params.setStatus(StoreStatus.NORMAL());
//
//        if (!StringUtils.isBlank(inputStoreLocator.getStoreType())) {
//            params.setStoreType(inputStoreLocator.getStoreType());
//        }
//        List<StoreLocator> storeLocatorResult = storeLocatorService.list(new QueryWrapper<>(params));
//        if (storeLocatorResult.isEmpty()) {
//            return null;
//        }
//        StoreLocator store = storeLocatorResult.get(0);
//        Long storeLocatorId = store.getStoreLocatorId();
//
//        QueryWrapper<StorePartition> queryStorePartition = new QueryWrapper<>();
//        queryStorePartition.lambda().eq(StorePartition::getStoreLocatorId, storeLocatorId)
//                .orderByAsc(StorePartition::getStorePartitionId);
//        List<StorePartition> storePartitionResult = storePartitionService.list(queryStorePartition);
//        if (storePartitionResult.isEmpty()) {
//            throw new IllegalStateException("store locator found but no partition found");
//        }
//
//        List<Long> missingNodeId = new ArrayList<>();
//        List<Long> partitionAtNodeIds = new ArrayList<>();
//
//        for (StorePartition storePartition : storePartitionResult) {
//            Long nodeId = storePartition.getNodeId();
//            if (!nodeIdToNode.containsKey(nodeId)) missingNodeId.add(nodeId);
//            partitionAtNodeIds.add(nodeId);
//        }
//        if (!missingNodeId.isEmpty()) {
//            QueryWrapper<ServerNode> queryServerNode = new QueryWrapper<>();
//            queryServerNode.lambda().eq(ServerNode::getStatus, ServerNodeStatus.HEALTHY())
//                    .eq(ServerNode::getNodeType, ServerNodeTypes.NODE_MANAGER())
//                    .in(ServerNode::getServerNodeId, missingNodeId);
//            List<ServerNode> nodeResult = serverNodeService.list(queryServerNode);
//            if (nodeResult.isEmpty()) {
//                LOGGER.error("No valid node for this store: " + JsonUtil.object2Json(inputStoreLocator) + " missingId " + missingNodeId.toString());
//                throw new IllegalStateException("No valid node for this store: " + JsonUtil.object2Json(inputStoreLocator));
//            }
//
//            for (ServerNode serverNode : nodeResult) {
//                Long serverNodeId = serverNode.getServerNodeId();
//                nodeIdToNode.putIfAbsent(serverNodeId, serverNode);
//            }
//        }
//
//        ErStoreLocator_JAVA outputStoreLocator = new ErStoreLocator_JAVA(store.getStoreLocatorId()
//                , store.getStoreType()
//                , store.getNamespace()
//                , store.getName()
//                , store.getPath()
//                , store.getTotalPartitions()
//                , store.getPartitioner()
//                , store.getSerdes());
//        QueryWrapper<StoreOption> storeOptionWrapper = new QueryWrapper<>();
//        storeOptionWrapper.lambda().eq(StoreOption::getStoreLocatorId, storeLocatorId);
//        List<StoreOption> storeOpts = storeOptionService.list(storeOptionWrapper);
//        ConcurrentHashMap<String, String> outputOptions = new ConcurrentHashMap<>();
//        if (inputOptions != null) {
//            outputOptions.putAll(inputOptions);
//        }
//        if (storeOpts != null) {
//            for (StoreOption r : storeOpts) outputOptions.put(r.getName(), r.getData());
//        }
//
//        // process output partitions
//        List<ErPartition_JAVA> outputPartitions = new ArrayList<>();
//        for (StorePartition p : storePartitionResult) {
//
//            ErProcessor_JAVA erProcessor = new ErProcessor_JAVA();
//            erProcessor.setId(p.getPartitionId());
//            erProcessor.setServerNodeId(p.getNodeId());
//
//            outputPartitions.add(new ErPartition_JAVA(p.getPartitionId()
//                    , outputStoreLocator
//                    , erProcessor
//                    , -1
//            ));
//        }
//        return new ErStore_JAVA(outputStoreLocator, outputPartitions, outputOptions);
//    }
//
//    private ErStore_JAVA doCreateStore(ErStore_JAVA input) {
//        Map<String, String> inputOptions = input.getOptions();
//        ErStoreLocator_JAVA inputStoreLocator = input.getStoreLocator();
//
//        StoreLocator newStoreLocator = new StoreLocator();
//        newStoreLocator.setStoreType(inputStoreLocator.getStoreType());
//        newStoreLocator.setNamespace(inputStoreLocator.getNamespace());
//        newStoreLocator.setName(inputStoreLocator.getName());
//        newStoreLocator.setPath(inputStoreLocator.getPath());
//        newStoreLocator.setTotalPartitions(inputStoreLocator.getTotalPartitions());
//        newStoreLocator.setPartitioner(inputStoreLocator.getPartitioner());
//        newStoreLocator.setSerdes(inputStoreLocator.getSerdes());
//        newStoreLocator.setStatus(StoreStatus.NORMAL());
//        boolean addStoreLocatorFlag = storeLocatorService.save(newStoreLocator);
//        if (!addStoreLocatorFlag) {
//            throw new CrudException_JAVA("Illegal rows affected returned when creating store locator: 0");
//        }
//
//        int newTotalPartitions = inputStoreLocator.getTotalPartitions();
//        List<ErPartition_JAVA> newPartitions = new ArrayList<>();
//        List<ErServerNode_JAVA> serverNodes =null;
//        //kaideng 屏蔽，可以放开
//
////                Arrays.stream(ServerNodeCrudOperator.doGetServerNodes(
////                new ErServerNode_JAVA(
////                        ServerNodeTypes.NODE_MANAGER()
////                        , ServerNodeStatus.HEALTHY()))).sorted(Comparator.comparingLong(ErServerNode_JAVA::id)).collect(Collectors.toList());
//        int nodesCount = serverNodes.size();
//        List<ErPartition_JAVA> specifiedPartitions = input.getPartitions();
//        boolean isPartitionsSpecified = specifiedPartitions.size() > 0;
//        if (newTotalPartitions <= 0) newTotalPartitions = nodesCount << 2;
//        ArrayList<Long> serverNodeIds = new ArrayList<>();
//        for (int i = 0; i < newTotalPartitions; i++) {
//            ErServerNode_JAVA node = serverNodes.get(i % nodesCount);
//            StorePartition nodeRecord = new StorePartition();
//            nodeRecord.setStoreLocatorId(newStoreLocator.getStoreLocatorId());
//            nodeRecord.setNodeId(isPartitionsSpecified ? input.getPartitions().get(i % specifiedPartitions.size()).getProcessor().getServerNodeId() : node.getId());
//            nodeRecord.setPartitionId(i);
//            nodeRecord.setStatus(PartitionStatus.PRIMARY());
//            boolean addNodeRecordFlag = storePartitionService.save(nodeRecord);
//            if (!addNodeRecordFlag) {
//                throw new CrudException_JAVA("Illegal rows affected when creating node: 0");
//            }
//            serverNodeIds.add(node.getId());
//
//            ErProcessor_JAVA binding = new ErProcessor_JAVA();
//            binding.setId(i);
//            binding.setServerNodeId(isPartitionsSpecified ? input.getPartitions().get(i % specifiedPartitions.size()).getProcessor().getServerNodeId() : node.getId());
//            binding.setTag("binding");
//
//            newPartitions.add(new ErPartition_JAVA(i
//                    , inputStoreLocator
//                    ,binding
//                    , -1));
//        }
//        ConcurrentHashMap<String, String> newOptions = new ConcurrentHashMap<>();
//        if (inputOptions != null) newOptions.putAll(inputOptions);
//        for (Map.Entry<String, String> entry : newOptions.entrySet()) {
//            StoreOption newStoreOption = new StoreOption();
//            newStoreOption.setStoreLocatorId(newStoreLocator.getStoreLocatorId());
//            newStoreOption.setName(entry.getKey());
//            newStoreOption.setData(entry.getValue());
//            storeOptionService.save(newStoreOption);
//        }
//        return new ErStore_JAVA(inputStoreLocator, newPartitions, newOptions);
//    }
//
//    @Transactional
//    private ErStore_JAVA doDeleteStore(ErStore_JAVA input) {
//        ErStoreLocator_JAVA inputStoreLocator = input.getStoreLocator();
//        ErStoreLocator_JAVA outputStoreLocator = null;
//        if ("*" .equals(inputStoreLocator.getName())) {
//            UpdateWrapper<StoreLocator> updateWrapper = new UpdateWrapper<>();
//            updateWrapper.lambda().setSql("name = concat(name, " + System.currentTimeMillis() + ")")
//                    .set(StoreLocator::getStatus, StoreStatus.DELETED())
//                    .eq(StoreLocator::getNamespace, inputStoreLocator.getNamespace())
//                    .eq(StoreLocator::getStatus, StoreStatus.NORMAL())
//                    .eq("*" .equals(inputStoreLocator.getStoreType()), StoreLocator::getStoreType, inputStoreLocator.getStoreType());
//            storeLocatorService.update(updateWrapper);
//            outputStoreLocator = inputStoreLocator;
//        } else {
//            QueryWrapper<StoreLocator> queryWrapper = new QueryWrapper<>();
//            queryWrapper.lambda().eq(StoreLocator::getStoreType, inputStoreLocator.getStoreType())
//                    .eq(StoreLocator::getNamespace, inputStoreLocator.getNamespace())
//                    .eq(StoreLocator::getName, inputStoreLocator.getName())
//                    .eq(StoreLocator::getStatus, StoreStatus.NORMAL());
//            List<StoreLocator> nodeResult = storeLocatorService.list(queryWrapper);
//            if (nodeResult.isEmpty()) {
//                return null;
//            }
//            StoreLocator nodeRecord = nodeResult.get(0);
//            String nameNow = nodeRecord.getName() + "." + System.currentTimeMillis();
//            UpdateWrapper<StoreLocator> updateWrapper = new UpdateWrapper<>();
//            updateWrapper.lambda().set(StoreLocator::getName, nameNow)
//                    .set(StoreLocator::getStatus, StoreStatus.DELETED())
//                    .eq(StoreLocator::getStoreLocatorId, nodeRecord.getStoreLocatorId());
//            storeLocatorService.update(updateWrapper);
//            inputStoreLocator.setName(nameNow);
//            outputStoreLocator = inputStoreLocator;
//        }
//        return new ErStore_JAVA(outputStoreLocator, new ArrayList<>(), new ConcurrentHashMap<>());
//    }
//
//    public ErStoreList_JAVA getStoreLocators(ErStore_JAVA input) {
//        QueryWrapper<StoreLocator> queryWrapper = new QueryWrapper<>();
//        ErStoreLocator_JAVA storeLocator = input.getStoreLocator();
//        String storeName = storeLocator.getName();
//        String storeNamespace = storeLocator.getNamespace();
//        queryWrapper.apply(!StringUtils.isBlank(storeName), " and name like " + storeName.replace('*', '%'))
//                .lambda().eq(!StringUtils.isBlank(storeNamespace), StoreLocator::getNamespace, storeNamespace);
//        List<StoreLocator> storeList = storeLocatorService.list(queryWrapper);
//        List<ErStore_JAVA> erStoreArr = new ArrayList<>();
//        for (int i = 0; i < storeList.size(); i++) {
//            StoreLocator store = storeList.get(i);
//            ErStoreLocator_JAVA erStoreLocator = new ErStoreLocator_JAVA(store.getStoreLocatorId()
//                    , store.getStoreType()
//                    , store.getNamespace()
//                    , store.getName()
//                    , store.getPath()
//                    , store.getTotalPartitions()
//                    , store.getPartitioner()
//                    , store.getSerdes());
//            erStoreArr.add(new ErStore_JAVA(erStoreLocator, new ArrayList<>(), new ConcurrentHashMap<>()));
//        }
//        return new ErStoreList_JAVA(erStoreArr, new ConcurrentHashMap<>());
//    }
//
//}
