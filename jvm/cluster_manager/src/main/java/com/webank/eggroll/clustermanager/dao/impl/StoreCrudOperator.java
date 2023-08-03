package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.eggroll.core.config.Dict;
import com.eggroll.core.exceptions.CrudException;
import com.eggroll.core.pojo.*;
import com.eggroll.core.utils.JsonUtil;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.clustermanager.entity.StoreLocator;
import com.webank.eggroll.clustermanager.entity.StoreOption;
import com.webank.eggroll.clustermanager.entity.StorePartition;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class StoreCrudOperator {

    @Autowired
    StoreLocatorService storeLocatorService;
    @Autowired
    StorePartitionService storePartitionService;
    @Autowired
    ServerNodeService serverNodeService;
    @Autowired
    StoreOptionService storeOptionService;

    private final Map<Long, Object> nodeIdToNode = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LogManager.getLogger(StoreCrudOperator.class);

    private synchronized ErStore doGetStore(ErStore input) {
        Map<String, String> inputOptions = input.getOptions();

        // getting input locator
        ErStoreLocator inputStoreLocator = input.getStoreLocator();

        StoreLocator params = new StoreLocator();
        params.setNamespace(inputStoreLocator.getNamespace());
        params.setName(inputStoreLocator.getName());
        params.setStatus(Dict.NORMAL);

        if (!StringUtils.isBlank(inputStoreLocator.getStoreType())) {
            params.setStoreType(inputStoreLocator.getStoreType());
        }
        List<StoreLocator> storeLocatorResult = storeLocatorService.list(new QueryWrapper<>(params));
        if (storeLocatorResult.isEmpty()) {
            return null;
        }
        StoreLocator store = storeLocatorResult.get(0);
        Long storeLocatorId = store.getStoreLocatorId();

        QueryWrapper<StorePartition> queryStorePartition = new QueryWrapper<>();
        queryStorePartition.lambda().eq(StorePartition::getStoreLocatorId, storeLocatorId)
                .orderByAsc(StorePartition::getStorePartitionId);
        List<StorePartition> storePartitionResult = storePartitionService.list(queryStorePartition);
        if (storePartitionResult.isEmpty()) {
            throw new IllegalStateException("store locator found but no partition found");
        }

        List<Long> missingNodeId = new ArrayList<>();
        List<Long> partitionAtNodeIds = new ArrayList<>();

        for (StorePartition storePartition : storePartitionResult) {
            Long nodeId = storePartition.getNodeId();
            if (!nodeIdToNode.containsKey(nodeId)) missingNodeId.add(nodeId);
            partitionAtNodeIds.add(nodeId);
        }
        if (!missingNodeId.isEmpty()) {
            QueryWrapper<ServerNode> queryServerNode = new QueryWrapper<>();
            queryServerNode.lambda().eq(ServerNode::getStatus, Dict.HEALTHY)
                    .eq(ServerNode::getNodeType, Dict.NODE_MANAGER)
                    .in(ServerNode::getServerNodeId, missingNodeId);
            List<ServerNode> nodeResult = serverNodeService.list(queryServerNode);
            if (nodeResult.isEmpty()) {
                LOGGER.error("No valid node for this store: " + JsonUtil.object2Json(inputStoreLocator) + " missingId " + missingNodeId.toString());
                throw new IllegalStateException("No valid node for this store: " + JsonUtil.object2Json(inputStoreLocator));
            }

            for (ServerNode serverNode : nodeResult) {
                Long serverNodeId = serverNode.getServerNodeId();
                nodeIdToNode.putIfAbsent(serverNodeId, serverNode);
            }
        }

        ErStoreLocator outputStoreLocator = new ErStoreLocator(store.getStoreLocatorId()
                , store.getStoreType()
                , store.getNamespace()
                , store.getName()
                , store.getPath()
                , store.getTotalPartitions()
                , store.getPartitioner()
                , store.getSerdes());
        QueryWrapper<StoreOption> storeOptionWrapper = new QueryWrapper<>();
        storeOptionWrapper.lambda().eq(StoreOption::getStoreLocatorId, storeLocatorId);
        List<StoreOption> storeOpts = storeOptionService.list(storeOptionWrapper);
        ConcurrentHashMap<String, String> outputOptions = new ConcurrentHashMap<>();
        if (inputOptions != null) {
            outputOptions.putAll(inputOptions);
        }
        if (storeOpts != null) {
            for (StoreOption r : storeOpts) outputOptions.put(r.getName(), r.getData());
        }

        // process output partitions
        List<ErPartition> outputPartitions = new ArrayList<>();
        for (StorePartition p : storePartitionResult) {

            ErProcessor erProcessor = new ErProcessor();
            erProcessor.setId(p.getPartitionId());
            erProcessor.setServerNodeId(p.getNodeId());

            outputPartitions.add(new ErPartition(p.getPartitionId()
                    , outputStoreLocator
                    , erProcessor
                    , -1
            ));
        }
        return new ErStore(outputStoreLocator, outputPartitions, outputOptions);
    }

    private ErStore doCreateStore(ErStore input) {
        Map<String, String> inputOptions = input.getOptions();
        ErStoreLocator inputStoreLocator = input.getStoreLocator();

        StoreLocator newStoreLocator = new StoreLocator();
        newStoreLocator.setStoreType(inputStoreLocator.getStoreType());
        newStoreLocator.setNamespace(inputStoreLocator.getNamespace());
        newStoreLocator.setName(inputStoreLocator.getName());
        newStoreLocator.setPath(inputStoreLocator.getPath());
        newStoreLocator.setTotalPartitions(inputStoreLocator.getTotalPartitions());
        newStoreLocator.setPartitioner(inputStoreLocator.getPartitioner());
        newStoreLocator.setSerdes(inputStoreLocator.getSerdes());
        newStoreLocator.setStatus(Dict.NORMAL);
        boolean addStoreLocatorFlag = storeLocatorService.save(newStoreLocator);
        if (!addStoreLocatorFlag) {

            throw new CrudException("Illegal rows affected returned when creating store locator: 0");
        }

        int newTotalPartitions = inputStoreLocator.getTotalPartitions();
        List<ErPartition> newPartitions = new ArrayList<>();
        List<ErServerNode> serverNodes = serverNodeService.getListByErServerNode(
                new ErServerNode(
                        Dict.NODE_MANAGER
                        , Dict.HEALTHY)).stream().sorted(Comparator.comparingLong(ErServerNode::getId)).collect(Collectors.toList());
        int nodesCount = serverNodes.size();
        List<ErPartition> specifiedPartitions = input.getPartitions();
        boolean isPartitionsSpecified = specifiedPartitions.size() > 0;
        if (newTotalPartitions <= 0) newTotalPartitions = nodesCount << 2;
        ArrayList<Long> serverNodeIds = new ArrayList<>();
        for (int i = 0; i < newTotalPartitions; i++) {
            ErServerNode node = serverNodes.get(i % nodesCount);
            StorePartition nodeRecord = new StorePartition();
            nodeRecord.setStoreLocatorId(newStoreLocator.getStoreLocatorId());
            nodeRecord.setNodeId(isPartitionsSpecified ? input.getPartitions().get(i % specifiedPartitions.size()).getProcessor().getServerNodeId() : node.getId());
            nodeRecord.setPartitionId(i);
            nodeRecord.setStatus(Dict.PRIMARY);
            boolean addNodeRecordFlag = storePartitionService.save(nodeRecord);
            if (!addNodeRecordFlag) {

                throw new CrudException("Illegal rows affected when creating node: 0");
            }
            serverNodeIds.add(node.getId());

            ErProcessor binding = new ErProcessor();
            binding.setId(i);
            binding.setServerNodeId(isPartitionsSpecified ? input.getPartitions().get(i % specifiedPartitions.size()).getProcessor().getServerNodeId() : node.getId());
            binding.setTag("binding");

            newPartitions.add(new ErPartition(i
                    , inputStoreLocator
                    ,binding
                    , -1));
        }
        ConcurrentHashMap<String, String> newOptions = new ConcurrentHashMap<>();
        if (inputOptions != null) newOptions.putAll(inputOptions);
        for (Map.Entry<String, String> entry : newOptions.entrySet()) {
            StoreOption newStoreOption = new StoreOption();
            newStoreOption.setStoreLocatorId(newStoreLocator.getStoreLocatorId());
            newStoreOption.setName(entry.getKey());
            newStoreOption.setData(entry.getValue());
            storeOptionService.save(newStoreOption);
        }
        return new ErStore(inputStoreLocator, newPartitions, newOptions);
    }

    @Transactional
    private ErStore doDeleteStore(ErStore input) {
        ErStoreLocator inputStoreLocator = input.getStoreLocator();
        ErStoreLocator outputStoreLocator = null;
        if ("*" .equals(inputStoreLocator.getName())) {
            UpdateWrapper<StoreLocator> updateWrapper = new UpdateWrapper<>();
            updateWrapper.lambda().setSql("name = concat(name, " + System.currentTimeMillis() + ")")
                    .set(StoreLocator::getStatus, Dict.DELETED)
                    .eq(StoreLocator::getNamespace, inputStoreLocator.getNamespace())
                    .eq(StoreLocator::getStatus, Dict.NORMAL)
                    .eq("*" .equals(inputStoreLocator.getStoreType()), StoreLocator::getStoreType, inputStoreLocator.getStoreType());
            storeLocatorService.update(updateWrapper);
            outputStoreLocator = inputStoreLocator;
        } else {
            QueryWrapper<StoreLocator> queryWrapper = new QueryWrapper<>();
            queryWrapper.lambda().eq(StoreLocator::getStoreType, inputStoreLocator.getStoreType())
                    .eq(StoreLocator::getNamespace, inputStoreLocator.getNamespace())
                    .eq(StoreLocator::getName, inputStoreLocator.getName())
                    .eq(StoreLocator::getStatus, Dict.NORMAL);
            List<StoreLocator> nodeResult = storeLocatorService.list(queryWrapper);
            if (nodeResult.isEmpty()) {
                return null;
            }
            StoreLocator nodeRecord = nodeResult.get(0);
            String nameNow = nodeRecord.getName() + "." + System.currentTimeMillis();
            UpdateWrapper<StoreLocator> updateWrapper = new UpdateWrapper<>();
            updateWrapper.lambda().set(StoreLocator::getName, nameNow)
                    .set(StoreLocator::getStatus, Dict.DELETED)
                    .eq(StoreLocator::getStoreLocatorId, nodeRecord.getStoreLocatorId());
            storeLocatorService.update(updateWrapper);
            inputStoreLocator.setName(nameNow);
            outputStoreLocator = inputStoreLocator;
        }
        return new ErStore(outputStoreLocator, new ArrayList<>(), new ConcurrentHashMap<>());
    }

//    public ErStoreList getStoreLocators(ErStore input) {
//        QueryWrapper<StoreLocator> queryWrapper = new QueryWrapper<>();
//        ErStoreLocator storeLocator = input.getStoreLocator();
//        String storeName = storeLocator.getName();
//        String storeNamespace = storeLocator.getNamespace();
//        queryWrapper.apply(!StringUtils.isBlank(storeName), " and name like " + storeName.replace('*', '%'))
//                .lambda().eq(!StringUtils.isBlank(storeNamespace), StoreLocator::getNamespace, storeNamespace);
//        List<StoreLocator> storeList = storeLocatorService.list(queryWrapper);
//        List<ErStore> erStoreArr = new ArrayList<>();
//        for (int i = 0; i < storeList.size(); i++) {
//            StoreLocator store = storeList.get(i);
//            ErStoreLocator erStoreLocator = new ErStoreLocator(store.getStoreLocatorId()
//                    , store.getStoreType()
//                    , store.getNamespace()
//                    , store.getName()
//                    , store.getPath()
//                    , store.getTotalPartitions()
//                    , store.getPartitioner()
//                    , store.getSerdes());
//            erStoreArr.add(new ErStore(erStoreLocator, new ArrayList<>(), new ConcurrentHashMap<>()));
//        }
//        return new ErStoreList(erStoreArr, new ConcurrentHashMap<>());
//    }

}
