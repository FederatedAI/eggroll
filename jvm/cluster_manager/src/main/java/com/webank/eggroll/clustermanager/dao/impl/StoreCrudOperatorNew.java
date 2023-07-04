package com.webank.eggroll.clustermanager.dao.impl;

import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.baomidou.mybatisplus.core.conditions.update.UpdateWrapper;
import com.webank.eggroll.clustermanager.entity.ServerNode;
import com.webank.eggroll.clustermanager.entity.StoreLocator;
import com.webank.eggroll.clustermanager.entity.StoreOption;
import com.webank.eggroll.clustermanager.entity.StorePartition;
import com.webank.eggroll.clustermanager.utils.JsonUtil;
import com.webank.eggroll.core.constant.PartitionStatus;
import com.webank.eggroll.core.constant.ServerNodeStatus;
import com.webank.eggroll.core.constant.ServerNodeTypes;
import com.webank.eggroll.core.constant.StoreStatus;
import com.webank.eggroll.core.error.CrudException;
import com.webank.eggroll.core.meta.*;
import com.webank.eggroll.core.resourcemanager.metadata.ServerNodeCrudOperator;
import com.webank.eggroll.core.util.TimeUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import scala.Array;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Service
public class StoreCrudOperatorNew {

    @Autowired
    StoreLocatorService storeLocatorService;
    @Autowired
    StorePartitionService storePartitionService;
    @Autowired
    ServerNodeService serverNodeService;
    @Autowired
    StoreOptionService storeOptionService;

    private Map<Long, Object> nodeIdToNode = new ConcurrentHashMap<>();
    private static final Logger LOGGER = LogManager.getLogger(StoreCrudOperatorNew.class);

    private ErStore doGetStore(ErStore input) {
        Map<String, String> inputOptions = input.options();

        // getting input locator
        ErStoreLocator inputStoreLocator = input.storeLocator();

        StoreLocator params = new StoreLocator();
        params.setNamespace(inputStoreLocator.namespace());
        params.setName(inputStoreLocator.name());
        params.setStatus(StoreStatus.NORMAL());

        if (!StringUtils.isBlank(inputStoreLocator.storeType())) {
            params.setStoreType(inputStoreLocator.storeType());
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

        for (int i = 0; i < storePartitionResult.size(); i++) {
            Long nodeId = storePartitionResult.get(i).getNodeId();
            if (!nodeIdToNode.containsKey(nodeId)) missingNodeId.add(nodeId);
            partitionAtNodeIds.add(nodeId);
        }
        if (!missingNodeId.isEmpty()) {
            QueryWrapper<ServerNode> queryServerNode = new QueryWrapper<>();
            queryServerNode.lambda().eq(ServerNode::getStatus, ServerNodeStatus.HEALTHY())
                    .eq(ServerNode::getNodeType, ServerNodeTypes.NODE_MANAGER())
                    .in(ServerNode::getServerNodeId, missingNodeId);
            List<ServerNode> nodeResult = serverNodeService.list(queryServerNode);
            if (nodeResult.isEmpty()) {
                LOGGER.error("No valid node for this store: " + JsonUtil.object2Json(inputStoreLocator) + " missingId " + missingNodeId.toString());
                throw new IllegalStateException("No valid node for this store: " + JsonUtil.object2Json(inputStoreLocator));
            }

            for (int i = 0; i < nodeResult.size(); i++) {
                Long serverNodeId = nodeResult.get(i).getServerNodeId();
                nodeIdToNode.putIfAbsent(serverNodeId, nodeResult.get(i));
            }
        }

        ErStoreLocator outputStoreLocator = new ErStoreLocator(store.getStoreLocatorId()
                ,store.getStoreType()
                ,store.getNamespace()
                ,store.getName()
                ,store.getPath()
                ,store.getTotalPartitions()
                ,store.getPartitioner()
                ,store.getSerdes());
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
            outputPartitions.add(new ErPartition(p.getPartitionId()
                    ,outputStoreLocator
                    ,new ErProcessor(p.getPartitionId(), p.getNodeId())
                    ,-1
            ));
        }
        ErPartition[] outputPartitionsArray = outputPartitions.toArray(new ErPartition[0]);
        return new ErStore(outputStoreLocator, outputPartitionsArray, outputOptions);
    }

    private ErStore doCreateStore(ErStore input) {
        Map<String, String> inputOptions = input.options();
        ErStoreLocator inputStoreLocator = input.storeLocator();

        StoreLocator newStoreLocator = new StoreLocator();
        newStoreLocator.setStoreType(inputStoreLocator.storeType());
        newStoreLocator.setNamespace(inputStoreLocator.namespace());
        newStoreLocator.setName(inputStoreLocator.name());
        newStoreLocator.setPath(inputStoreLocator.path());
        newStoreLocator.setTotalPartitions(inputStoreLocator.totalPartitions());
        newStoreLocator.setPartitioner(inputStoreLocator.partitioner());
        newStoreLocator.setSerdes(inputStoreLocator.serdes());
        newStoreLocator.setStatus(StoreStatus.NORMAL());
        boolean addStoreLocatorFlag = storeLocatorService.save(newStoreLocator);
        if (!addStoreLocatorFlag) {
            throw new CrudException("Illegal rows affected returned when creating store locator: 0");
        }

        int newTotalPartitions = inputStoreLocator.totalPartitions();
        List<ErPartition> newPartitions = new ArrayList<>();

        List<ErServerNode> serverNodes = Arrays.stream(ServerNodeCrudOperator.doGetServerNodes(
                new ErServerNode(
                        ServerNodeTypes.NODE_MANAGER()
                        ,ServerNodeStatus.HEALTHY()))).sorted(Comparator.comparingLong(ErServerNode::id)).collect(Collectors.toList());
        int nodesCount = serverNodes.size();
        ErPartition[] specifiedPartitions = input.partitions();
        boolean isPartitionsSpecified = specifiedPartitions.length > 0;
        if (newTotalPartitions <= 0) newTotalPartitions = nodesCount << 2;
        ArrayList<Long> serverNodeIds = new ArrayList<>();
        for (int i = 0; i < newTotalPartitions; i++) {
            ErServerNode node = serverNodes.get(i % nodesCount);
            StorePartition nodeRecord = new StorePartition();
            nodeRecord.setStoreLocatorId(newStoreLocator.getStoreLocatorId());
            nodeRecord.setNodeId(isPartitionsSpecified ? input.partitions()[i % specifiedPartitions.length].processor().serverNodeId() : node.id());
            nodeRecord.setPartitionId(i);
            nodeRecord.setStatus(PartitionStatus.PRIMARY());
            boolean addNodeRecordFlag = storePartitionService.save(nodeRecord);
            if (!addNodeRecordFlag) {
                throw new CrudException("Illegal rows affected when creating node: 0");
            }
            serverNodeIds.add(node.id());
            newPartitions.add(new ErPartition(i
                    ,inputStoreLocator
                    ,new ErProcessor(i
                                ,isPartitionsSpecified ? input.partitions()[i % specifiedPartitions.length].processor().serverNodeId() : node.id()
                                ,"binding")
                    , -1));
        }
        ConcurrentHashMap<String, String> newOptions = new ConcurrentHashMap<>();
        if (inputOptions != null) newOptions.putAll(inputOptions);
        Iterator<Map.Entry<String, String>> itOptions = newOptions.entrySet().iterator();
        while(itOptions.hasNext()){
            Map.Entry<String, String> entry = itOptions.next();
            StoreOption newStoreOption = new StoreOption();
            newStoreOption.setStoreLocatorId(newStoreLocator.getStoreLocatorId());
            newStoreOption.setName(entry.getKey());
            newStoreOption.setData(entry.getValue());
            storeOptionService.save(newStoreOption);
        }
        return new ErStore(inputStoreLocator,newPartitions.toArray(new ErPartition[0]),newOptions);
    }

    @Transactional
    private ErStore doDeleteStore(ErStore input){
        ErStoreLocator inputStoreLocator = input.storeLocator();
        ErStoreLocator outputStoreLocator = null;
        if ("*".equals(inputStoreLocator.name())) {
            UpdateWrapper<StoreLocator> updateWrapper = new UpdateWrapper<>();
            updateWrapper.lambda().setSql("name = concat(name, " + TimeUtils.getNowMs(null) +")")
                    .set(StoreLocator::getStatus,StoreStatus.DELETED())
                    .eq(StoreLocator::getNamespace,inputStoreLocator.namespace())
                    .eq(StoreLocator::getStatus,StoreStatus.NORMAL())
                    .eq("*".equals(inputStoreLocator.storeType()),StoreLocator::getStoreType,inputStoreLocator.storeType());
            storeLocatorService.update(updateWrapper);
            outputStoreLocator = inputStoreLocator;
        }else{
            QueryWrapper<StoreLocator> queryWrapper = new QueryWrapper<>();
            queryWrapper.lambda().eq(StoreLocator::getStoreType,inputStoreLocator.storeType())
                    .eq(StoreLocator::getNamespace,inputStoreLocator.namespace())
                    .eq(StoreLocator::getName,inputStoreLocator.name())
                    .eq(StoreLocator::getStatus,StoreStatus.NORMAL());
            List<StoreLocator> nodeResult = storeLocatorService.list(queryWrapper);
            if (nodeResult.isEmpty()) {
                return null;
            }
            StoreLocator nodeRecord = nodeResult.get(0);
            String nameNow = nodeRecord.getName() + "." + TimeUtils.getNowMs(null);
            UpdateWrapper<StoreLocator> updateWrapper = new UpdateWrapper<>();
            updateWrapper.lambda().set(StoreLocator::getName,nameNow)
                    .set(StoreLocator::getStatus,StoreStatus.DELETED())
                    .eq(StoreLocator::getStoreLocatorId,nodeRecord.getStoreLocatorId());
            storeLocatorService.update(updateWrapper);
//            ????
//            inputStoreLocator.copy(name = nodeRecord.name)
            outputStoreLocator = inputStoreLocator;
        }
        return new ErStore(outputStoreLocator, new ErPartition[0],new ConcurrentHashMap<>());
    }

}
