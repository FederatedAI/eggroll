/*
 * Copyright 2019 The Eggroll Authors. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.webank.ai.eggroll.core.utils;

import com.google.common.base.Preconditions;
import com.webank.ai.eggroll.api.core.BasicMeta;
import com.webank.ai.eggroll.api.core.DataStructure;
import com.webank.ai.eggroll.api.storage.Kv;
import com.webank.ai.eggroll.api.storage.StorageBasic;
import com.webank.ai.eggroll.core.model.ComputingEngine;
import com.webank.ai.eggroll.core.model.DtableStatus;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Dtable;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Fragment;
import com.webank.ai.eggroll.framework.meta.service.dao.generated.model.Node;
import org.apache.commons.lang3.StringUtils;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.Properties;

@Component
@Scope("prototype")
public class TypeConversionUtils {
    private final Object storageBuilderLock;
    private final Object createTableInfoBuilderLock;
    private final Object endpointBuilderLock;
    private final Object operandBuilderLock;
    private StorageBasic.StorageLocator.Builder storageLocatorBuilder;
    private Kv.CreateTableInfo.Builder createTableInfoBuilder;
    private BasicMeta.Endpoint.Builder endpointBuilder;
    private Kv.Operand.Builder operandBuilder;
    // todo: use this variable to optimize performance
    private boolean isSynchronizedNeeded;

    public TypeConversionUtils(boolean isSynchronizedNeeded) {
        storageLocatorBuilder = StorageBasic.StorageLocator.newBuilder();
        createTableInfoBuilder = Kv.CreateTableInfo.newBuilder();
        endpointBuilder = BasicMeta.Endpoint.newBuilder();
        operandBuilder = Kv.Operand.newBuilder();
        storageBuilderLock = new Object();
        createTableInfoBuilderLock = new Object();
        endpointBuilderLock = new Object();
        operandBuilderLock = new Object();

        this.isSynchronizedNeeded = isSynchronizedNeeded;
    }

    public TypeConversionUtils() {
        this(false);
    }

    public TypeConversionUtils setSynchronizedNeeded(boolean synchronizedNeeded) {
        isSynchronizedNeeded = synchronizedNeeded;
        return this;
    }

    public Dtable toDtable(StorageBasic.StorageLocator storageLocator) {
        Dtable result = new Dtable();
        result.setNamespace(storageLocator.getNamespace());
        result.setTableName(storageLocator.getName());
        result.setTableType(storageLocator.getType().name());

        return result;
    }

    public Dtable toDtable(Kv.CreateTableInfo createTableInfo) {
        Dtable result = toDtable(createTableInfo.getStorageLocator());
        result.setTotalFragments(createTableInfo.getFragmentCount());
        result.setStatus(DtableStatus.NORMAL.name());

        return result;
    }

    public StorageBasic.StorageLocator toStorageLocator(Dtable dtable) {
        synchronized (storageBuilderLock) {
            storageLocatorBuilder.clear();

            String namespace = dtable.getNamespace();
            if (StringUtils.isNotBlank(namespace)) {
                storageLocatorBuilder.setNamespace(namespace);
            }

            String name = dtable.getTableName();
            if (StringUtils.isNotBlank(name)) {
                storageLocatorBuilder.setName(name);
            }

            String type = dtable.getTableType();
            if (StringUtils.isNotBlank(type)) {
                storageLocatorBuilder.setType(StorageBasic.StorageType.valueOf(type));
            }

            return storageLocatorBuilder.build();
        }
    }

    public Kv.CreateTableInfo toCreateTableInfo(Dtable dtable) {
        StorageBasic.StorageLocator storageLocator = toStorageLocator(dtable);

        synchronized (createTableInfoBuilderLock) {
            createTableInfoBuilder.clear();
            createTableInfoBuilder.setStorageLocator(storageLocator);

            Integer fragments = dtable.getTotalFragments();
            if (fragments != null) {
                createTableInfoBuilder.setFragmentCount(fragments);
            }

            return createTableInfoBuilder.build();
        }
    }

    public BasicMeta.Endpoint toEndpoint(Node node) {
        BasicMeta.Endpoint result = null;

        if (node != null) {
            synchronized (endpointBuilderLock) {
                endpointBuilder.clear();
                String hostname = node.getHost();
                if (StringUtils.isNotBlank(hostname)) {
                    endpointBuilder.setHostname(hostname);
                }

                String ip = node.getIp();
                if (StringUtils.isNotBlank(ip)) {
                    endpointBuilder.setIp(ip);
                }
                result = endpointBuilder
                        .setPort(node.getPort())
                        .build();
            }
        }

        return result;
    }

    public Kv.Operand toOperand(DataStructure.RawEntry rawEntry) {
        Kv.Operand result = null;

        synchronized (operandBuilderLock) {
            result = operandBuilder.clear()
                    .setKey(rawEntry.getKey())
                    .setValue(rawEntry.getValue())
                    .build();
        }

        return result;
    }

    public Node toCurrentNode(com.webank.ai.eggroll.meta.service.dao.generated.model.Node other) {
        Node result = new Node();

        result.setCreatedAt(other.getCreatedAt());
        result.setHost(other.getHost());
        result.setIp(other.getIp());
        result.setLastHeartbeatAt(other.getLastHeartbeatAt());
        result.setNodeId(other.getNodeId());
        result.setPort(other.getPort());
        result.setStatus(other.getStatus());
        result.setType(other.getType());
        result.setUpdatedAt(other.getUpdatedAt());

        return result;
    }

    public Dtable toCurrentDtable(com.webank.ai.eggroll.meta.service.dao.generated.model.Dtable other) {
        Dtable result = new Dtable();
        result.setCreatedAt(other.getCreatedAt());
        result.setDispatcher(other.getDispatcher());
        result.setNamespace(other.getNamespace());
        result.setSerdes(other.getSerdes());
        result.setStatus(other.getStatus());
        result.setStorageVersion(other.getStorageVersion());
        result.setTableId(other.getTableId());
        result.setTableName(other.getTableName());
        result.setTableType(other.getTableType());
        result.setTotalFragments(other.getTotalFragments());
        result.setUpdatedAt(other.getUpdatedAt());

        return result;
    }

    public Fragment toCurrentFragment(com.webank.ai.eggroll.meta.service.dao.generated.model.Fragment other) {
        Fragment result = new Fragment();
        result.setFragmentId(other.getFragmentId());
        result.setNodeId(other.getNodeId());
        result.setFragmentOrder(other.getFragmentOrder());
        result.setCreatedAt(other.getCreatedAt());
        result.setStatus(other.getStatus());
        result.setTableId(other.getTableId());
        result.setUpdatedAt(other.getUpdatedAt());

        return result;
    }

    public com.webank.ai.eggroll.meta.service.dao.generated.model.Node toCompatibleNode(Node other) {
        com.webank.ai.eggroll.meta.service.dao.generated.model.Node result = new com.webank.ai.eggroll.meta.service.dao.generated.model.Node();

        result.setCreatedAt(other.getCreatedAt());
        result.setHost(other.getHost());
        result.setIp(other.getIp());
        result.setLastHeartbeatAt(other.getLastHeartbeatAt());
        result.setNodeId(other.getNodeId());
        result.setPort(other.getPort());
        result.setStatus(other.getStatus());
        result.setType(other.getType());
        result.setUpdatedAt(other.getUpdatedAt());

        return result;
    }

    public com.webank.ai.eggroll.meta.service.dao.generated.model.Dtable toCompatibleDtable(Dtable other) {
        com.webank.ai.eggroll.meta.service.dao.generated.model.Dtable result = new com.webank.ai.eggroll.meta.service.dao.generated.model.Dtable();
        result.setCreatedAt(other.getCreatedAt());
        result.setDispatcher(other.getDispatcher());
        result.setNamespace(other.getNamespace());
        result.setSerdes(other.getSerdes());
        result.setStatus(other.getStatus());
        result.setStorageVersion(other.getStorageVersion());
        result.setTableId(other.getTableId());
        result.setTableName(other.getTableName());
        result.setTableType(other.getTableType());
        result.setTotalFragments(other.getTotalFragments());
        result.setUpdatedAt(other.getUpdatedAt());

        return result;
    }

    public com.webank.ai.eggroll.meta.service.dao.generated.model.Fragment toCompatibleFragment(Fragment other) {
        com.webank.ai.eggroll.meta.service.dao.generated.model.Fragment result = new com.webank.ai.eggroll.meta.service.dao.generated.model.Fragment();
        result.setFragmentId(other.getFragmentId());
        result.setNodeId(other.getNodeId());
        result.setFragmentOrder(other.getFragmentOrder());
        result.setCreatedAt(other.getCreatedAt());
        result.setStatus(other.getStatus());
        result.setTableId(other.getTableId());
        result.setUpdatedAt(other.getUpdatedAt());

        return result;
    }

    public Properties toProperties(Map<?, ?> conf) {
        Properties result = new Properties();
        result.putAll(conf);

        return result;
    }

    public BasicMeta.Endpoint toEndpoint(ComputingEngine computingEngine) {
        Preconditions.checkNotNull(computingEngine);
        BasicMeta.Endpoint.Builder builder = BasicMeta.Endpoint.newBuilder()
                .setIp(computingEngine.getHost())
                .setPort(computingEngine.getPort());
        return builder.build();
    }


}
