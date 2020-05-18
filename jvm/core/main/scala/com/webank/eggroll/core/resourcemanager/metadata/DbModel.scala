package com.webank.eggroll.core.resourcemanager.metadata

import java.util.Date

import com.webank.eggroll.core.meta._
import com.webank.eggroll.core.constant.StringConstants

case class DbStoreLocator(id: Long = -1,
                          storeType: String = StringConstants.EMPTY,
                          namespace: String = StringConstants.EMPTY,
                          name: String = StringConstants.EMPTY,
                          path: String = StringConstants.EMPTY,
                          totalPartitions: Int = 0,
                          partitioner: String = StringConstants.EMPTY,
                          serdes: String = StringConstants.EMPTY)

case class DbStorePartition(storePartitionId: Long = -1,
                            storeLocatorId: Long = -1,
                            nodeId: Long = -1,
                            partitionId: Int = 0,
                            status: String = StringConstants.EMPTY)

case class DbServerNode(id: Long = -1,
                        name: String = StringConstants.EMPTY,
                        clusterId: Long = 0,
                        endpoint: ErEndpoint = ErEndpoint(host = StringConstants.EMPTY, port = -1),
                        nodeType: String = StringConstants.EMPTY,
                        status: String = StringConstants.EMPTY,
                        lastHeartbeatAt: Date,
                        createdAt: Date,
                        updatedAt: Date)

case class DbStoreOption(storeLocatorId: Long = -1,
                         name: String = StringConstants.EMPTY,
                         data: String = StringConstants.EMPTY,
                         createdAt: Date,
                         updatedAt: Date)
