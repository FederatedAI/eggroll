package com.webank.eggroll.core.resourcemanager.metadata

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
