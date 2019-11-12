/*
 * Copyright (c) 2019 - now, Eggroll Authors. All Rights Reserved.
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
 *
 *
 */

package com.webank.eggroll.clustermanager.metaservice

import com.webank.eggroll.clustermanager.constant.RdbConstants
import com.webank.eggroll.clustermanager.datasource.RdbConnectionPool
import com.webank.eggroll.core.util.Logging
import com.webank.eggroll.framework.clustermanager.dao.generated.mapper.StoreMapper
import com.webank.eggroll.framework.clustermanager.dao.generated.model.{Store, StoreExample}
import org.apache.ibatis.session.defaults.DefaultSqlSession
import org.apache.ibatis.session.{SqlSession, SqlSessionFactoryBuilder, TransactionIsolationLevel}
import java.util

class StoreCrudOperator extends Logging{
  def getStoreById(storeId: Long): Store = {
    val storeExample = new StoreExample
    storeExample.createCriteria().andStoreIdEqualTo(storeId)

    var resultList: util.List[Store] = new util.ArrayList[Store]()
    val sqlSession = RdbConnectionPool.sqlSessionFactory.openSession()
    try {
      val storeMapper = sqlSession.getMapper(classOf[StoreMapper])
      resultList = storeMapper.selectByExampleWithRowbounds(storeExample, RdbConstants.SINGLE_ROWBOUND)
    } catch {
      case e: Exception =>
        logError(e)
        throw e
    } finally {
      sqlSession.close()
    }

    if (!resultList.isEmpty) {
      resultList.get(0)
    } else {
      null
    }
  }

  def insertStore(): Store = {
    val store = new Store
    store.setStoreType("FILE")
    store.setNamespace("namespace")
    store.setName("name")
    store.setPartitioner("JAVA_HASH")
    store.setStatus("NORMAL")
    store.setTotalPartitions(10)


    val sqlSession = RdbConnectionPool.sqlSessionFactory.openSession(TransactionIsolationLevel.READ_COMMITTED)
    val transaction = RdbConnectionPool.transactionFactory.newTransaction(RdbConnectionPool.dataSource, TransactionIsolationLevel.READ_COMMITTED, false)

    try {
      val storeMapper = sqlSession.getMapper(classOf[StoreMapper])
      val rowsAffected = storeMapper.insertSelective(store)

      println(s"rowsAffected: ${rowsAffected}")
      sqlSession.commit()
      //transaction.commit()
    } finally {
      //transaction.close()
      sqlSession.close()
    }

    store
  }
}
