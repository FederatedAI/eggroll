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

package com.webank.eggroll.clustermanager.datasource

import com.webank.eggroll.core.constant.ClusterManagerConfKeys
import com.webank.eggroll.core.session.DefaultErConf
import org.apache.commons.dbcp2.BasicDataSource
import org.apache.ibatis.mapping.Environment
import org.apache.ibatis.session.Configuration
import org.apache.ibatis.session.defaults.DefaultSqlSessionFactory
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory
import org.apache.ibatis.transaction.managed.ManagedTransactionFactory

object RdbConnectionPool {
  val dataSource: BasicDataSource = new BasicDataSource
  dataSource.setDriverClassName(DefaultErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME, "com.mysql.cj.jdbc.Driver"))
  dataSource.setUrl(DefaultErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_URL))
  dataSource.setUsername(DefaultErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME))
  dataSource.setPassword(DefaultErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD))
  dataSource.setMaxIdle(DefaultErConf.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_IDLE, 10))
  dataSource.setMaxTotal(DefaultErConf.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_TOTAL, 100))
  dataSource.setTimeBetweenEvictionRunsMillis(DefaultErConf.getLong(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_TIME_BETWEEN_EVICTION_RUNS_MS, 10000L))
  dataSource.setMinEvictableIdleTimeMillis(DefaultErConf.getLong(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MIN_EVICTABLE_IDLE_TIME_MS, 120000L))
  dataSource.setDefaultAutoCommit(DefaultErConf.getBoolean(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_DEFAULT_AUTO_COMMIT, false))

  val transactionFactory = new JdbcTransactionFactory
  val environment = new Environment("meta-service", transactionFactory, dataSource)

  val configuration = new Configuration(environment)
  configuration.addMappers("com.webank.eggroll.framework.clustermanager.dao.generated.mapper")

  val sqlSessionFactory = new DefaultSqlSessionFactory(configuration)

  def getConnection(): Unit = {

  }
}

