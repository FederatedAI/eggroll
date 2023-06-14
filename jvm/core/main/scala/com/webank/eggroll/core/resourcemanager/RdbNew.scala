package com.webank.eggroll.core.resourcemanager

import com.webank.eggroll.core.constant.ClusterManagerConfKeys
import com.webank.eggroll.core.session.StaticErConf
import com.zaxxer.hikari.{HikariConfig, HikariDataSource}
import org.apache.commons.lang3.StringUtils
import org.apache.commons.lang3.reflect.MethodUtils

object RdbNew {

  val plainPassword = StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD)
  val passwordDecryptorInfo = ClusterManagerConfKeys.EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR.get()
  val passwordDecryptorArgs = ClusterManagerConfKeys.EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR_ARGS.get()
  val passwordDecryptorArgsSpliter = ClusterManagerConfKeys.EGGROLL_RESOURCEMANAGER_CLUSTERMANAGER_JDBC_PASSWORD_DECRYPTOR_ARGS_SPLITER.get()

  val realPassword: String = if (StringUtils.isBlank(passwordDecryptorInfo)) {
    plainPassword
  } else {
    val splitted = passwordDecryptorInfo.split("#")
    val decryptor = Class.forName(splitted(0)).newInstance()

    MethodUtils.invokeExactMethod(decryptor, splitted(1), passwordDecryptorArgs.split(passwordDecryptorArgsSpliter): _*).asInstanceOf[String]
  }

//  dataSource.setDriverClassName(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME, "com.mysql.cj.jdbc.Driver"))
//  dataSource.setUrl(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_URL))
//  dataSource.setUsername(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME))
//  dataSource.setPassword(realPassword)
//  dataSource.setMaxIdle(StaticErConf.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_IDLE, 10))
//  dataSource.setMaxTotal(StaticErConf.getInt(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MAX_TOTAL, 100))
//  dataSource.setTimeBetweenEvictionRunsMillis(StaticErConf.getLong(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_TIME_BETWEEN_EVICTION_RUNS_MS, 10000L))
//  dataSource.setMinEvictableIdleTimeMillis(StaticErConf.getLong(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_MIN_EVICTABLE_IDLE_TIME_MS, 120000L))
//  dataSource.setDefaultAutoCommit(StaticErConf.getBoolean(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_DATASOURCE_DB_DEFAULT_AUTO_COMMIT, false))

  val config = new HikariConfig()
  config.setDriverClassName(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME, "com.mysql.cj.jdbc.Driver"))
  config.setJdbcUrl(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_URL))
  config.setMaximumPoolSize(10)
  config.addDataSourceProperty("cachePrepStmts", "true")
  config.addDataSourceProperty("prepStmtCacheSize", "250")
  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
  config.setPassword(realPassword)
  config.setUsername(StaticErConf.getString(ClusterManagerConfKeys.CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME))
  val dataSource = new HikariDataSource(config)

}



//def createDataSource(databaseConfig: DatabaseConfig): HikariDataSource = {
//  val config = new HikariConfig()
//
//  config.setDriverClassName("com.mysql.cj.jdbc.Driver")
//  config.setJdbcUrl(databaseConfig.toString)
//  config.setMaximumPoolSize(10)
//  config.addDataSourceProperty("cachePrepStmts", "true")
//  config.addDataSourceProperty("prepStmtCacheSize", "250")
//  config.addDataSourceProperty("prepStmtCacheSqlLimit", "2048")
//
//  new HikariDataSource(config)
//  }
