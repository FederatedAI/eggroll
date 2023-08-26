//package com.webank.eggroll.guice.module;
//
//import com.google.inject.Inject;
//
//import com.google.inject.Provider;
//import com.google.inject.name.Named;
//
//import com.alibaba.druid.pool.DruidDataSource;
//
//
//HikariDataSource
//import com.zaxxer.hikari.HikariDataSource;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.sql.SQLException;
//import java.util.Properties;
//
//import javax.sql.DataSource;
//
//
//public class DruidDataSourceProvider implements Provider<DataSource> {
//
//    Logger logger = LoggerFactory.getLogger(getClass());
//
//    HikariDataSource dataSource = new HikariDataSource();
//
//    @Inject
//    public void setDriverClassName(@Named("jdbc.driverClassName") final String driverClassName) {
//        dataSource.setDriverClassName(driverClassName);
//    }
//
//    @Inject
//    public void setUrl(@Named("jdbc.url") final String url) {
//        dataSource.setJdbcUrl(url);
//    }
//
//    @Inject
//    public void setUsername(@Named("jdbc.username") final String username) {
//        dataSource.setUsername(username);
//    }
//
//    @Inject
//    public void setPassword(@Named("jdbc.password") final String password) {
//        dataSource.setPassword(password);
//    }
//
//    @Inject(optional = true)
//    public void setDefaultAutoCommit(@Named("jdbc.autoCommit") final boolean defaultAutoCommit) {
//        dataSource.setDefaultAutoCommit(defaultAutoCommit);
//    }
//
//    @Inject(optional = true)
//    public void setDefaultReadOnly(@Named("jdbc.readOnly") final boolean defaultReadOnly) {
//        dataSource.setDefaultReadOnly(defaultReadOnly);
//    }
//
//    @Inject(optional = true)
//    public void setDefaultTransactionIsolation(
//            @Named("jdbc.transactionIsolation") final int defaultTransactionIsolation) {
//        dataSource.setDefaultTransactionIsolation(defaultTransactionIsolation);
//    }
//
//    @Inject(optional = true)
//    public void setDefaultCatalog(@Named("jdbc.catalog") final String defaultCatalog) {
//        dataSource.setDefaultCatalog(defaultCatalog);
//    }
//
//    @Inject(optional = true)
//    public void setMaxActive(@Named("jdbc.maxActive") final int maxActive) {
//        dataSource.setMaxActive(maxActive);
//    }
//
//    @Inject(optional = true)
//    public void setMinIdle(@Named("jdbc.minIdle") final int minIdle) {
//        dataSource.setMinIdle(minIdle);
//    }
//
//    @Inject(optional = true)
//    public void setInitialSize(@Named("jdbc.initialSize") final int initialSize) {
//        dataSource.setInitialSize(initialSize);
//    }
//
//    @Inject(optional = true)
//    public void setMaxWait(@Named("jdbc.maxWait") final long maxWait) {
//        dataSource.setMaxWait(maxWait);
//    }
//
//    @Inject(optional = true)
//    public void setTestOnBorrow(@Named("jdbc.testOnBorrow") final boolean testOnBorrow) {
//        dataSource.setTestOnBorrow(testOnBorrow);
//    }
//
//    @Inject(optional = true)
//    public void setTestOnReturn(@Named("jdbc.testOnReturn") final boolean testOnReturn) {
//        dataSource.setTestOnReturn(testOnReturn);
//    }
//
//    @Inject(optional = true)
//    public void setTimeBetweenEvictionRunsMillis(
//            @Named("jdbc.timeBetweenEvictionRunsMillis") final long timeBetweenEvictionRunsMillis) {
//        dataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
//    }
//
//    @Inject(optional = true)
//    public void setMinEvictableIdleTimeMillis(
//            @Named("jdbc.minEvictableIdleTimeMillis") final long minEvictableIdleTimeMillis) {
//        dataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
//    }
//
//    @Inject(optional = true)
//    public void setTestWhileIdle(@Named("jdbc.testWhileIdle") final boolean testWhileIdle) {
//        dataSource.setTestWhileIdle(testWhileIdle);
//    }
//
//    @Inject(optional = true)
//    public void setValidationQuery(@Named("jdbc.validationQuery") final String validationQuery) {
//        dataSource.setValidationQuery(validationQuery);
//    }
//
//    @Inject(optional = true)
//    public void setValidationQueryTimeout(@Named("jdbc.validationQueryTimeout") final int validationQueryTimeout) {
//        dataSource.setValidationQueryTimeout(validationQueryTimeout);
//    }
//
//    @Inject(optional = true)
//    public void setAccessToUnderlyingConnectionAllowed(
//            @Named("jdbc.accessToUnderlyingConnectionAllowed") final boolean accessToUnderlyingConnectionAllowed) {
//        dataSource.setAccessToUnderlyingConnectionAllowed(accessToUnderlyingConnectionAllowed);
//    }
//
//    @Inject(optional = true)
//    public void setRemoveAbandoned(@Named("jdbc.removeAbandoned") final boolean removeAbandoned) {
//        dataSource.setRemoveAbandoned(removeAbandoned);
//    }
//
//    @Inject(optional = true)
//    public void setRemoveAbandonedTimeout(@Named("jdbc.removeAbandonedTimeout") final int removeAbandonedTimeout) {
//        dataSource.setRemoveAbandonedTimeout(removeAbandonedTimeout);
//    }
//
//    @Inject(optional = true)
//    public void setLogAbandoned(@Named("jdbc.logAbandoned") final boolean logAbandoned) {
//        dataSource.setLogAbandoned(logAbandoned);
//    }
//
//    @Inject(optional = true)
//    public void setPoolPreparedStatements(@Named("jdbc.poolPreparedStatements") final boolean poolPreparedStatements) {
//        dataSource.setPoolPreparedStatements(poolPreparedStatements);
//    }
//
//    @Inject(optional = true)
//    public void setMaxOpenPreparedStatements(
//            @Named("jdbc.maxOpenPreparedStatements") final int maxOpenPreparedStatements) {
//        dataSource.setMaxOpenPreparedStatements(maxOpenPreparedStatements);
//    }
//
//    @Inject(optional = true)
//    public void setConnectProperties(@Named("jdbc.connectProperties") final Properties connectionProperties) {
//        dataSource.setConnectProperties(connectionProperties);
//    }
//
//    @Inject(optional = true)
//    public void setConnectionProperties(@Named("jdbc.connectionProperties") final String connectionProperties) {
//        dataSource.setConnectionProperties(connectionProperties);
//    }
//
//    @Inject(optional = true)
//    public void setFilters(@Named("jdbc.filters") final String filters) throws SQLException {
//        dataSource.setFilters(filters);
//    }
//
//    @Inject(optional = true)
//    public void setExceptionSorter(@Named("jdbc.exceptionSorter") final String exceptionSorter) throws SQLException {
//        dataSource.setExceptionSorter(exceptionSorter);
//    }
//
//    @Inject(optional = true)
//    public void setExceptionSorterClassName(@Named("jdbc.exceptionSorterClassName") final String exceptionSorterClassName)
//            throws Exception {
//        dataSource.setExceptionSorterClassName(exceptionSorterClassName);
//    }
//
//    @Override
//    public String toString() {
//        return "DruidDataSourceProvider{" +
//                "jdbc.url=" + dataSource.getUrl() +
//                ",username='" + dataSource.getUsername() +
//                '}';
//    }
//
//    @Override
//    public DataSource get() {
//        logger.info("dataSource config ---> " + toString());
//        return dataSource;
//    }
//
//}