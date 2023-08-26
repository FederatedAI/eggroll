package com.webank.eggroll.guice.module;

import com.google.inject.AbstractModule;
import com.google.inject.name.Names;
 import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.mybatis.guice.MyBatisModule;
import org.mybatis.guice.datasource.hikaricp.HikariCPProvider;

import java.util.HashMap;
import java.util.Map;


public class ClusterModule extends AbstractModule {


    protected void configure() {
        //PropertyUtil.loadFile(file, getClass())
        System.err.println("cluster module configure begin");

        Map<String,String> conf=new HashMap<>();

//        jdbc.driverClassName=com.mysql.jdbc.Driver
//        jdbc.url=jdbc:mysql://10.0.2.30:3306/xxx_test?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=true&zeroDateTimeBehavior=convertToNull
//        jdbc.username=test
//        jdbc.password=test
//        jdbc.filters=stat,wall
//        jdbc.maxActive=20
//        jdbc.initialSize=5
//        jdbc.maxWait=60000
//        jdbc.minIdle=10
//        jdbc.timeBetweenEvictionRunsMillis=60000
//        jdbc.minEvictableIdleTimeMillis=300000
//        jdbc.validationQuery=SELECT 1
//        jdbc.testWhileIdle=true
//        jdbc.testOnBorrow=false
//        jdbc.testOnReturn=false
//        jdbc.maxOpenPreparedStatements=20
//        jdbc.removeAbandoned=true
//        jdbc.removeAbandonedTimeout=1800
//        jdbc.logAbandoned=true
        conf.put("jdbc.driverClassName","com.mysql.jdbc.Driver");
        conf.put("JDBC.url","jdbc:mysql://localhost:3306/eggroll_meta?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&failOverReadOnly=true&zeroDateTimeBehavior=convertToNull");
        conf.put("JDBC.username","fate");
        conf.put("JDBC.password","fate_dev");
        conf.put("mybatis.environment.id","cluster-manager-mybatis");

        Names.bindProperties(binder(), conf);
       // bind(DbQueryService.class).to(DbQueryServiceImpl.class);
        this.install(new MyBatisModule() {
            @Override
            protected void initialize() {
                //绑定我们自定义的数据源provider，也可以使用guice已经编写好的
                useConfigurationProvider(ConfigurationProvider.class);
                useSqlSessionFactoryProvider(MybatisPlusSqlSessionProvider.class);
                bindDataSourceProviderType(HikariCPProvider.class);
                bindTransactionFactoryType(JdbcTransactionFactory.class);
                //添加我们的mapper接口，可以按类注入（即通过类名注入），也可以指定整个包的路径
//                addMapperClass(ExtraScoreInfoMapper.class);
                addMapperClasses("com.webank.eggroll.clustermanager.dao.mapper");
            }
        });
    }


}
