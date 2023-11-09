package com.webank.eggroll.guice.module;

import com.baomidou.mybatisplus.core.toolkit.reflect.GenericTypeUtils;
import com.eggroll.core.config.MetaInfo;
import com.github.pagehelper.PageHelper;
import com.github.pagehelper.PageInterceptor;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.Singleton;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.google.inject.spi.ProvisionListener;
import com.webank.eggroll.clustermanager.schedule.Quartz;
import com.webank.eggroll.clustermanager.schedule.Schedule;
import com.webank.eggroll.clustermanager.schedule.ScheduleInfo;
import com.webank.eggroll.clustermanager.schedule.Tasks;
import com.webank.eggroll.clustermanager.session.DefaultSessionManager;
import com.webank.eggroll.clustermanager.session.SessionManager;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.mybatis.guice.MyBatisModule;
import org.mybatis.guice.datasource.hikaricp.HikariCPProvider;
import org.mybatis.guice.transactional.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;


public class ClusterModule extends AbstractModule {

    Logger logger = LoggerFactory.getLogger(ClusterModule.class);

    @Override
    protected void configure() {

        Matcher<Class> subpacket = Matchers.inSubpackage("com.webank");
        ProvisionListener listener = new ProvisionListener() {
            @Override
            public <T> void onProvision(ProvisionInvocation<T> provision) {
                Key key = provision.getBinding().getKey();
                Class rawType = key.getTypeLiteral().getRawType();
                if (rawType != null && subpacket.matches(rawType)) {
                    //   key.getTypeLiteral().getRawType().
                    Method[] methods = rawType.getMethods();
                    System.err.println("xxxxxxxxxxx" + rawType);
                    Arrays.stream(methods).forEach(method -> {
                        try {
                            Schedule config = method.getDeclaredAnnotation(Schedule.class);

                            if (config != null) {
//                                String methodName = method.getName();
//                                Class clazz = field.getType();
                                ScheduleInfo scheduleInfo = new ScheduleInfo();
                                scheduleInfo.setKey(key);
                                scheduleInfo.setMethod(method);
                                scheduleInfo.setCron(config.cron());
                                Quartz.sheduleInfoMap.put(rawType.getName() + "_" + method.getName(), scheduleInfo);

                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    });

                    System.err.println(key.getTypeLiteral().getRawType().getName());
                }
            }
        };
        //PropertyUtil.loadFile(file, getClass())

        this.bindListener(Matchers.any(), listener);

        Map<String, String> conf = new HashMap<>();

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


        conf.put("jdbc.driverClassName", MetaInfo.CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME);
        conf.put("JDBC.url", MetaInfo.CONFKEY_CLUSTER_MANAGER_JDBC_URL);
        conf.put("JDBC.username", MetaInfo.CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME);
        conf.put("JDBC.password", MetaInfo.CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD);
        conf.put("mybatis.environment.id", "cluster-manager-mybatis");

        Names.bindProperties(binder(), conf);
        //bind(Dispatcher.class).to(Dispatcher.class);
        bind(SessionManager.class).to(DefaultSessionManager.class);


        this.install(new MyBatisModule() {
            @Override
            protected void initialize() {
                GenericTypeUtils.setGenericTypeResolver(new MybatisPlusUtil());
                //绑定我们自定义的数据源provider，也可以使用guice已经编写好的
                useConfigurationProvider(MybatisPlusConfigurationProvider.class);
                useSqlSessionFactoryProvider(MybatisPlusSqlSessionProvider.class);
                bindDataSourceProviderType(HikariCPProvider.class);
                bindTransactionFactoryType(JdbcTransactionFactory.class);
                //添加我们的mapper接口，可以按类注入（即通过类名注入），也可以指定整个包的路径
//                addMapperClass(ExtraScoreInfoMapper.class);
                addMapperClasses(MetaInfo.EGGROLL_MYBATIS_MAPPER_PACKAGE);
                //添加pagehelper的拦截器
                addInterceptorClass(PageInterceptor.class);
            }
        });
    }

//    private void bindScheduler() {
//        try {
//            bind(SchedulerFactory.class).toInstance(new StdSchedulerFactory(getProperties("quartz.properties")));
//            bind(GuiceJobFactory.class);
//            bind(Quartz.class).asEagerSingleton();
//        } catch (SchedulerException e) {
//            LOGGER.warn(e.getMessage(), e);
//        }
//    }


}
