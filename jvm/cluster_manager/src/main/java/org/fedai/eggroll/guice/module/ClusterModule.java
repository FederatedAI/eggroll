package org.fedai.eggroll.guice.module;

import com.baomidou.mybatisplus.core.toolkit.reflect.GenericTypeUtils;
import com.github.pagehelper.PageInterceptor;
import com.google.inject.AbstractModule;
import com.google.inject.Key;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import com.google.inject.spi.ProvisionListener;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.fedai.eggroll.clustermanager.schedule.Quartz;
import org.fedai.eggroll.clustermanager.schedule.Schedule;
import org.fedai.eggroll.clustermanager.schedule.ScheduleInfo;
import org.fedai.eggroll.clustermanager.session.DefaultSessionManager;
import org.fedai.eggroll.clustermanager.session.SessionManager;
import org.fedai.eggroll.core.config.MetaInfo;
import org.mybatis.guice.MyBatisModule;
import org.mybatis.guice.datasource.hikaricp.HikariCPProvider;
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

        Matcher<Class> subpacket = Matchers.inSubpackage("org.fedai.eggroll");
        ProvisionListener listener = new ProvisionListener() {
            @Override
            public <T> void onProvision(ProvisionInvocation<T> provision) {
                Key key = provision.getBinding().getKey();
                Class rawType = key.getTypeLiteral().getRawType();
                if (rawType != null && subpacket.matches(rawType)) {
                    Method[] methods = rawType.getMethods();
                    Arrays.stream(methods).forEach(method -> {
                        try {
                            Schedule config = method.getDeclaredAnnotation(Schedule.class);

                            if (config != null) {
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

        this.bindListener(Matchers.any(), listener);
        Map<String, String> conf = new HashMap<>();
        conf.put("jdbc.driverClassName", MetaInfo.CONFKEY_CLUSTER_MANAGER_JDBC_DRIVER_CLASS_NAME);
        conf.put("JDBC.url", MetaInfo.CONFKEY_CLUSTER_MANAGER_JDBC_URL);
        conf.put("JDBC.username", MetaInfo.CONFKEY_CLUSTER_MANAGER_JDBC_USERNAME);
        conf.put("JDBC.password", MetaInfo.CONFKEY_CLUSTER_MANAGER_JDBC_PASSWORD);
        conf.put("mybatis.environment.id", "cluster-manager-mybatis");
        Names.bindProperties(binder(), conf);
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
                addMapperClasses(MetaInfo.EGGROLL_MYBATIS_MAPPER_PACKAGE);
                //添加pagehelper的拦截器
                addInterceptorClass(PageInterceptor.class);
            }
        });
    }


}
