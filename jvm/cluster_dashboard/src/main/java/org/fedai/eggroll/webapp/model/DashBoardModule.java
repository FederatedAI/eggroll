package org.fedai.eggroll.webapp.model;

import com.baomidou.mybatisplus.core.toolkit.reflect.GenericTypeUtils;
import com.google.inject.AbstractModule;
import com.google.inject.matcher.Matcher;
import com.google.inject.matcher.Matchers;
import com.google.inject.name.Names;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
import org.fedai.eggroll.clustermanager.session.DefaultSessionManager;
import org.fedai.eggroll.clustermanager.session.SessionManager;
import org.fedai.eggroll.core.config.MetaInfo;
import org.fedai.eggroll.guice.module.MybatisPlusConfigurationProvider;
import org.fedai.eggroll.guice.module.MybatisPlusSqlSessionProvider;
import org.fedai.eggroll.guice.module.MybatisPlusUtil;
import org.mybatis.guice.MyBatisModule;
import org.mybatis.guice.datasource.hikaricp.HikariCPProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class DashBoardModule extends AbstractModule {
    Logger logger = LoggerFactory.getLogger(DashBoardModule.class);

    @Override
    protected void configure() {

        Matcher<Class> subpacket = Matchers.inSubpackage("org.fedai.eggroll");
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
                addMapperClasses(MetaInfo.EGGROLL_DASHBOARD_MYBATIS_MAPPER_PACKAGE);

            }
        });
    }

}
