//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package com.webank.eggroll.guice.module;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.extension.MybatisMapWrapperFactory;
import com.eggroll.core.config.MetaInfo;
import com.google.inject.Inject;
import com.google.inject.ProvisionException;

import com.webank.eggroll.clustermanager.dao.mapper.ServerNodeMapper;
import edu.umd.cs.findbugs.annotations.Nullable;

import org.apache.ibatis.executor.ErrorContext;
import org.apache.ibatis.io.ResolverUtil;
import org.apache.ibatis.logging.log4j2.Log4j2Impl;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.apache.ibatis.mapping.DatabaseIdProvider;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.AutoMappingBehavior;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.ExecutorType;
;
import org.mybatis.guice.configuration.ConfigurationSettingListener;
import org.mybatis.guice.configuration.settings.ConfigurationSetting;
import org.mybatis.guice.configuration.settings.MapperConfigurationSetting;
import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import javax.inject.Named;
import javax.inject.Provider;
import javax.inject.Singleton;
import javax.sql.DataSource;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

@Singleton
public class MybatisPlusConfigurationProvider implements Provider<Configuration>, ConfigurationSettingListener {

    Logger logger = LoggerFactory.getLogger(MybatisPlusConfigurationProvider.class);

    private final Environment environment;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.lazyLoadingEnabled")
    private boolean lazyLoadingEnabled = false;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.aggressiveLazyLoading")
    private boolean aggressiveLazyLoading = true;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.multipleResultSetsEnabled")
    private boolean multipleResultSetsEnabled = true;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.useGeneratedKeys")
    private boolean useGeneratedKeys = false;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.useColumnLabel")
    private boolean useColumnLabel = true;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.cacheEnabled")
    private boolean cacheEnabled = true;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.defaultExecutorType")
    private ExecutorType defaultExecutorType;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.autoMappingBehavior")
    private AutoMappingBehavior autoMappingBehavior;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.callSettersOnNulls")
    private boolean callSettersOnNulls;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.defaultStatementTimeout")
    @Nullable
    private Integer defaultStatementTimeout;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.mapUnderscoreToCamelCase")
    private boolean mapUnderscoreToCamelCase;
    @Inject(
            optional = true
    )
    @Named("mybatis.configuration.failFast")
    private boolean failFast;
    @Inject(
            optional = true
    )
    private DatabaseIdProvider databaseIdProvider;
    @Inject
    private DataSource dataSource;
    private Set<ConfigurationSetting> configurationSettings;
    private Set<MapperConfigurationSetting> mapperConfigurationSettings;

    @Inject
    public MybatisPlusConfigurationProvider(Environment environment) {
        this.defaultExecutorType = ExecutorType.SIMPLE;
        this.autoMappingBehavior = AutoMappingBehavior.PARTIAL;
        this.callSettersOnNulls = false;
        this.mapUnderscoreToCamelCase = false;
        this.failFast = false;
        this.configurationSettings = new HashSet();
        this.mapperConfigurationSettings = new HashSet();
        this.environment = environment;
    }

    /**
     * @deprecated
     */
    @Deprecated
    public void setEnvironment(Environment environment) {
    }

    public void setFailFast(boolean failFast) {
        this.failFast = failFast;
    }

    @Override
    public void addConfigurationSetting(ConfigurationSetting configurationSetting) {
        this.configurationSettings.add(configurationSetting);
    }

    @Override
    public void addMapperConfigurationSetting(MapperConfigurationSetting mapperConfigurationSetting) {
        this.mapperConfigurationSettings.add(mapperConfigurationSetting);
    }

    protected MybatisConfiguration newConfiguration(Environment environment) {
        return new MybatisConfiguration(environment);
    }


    private static Set<Class<?>> getClasses(String packageName) {
        return getClasses(new ResolverUtil.IsA(Object.class), packageName);
    }

    private static Set<Class<?>> getClasses(ResolverUtil.Test test, String packageName) {
        return (new ResolverUtil()).find(test, packageName).getClasses();
    }


    @Override
    public Configuration get() {
        MybatisConfiguration configuration = this.newConfiguration(this.environment);
        configuration.setLazyLoadingEnabled(this.lazyLoadingEnabled);
        configuration.setAggressiveLazyLoading(this.aggressiveLazyLoading);
        configuration.setMultipleResultSetsEnabled(this.multipleResultSetsEnabled);
        configuration.setUseGeneratedKeys(this.useGeneratedKeys);
        configuration.setUseColumnLabel(this.useColumnLabel);
        configuration.setCacheEnabled(this.cacheEnabled);
        configuration.setDefaultExecutorType(this.defaultExecutorType);
        configuration.setAutoMappingBehavior(this.autoMappingBehavior);
        configuration.setCallSettersOnNulls(this.callSettersOnNulls);
        configuration.setDefaultStatementTimeout(this.defaultStatementTimeout);
        configuration.setMapUnderscoreToCamelCase(this.mapUnderscoreToCamelCase);

        configuration.setMapUnderscoreToCamelCase(true);

        configuration.setObjectWrapperFactory(new MybatisMapWrapperFactory());

        configuration.setLogImpl(Log4j2Impl.class);
        // configuration.setLogImpl(StdOutImpl.class);

        configuration.setCacheEnabled(MetaInfo.EGGROLL_MYBATIS_cache_enabled);

        Set<Class<?>> classes = getClasses(MetaInfo.EGGROLL_MYBATIS_MAPPER_PACKAGE);

        for (Class<?> clazz : classes) {
            try {

                configuration.addMapper(clazz);
                logger.info("mybatis plus add class {}", clazz);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        Iterator var2 = this.configurationSettings.iterator();

        while (var2.hasNext()) {
            ConfigurationSetting setting = (ConfigurationSetting) var2.next();
            setting.applyConfigurationSetting(configuration);
        }

        try {
            if (this.databaseIdProvider != null) {
                configuration.setDatabaseId(this.databaseIdProvider.getDatabaseId(this.dataSource));
            }

            var2 = this.mapperConfigurationSettings.iterator();

            while (var2.hasNext()) {
                MapperConfigurationSetting setting = (MapperConfigurationSetting) var2.next();
                setting.applyConfigurationSetting(configuration);
            }

            if (this.failFast) {
                configuration.getMappedStatementNames();
            }
        } catch (Throwable var7) {
            throw new ProvisionException("An error occurred while building the org.apache.ibatis.session.Configuration", var7);
        } finally {
            ErrorContext.instance().reset();
        }

        return configuration;
    }
}
