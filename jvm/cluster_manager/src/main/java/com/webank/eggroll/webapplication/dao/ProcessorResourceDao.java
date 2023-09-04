package com.webank.eggroll.webapplication.dao;

import com.webank.eggroll.clustermanager.entity.ProcessorResource;

import java.util.Arrays;
import java.util.List;

//
//import com.baomidou.mybatisplus.core.toolkit.reflect.GenericTypeUtils;
//import com.eggroll.core.config.MetaInfo;
//import com.google.inject.AbstractModule;
//import com.google.inject.name.Names;
//import com.webank.eggroll.clustermanager.entity.ProcessorResource;
//import com.webank.eggroll.clustermanager.session.DefaultSessionManager;
//import com.webank.eggroll.clustermanager.session.SessionManager;
//import com.webank.eggroll.guice.module.MybatisPlusConfigurationProvider;
//import com.webank.eggroll.guice.module.MybatisPlusSqlSessionProvider;
//import com.webank.eggroll.guice.module.MybatisPlusUtil;
//import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;
//import org.mybatis.guice.MyBatisModule;
//import org.mybatis.guice.datasource.hikaricp.HikariCPProvider;
//
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.sql.Statement;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
public class ProcessorResourceDao {

    public List<ProcessorResource> getAllResources() {
        return Arrays.asList(new ProcessorResource());
    }

}
