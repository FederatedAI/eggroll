package com.webank.eggroll.guice.module;

import com.baomidou.mybatisplus.core.MybatisConfiguration;
import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import com.google.inject.Provider;
import org.apache.ibatis.logging.stdout.StdOutImpl;
import org.apache.ibatis.mapping.Environment;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;
import org.apache.ibatis.session.SqlSessionFactoryBuilder;
import org.apache.ibatis.transaction.TransactionFactory;
import org.apache.ibatis.transaction.jdbc.JdbcTransactionFactory;

import javax.inject.Inject;
import javax.sql.DataSource;

public class MybatisPlusSqlSessionProvider implements Provider<SqlSessionFactory> {


    private SqlSessionFactory sqlSessionFactory;

    public MybatisPlusSqlSessionProvider() {
    }

    @Inject
    public void createNewSqlSessionFactory(Configuration configuration) {
        this.sqlSessionFactory = new MybatisSqlSessionFactoryBuilder().build(configuration);
    }

    public SqlSessionFactory get() {
        return this.sqlSessionFactory;
    }


//    private  void  build(){
//
//        //DataSource dataSource = dataSource();
//        TransactionFactory transactionFactory = new JdbcTransactionFactory();
//        Environment environment = new Environment("Production", transactionFactory, dataSource);
//        MybatisConfiguration configuration = new MybatisConfiguration(environment);
//        configuration.addMapper(PersonMapper.class);
//        configuration.setLogImpl(StdOutImpl.class);
//        return new MybatisSqlSessionFactoryBuilder().build(configuration);
//    }
}
