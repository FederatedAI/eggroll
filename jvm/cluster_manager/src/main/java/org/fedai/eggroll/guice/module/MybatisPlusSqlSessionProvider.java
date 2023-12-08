package org.fedai.eggroll.guice.module;

import com.baomidou.mybatisplus.core.MybatisSqlSessionFactoryBuilder;
import com.google.inject.Provider;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSessionFactory;

import javax.inject.Inject;

public class MybatisPlusSqlSessionProvider implements Provider<SqlSessionFactory> {


    private SqlSessionFactory sqlSessionFactory;

    public MybatisPlusSqlSessionProvider() {
    }

    @Inject
    public void createNewSqlSessionFactory(Configuration configuration) {
        this.sqlSessionFactory = new MybatisSqlSessionFactoryBuilder().build(configuration);
    }

    @Override
    public SqlSessionFactory get() {
        return this.sqlSessionFactory;
    }

}
